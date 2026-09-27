from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from .schemas import BackupError

# The four business tables whose row counts the manifest reports. Table
# names are fixed constants, never request input, so interpolating them
# into the COUNT queries below is safe.
BUSINESS_TABLES = ("samples", "events", "event_changes", "event_acks")

DATABASE_ENTRY = "water.db"
MANIFEST_ENTRY = "manifest.json"


def create_backup_zip(source: sqlite3.Connection) -> bytes:
    """Package one committed snapshot of the live database as ZIP bytes.

    The SQLite online backup API copies the database into a temporary
    file; the caller serializes this with every write transaction, so the
    copy is exactly one committed state and never mixes rows across
    transactions. The copy is checked with PRAGMA integrity_check and the
    manifest is read from the copy (never from the live database) before
    anything is packed. Any failure raises BackupError; the temporary
    directory is removed by the context manager, so no half-finished
    archive or leftover file remains, and the live database is never
    modified (the backup API only reads the source).
    """
    with tempfile.TemporaryDirectory(prefix="water-backup-") as tmp_dir:
        db_path = Path(tmp_dir) / DATABASE_ENTRY
        try:
            _snapshot_to_file(source, db_path)

            check = sqlite3.connect(str(db_path))
            try:
                errors = _integrity_errors(check)
                if errors:
                    raise BackupError(
                        "integrity check failed for the backup database: "
                        + "; ".join(errors)
                    )
                manifest = _read_manifest(check)
            finally:
                check.close()

            manifest["water_db_sha256"] = _sha256_file(db_path)
            return _pack_zip(db_path, manifest)
        except BackupError:
            raise
        except (sqlite3.Error, OSError) as exc:
            raise BackupError(f"could not create backup: {exc}") from exc


def _snapshot_to_file(source: sqlite3.Connection, db_path: Path) -> None:
    """Copy the live database into a new standalone file.

    The online backup API commits the copy as one transaction, so the
    file holds a single consistent snapshot. The destination inherits the
    source's WAL mode, which would leave committed pages in a sidecar
    water.db-wal; switching the copy back to a rollback journal folds
    everything into the main file, so water.db alone is the complete
    database.
    """
    target = sqlite3.connect(str(db_path))
    try:
        source.backup(target)
        target.execute("PRAGMA journal_mode=DELETE")
    finally:
        target.close()


def _integrity_errors(connection: sqlite3.Connection) -> list[str]:
    """Run PRAGMA integrity_check; return [] when the database is sound."""
    rows = connection.execute("PRAGMA integrity_check").fetchall()
    messages = [str(row[0]) for row in rows]
    return [] if messages == ["ok"] else messages


def _read_manifest(connection: sqlite3.Connection) -> dict[str, Any]:
    """Read the manifest facts from the backup database itself."""
    row = connection.execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    if row is None:
        raise BackupError("backup database has no schema_version")
    tables = {
        table: int(
            connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
        )
        for table in BUSINESS_TABLES
    }
    head = connection.execute(
        "SELECT MAX(change_id) FROM event_changes"
    ).fetchone()[0]
    return {
        "schema_version": int(row[0]),
        "tables": tables,
        "max_change_id": 0 if head is None else int(head),
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pack_zip(db_path: Path, manifest: dict[str, Any]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.write(db_path, DATABASE_ENTRY)
        archive.writestr(
            MANIFEST_ENTRY,
            json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        )
    return buffer.getvalue()
