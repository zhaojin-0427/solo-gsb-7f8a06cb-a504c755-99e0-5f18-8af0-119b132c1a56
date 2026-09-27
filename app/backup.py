from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from . import db

DATABASE_ENTRY = "water.db"
MANIFEST_ENTRY = "manifest.json"
# The four business tables whose committed row counts describe the
# snapshot; schema_meta is configuration, not business data.
MANIFEST_TABLES = ("samples", "events", "event_changes", "event_acks")


class BackupError(Exception):
    """A backup could not be produced safely.

    Raised when the backup database fails ``PRAGMA integrity_check``;
    every temporary artifact has already been removed by the time this
    propagates, nothing half-finished is sent, and the online database is
    never modified by the backup path.
    """


@dataclass(frozen=True)
class BackupArchive:
    """The produced ZIP: its path and the manifest it contains."""

    path: Path
    manifest: dict[str, object]


def _table_count(connection: sqlite3.Connection, table: str) -> int:
    # The table names come from the fixed MANIFEST_TABLES tuple, never
    # from request input, so interpolating them is safe.
    row = connection.execute(
        f"SELECT COUNT(*) FROM {table}"
    ).fetchone()
    return int(row[0])


def _read_manifest_data(
    connection: sqlite3.Connection,
) -> dict[str, object]:
    """Read schema_version, business-table counts and max change_id.

    Every value is read from the backup database (the just-finished
    single committed snapshot), never from the live database, so the
    manifest can only describe rows that are actually inside the archive.
    """
    version = db.get_meta(connection, "schema_version")
    if version is None:
        raise BackupError(
            "backup database has no schema_version in schema_meta"
        )
    counts = {
        table: _table_count(connection, table) for table in MANIFEST_TABLES
    }
    max_row = connection.execute(
        "SELECT MAX(change_id) FROM event_changes"
    ).fetchone()
    max_change_id = 0 if max_row[0] is None else int(max_row[0])
    return {
        "schema_version": int(version),
        "table_counts": counts,
        "max_change_id": max_change_id,
    }


def _sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _remove_staging(staging: Path) -> None:
    """Best-effort removal of the temporary staging directory."""
    for child in staging.iterdir():
        child.unlink()
    staging.rmdir()


def build_backup_archive(
    connection: sqlite3.Connection,
    *,
    work_dir: str | Path | None = None,
) -> BackupArchive:
    """Build a ZIP holding one consistent ``water.db`` plus a manifest.

    The database is produced by the SQLite online backup API (see
    :func:`app.db.online_backup`) and therefore holds a single committed
    snapshot; it is then integrity-checked inside the temporary staging
    directory and only packaged when the check reports ``ok``. The
    manifest records the schema version, the row counts of the four
    business tables, ``max_change_id`` (0 when the change stream is
    empty), the snapshot time and the SHA-256 of the packaged
    ``water.db``. Any failure removes all staging files; the ZIP is
    written to its final path only after the payload is complete.
    """
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    staging = Path(tempfile.mkdtemp(prefix="water-backup-", dir=work_dir))
    database_path = staging / DATABASE_ENTRY
    archive_path = staging / "water-backup.zip"
    try:
        db.online_backup(connection, database_path)

        check_connection = sqlite3.connect(
            str(database_path), isolation_level=None
        )
        # db.get_meta indexes rows by column name.
        check_connection.row_factory = sqlite3.Row
        try:
            check = str(
                check_connection.execute(
                    "PRAGMA integrity_check"
                ).fetchone()[0]
            )
            if check != "ok":
                raise BackupError(
                    f"backup database integrity check failed: {check}"
                )
            manifest = _read_manifest_data(check_connection)
        finally:
            check_connection.close()

        sha256 = _sha256_of_file(database_path)
        manifest = {
            "created_at": created_at,
            "database": DATABASE_ENTRY,
            "water_db_sha256": sha256,
            **manifest,
        }

        with zipfile.ZipFile(
            archive_path, "w", compression=zipfile.ZIP_DEFLATED
        ) as archive:
            archive.write(database_path, arcname=DATABASE_ENTRY)
            archive.writestr(
                MANIFEST_ENTRY,
                json.dumps(manifest, indent=2, sort_keys=True) + "\n",
            )
    except BackupError:
        # Integrity-check failure: leave nothing behind and do not send a
        # partial download; the online database is never touched here.
        _remove_staging(staging)
        raise
    except Exception as exc:
        # Any other failure (backup API, filesystem, zip) likewise cleans
        # up and surfaces as a controlled error, never a partial archive.
        _remove_staging(staging)
        raise BackupError(f"backup failed: {exc}") from exc

    fd, name = tempfile.mkstemp(
        prefix=f"water-backup-{created_at.replace(':', '-')}-",
        suffix=".zip",
        dir=work_dir,
    )
    os.close(fd)
    final_path = Path(name)
    archive_path.replace(final_path)
    database_path.unlink()
    staging.rmdir()
    return BackupArchive(path=final_path, manifest=manifest)
