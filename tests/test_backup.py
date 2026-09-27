from __future__ import annotations

import hashlib
import io
import json
import sqlite3
import tempfile
import threading
import zipfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import db

BUSINESS_TABLES = ("samples", "events", "event_changes", "event_acks")


def download_backup(client: TestClient):
    """POST the backup endpoint and return the parsed archive parts."""
    response = client.post("/maintenance/backup")
    assert response.status_code == 200, response.text
    assert response.headers["content-type"] == "application/zip"
    assert "attachment" in response.headers["content-disposition"]
    assert response.headers["content-disposition"].endswith(".zip\"")

    archive = zipfile.ZipFile(io.BytesIO(response.content))
    assert archive.namelist() == ["water.db", "manifest.json"]
    manifest = json.loads(archive.read("manifest.json"))
    database_bytes = archive.read("water.db")
    return response, archive, manifest, database_bytes


def check_snapshot(manifest: dict, database_bytes: bytes, *, extracted: Path):
    """Validate one backup database against its manifest and consistency.

    The packaged water.db must pass integrity_check, its SHA-256 must match
    the manifest, and every count/max_change_id in the manifest must equal
    the values actually present in the backup database (not the live one).
    Returns a dict of the values read from the extracted snapshot.
    """
    expected_sha = hashlib.sha256(database_bytes).hexdigest()
    assert manifest["database"] == "water.db"
    assert manifest["water_db_sha256"] == expected_sha
    assert isinstance(manifest["created_at"], str) and manifest["created_at"]

    extracted.write_bytes(database_bytes)
    connection = sqlite3.connect(str(extracted))
    try:
        assert (
            connection.execute("PRAGMA integrity_check").fetchone()[0]
            == "ok"
        )
        version = connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()[0]
        assert manifest["schema_version"] == int(version) == db.SCHEMA_VERSION

        counts = {
            table: connection.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).fetchone()[0]
            for table in BUSINESS_TABLES
        }
        assert manifest["table_counts"] == counts

        max_change_id = connection.execute(
            "SELECT MAX(change_id) FROM event_changes"
        ).fetchone()[0]
        expected_max = 0 if max_change_id is None else int(max_change_id)
        assert manifest["max_change_id"] == expected_max
        # The change stream, when present, is gap-free from 1.
        if counts["event_changes"]:
            ids = [
                row[0]
                for row in connection.execute(
                    "SELECT change_id FROM event_changes ORDER BY change_id"
                )
            ]
            assert ids == list(range(1, len(ids) + 1))
            assert expected_max == ids[-1]
        else:
            assert expected_max == 0
        return counts, expected_max
    finally:
        connection.close()


def test_backup_empty_database(client: TestClient, tmp_path: Path):
    response, _archive, manifest, database_bytes = download_backup(client)
    # No body is needed for the POST; nothing is sent on failure either.
    assert response.request.content == b""

    counts, max_change_id = check_snapshot(
        manifest,
        database_bytes,
        extracted=tmp_path / "empty.db",
    )
    assert counts == {table: 0 for table in BUSINESS_TABLES}
    assert max_change_id == 0


def test_backup_contents_match_committed_state(
    client: TestClient, tmp_path: Path
):
    client.post("/records", json={"records": [
        {"device_id": "sensor-a", "sequence": 1,
         "sampled_at": "2026-09-23T08:00:00Z", "turbidity": 12.0},
        {"device_id": "sensor-a", "sequence": 2,
         "sampled_at": "2026-09-23T08:01:00Z", "turbidity": 4.0},
        {"device_id": "sensor-b", "sequence": 1,
         "sampled_at": "2026-09-23T08:00:00Z", "turbidity": 11.0},
        {"device_id": "sensor-b", "sequence": 2,
         "sampled_at": "2026-09-23T08:01:00Z", "turbidity": 13.0},
    ]})
    events = client.get("/devices/sensor-a/events").json()["events"]
    client.post(
        "/devices/sensor-a/events/evt:sensor-a:1/ack",
        json={"expected_event": {
            key: value for key, value in events[0].items()
            if key != "acknowledged"
        }},
    )

    _response, _archive, manifest, database_bytes = download_backup(client)
    counts, max_change_id = check_snapshot(
        manifest,
        database_bytes,
        extracted=tmp_path / "snapshot.db",
    )
    # One committed write transaction produced two event upserts; one ack.
    assert counts == {
        "samples": 4,
        "events": 2,
        "event_changes": 2,
        "event_acks": 1,
    }
    assert max_change_id == 2


def test_restored_database_boots_and_serves_data(
    client: TestClient, tmp_path: Path, monkeypatch
):
    client.post("/records", json={"records": [
        {"device_id": "sensor-a", "sequence": 1,
         "sampled_at": "2026-09-23T08:00:00Z", "turbidity": 12.0},
        {"device_id": "sensor-a", "sequence": 2,
         "sampled_at": "2026-09-23T08:01:00Z", "turbidity": 4.0},
        {"device_id": "sensor-b", "sequence": 1,
         "sampled_at": "2026-09-23T08:00:00Z", "turbidity": 11.0},
        {"device_id": "sensor-b", "sequence": 2,
         "sampled_at": "2026-09-23T08:01:00Z", "turbidity": 4.0},
    ]})

    _response, _archive, manifest, database_bytes = download_backup(client)
    restored_path = tmp_path / "restore" / "water.db"
    restored_path.parent.mkdir(parents=True)
    restored_path.write_bytes(database_bytes)
    # The packaged database is a single self-contained file.
    assert {p.name for p in restored_path.parent.iterdir()} == {"water.db"}

    # Boot a separate application whose WATER_DATABASE_PATH points at the
    # extracted backup, exactly as the documented offline recovery does.
    monkeypatch.setenv("WATER_DATABASE_PATH", str(restored_path))
    from app.main import app

    with TestClient(app) as restored_client:
        events_a = restored_client.get(
            "/devices/sensor-a/events"
        ).json()
        assert events_a["total"] == 1
        assert events_a["events"][0]["event_id"] == "evt:sensor-a:1"
        assert events_a["events"][0]["status"] == "recovered"
        assert restored_client.get(
            "/devices/sensor-b/events"
        ).json()["total"] == 1

        changes = restored_client.get("/event-changes?after=0").json()
        assert changes["head_change_id"] == 2
        assert len(changes["changes"]) == 2

        # The restored database accepts new writes and keeps the change
        # stream gap-free past the restored head.
        write = restored_client.post("/records", json={"records": [
            {"device_id": "sensor-c", "sequence": 1,
             "sampled_at": "2026-09-23T10:00:00Z", "turbidity": 12.0},
            {"device_id": "sensor-c", "sequence": 2,
             "sampled_at": "2026-09-23T10:01:00Z", "turbidity": 4.0},
        ]})
        assert write.status_code == 201
        changes_after = restored_client.get(
            "/event-changes?after=2"
        ).json()
        assert [c["change_id"] for c in changes_after["changes"]] == [3]

    # The SHA-256 recorded in the manifest is that of the packaged file.
    assert hashlib.sha256(database_bytes).hexdigest() == manifest[
        "water_db_sha256"
    ]


def test_backups_never_mix_transactions_under_concurrency(
    client: TestClient, tmp_path: Path
):
    # Seed one device with one recovered event and acknowledge it; a
    # correction thread repeatedly changes and restores that event's peak,
    # invalidating the acknowledgement on the first change. Every device
    # always carries an even number of alternating high/low samples, so
    # every committed snapshot obeys the invariants asserted below.
    client.post("/records", json={"records": [
        {"device_id": "ackdev", "sequence": 1,
         "sampled_at": "2026-09-23T08:00:00Z", "turbidity": 12.0},
        {"device_id": "ackdev", "sequence": 2,
         "sampled_at": "2026-09-23T08:01:00Z", "turbidity": 4.0},
    ]})
    ack_event = client.get("/devices/ackdev/events").json()["events"][0]
    client.post(
        "/devices/ackdev/events/evt:ackdev:1/ack",
        json={"expected_event": {
            key: value for key, value in ack_event.items()
            if key != "acknowledged"
        }},
    )

    writer_errors: list[Exception] = []
    stop = threading.Event()

    def write_device(device: str) -> None:
        try:
            for iteration in range(20):
                minute = iteration
                client.post("/records", json={"records": [
                    {"device_id": device,
                     "sequence": 2 * iteration,
                     "sampled_at": f"2026-09-23T09:{minute:02d}:00Z",
                     "turbidity": 12.0},
                    {"device_id": device,
                     "sequence": 2 * iteration + 1,
                     "sampled_at": f"2026-09-23T09:{minute:02d}:30Z",
                     "turbidity": 4.0},
                ]})
        except Exception as exc:  # pragma: no cover - test diagnostics
            writer_errors.append(exc)

    def correct_loop() -> None:
        # Alternate the ack event's peak between 12.0 and 12.5; each
        # successful correction is its own commit that concurrent backups
        # may straddle.
        values = [(12.0, 12.5), (12.5, 12.0)]
        iteration = 0
        try:
            while not stop.is_set():
                old, new = values[iteration % 2]
                response = client.post("/records/correction", json={
                    "device_id": "ackdev",
                    "sequence": 1,
                    "old_sampled_at": "2026-09-23T08:00:00Z",
                    "old_turbidity": old,
                    "new_sampled_at": "2026-09-23T08:00:00Z",
                    "new_turbidity": new,
                })
                assert response.status_code == 200, response.text
                iteration += 1
        except Exception as exc:  # pragma: no cover - test diagnostics
            writer_errors.append(exc)

    writers = [
        threading.Thread(target=write_device, args=(f"dev-{i}",))
        for i in range(3)
    ] + [threading.Thread(target=correct_loop)]

    snapshots: list[tuple[dict, int]] = []
    for thread in writers:
        thread.start()

    try:
        for backup_index in range(8):
            _r, _a, manifest, database_bytes = download_backup(client)
            counts, max_change_id = check_snapshot(
                manifest,
                database_bytes,
                extracted=tmp_path / f"snapshot-{backup_index}.db",
            )
            snapshots.append((counts, max_change_id))
    finally:
        stop.set()
        for thread in writers:
            thread.join()

    assert not writer_errors, writer_errors

    # Invariants that must hold for every single committed snapshot even
    # though backups raced with sampling writes, late recomputations,
    # corrections and acknowledgements:
    for counts, max_change_id in snapshots:
        samples = counts["samples"]
        events = counts["events"]
        changes = counts["event_changes"]
        acks = counts["event_acks"]

        # Every device holds complete high/low sample pairs at commit time.
        assert samples % 2 == 0
        # One derived event per committed pair.
        assert events == samples // 2
        # A write batch adds one event and one upsert together; each
        # correction re-upserts the surviving ackdev event without adding
        # or removing an event, so corrections only grow the surplus.
        assert changes >= events
        # The change stream is gap-free and its head equals its count.
        assert max_change_id == changes
        # The ack is present, or absent right after a correction commit.
        assert acks in (0, 1)

    # Final committed totals after all workers joined.
    final = client.post("/maintenance/backup")
    assert final.status_code == 200
    final_archive = zipfile.ZipFile(io.BytesIO(final.content))
    final_manifest = json.loads(final_archive.read("manifest.json"))
    final_counts = final_manifest["table_counts"]
    # 2 ackdev seed samples + 3 devices * 20 iterations * 2 samples.
    assert final_counts["samples"] == 2 + 3 * 20 * 2
    assert final_counts["events"] == final_counts["samples"] // 2
    assert final_counts["event_changes"] == final_manifest["max_change_id"]


def test_failed_backup_cleans_up_and_leaves_live_db(
    client: TestClient, tmp_path: Path, monkeypatch
):
    client.post("/records", json={"records": [
        {"device_id": "sensor-a", "sequence": 1,
         "sampled_at": "2026-09-23T08:00:00Z", "turbidity": 12.0},
        {"device_id": "sensor-a", "sequence": 2,
         "sampled_at": "2026-09-23T08:01:00Z", "turbidity": 4.0},
    ]})

    def fail_backup(_connection, _target_path):
        raise sqlite3.DatabaseError("simulated backup failure")

    real_online_backup = db.online_backup
    monkeypatch.setattr(db, "online_backup", fail_backup)
    response = client.post("/maintenance/backup")
    assert response.status_code == 500
    assert "integrity" not in response.text  # this was an API failure
    assert response.headers["content-type"] != "application/zip"

    # No staging directory or final archive remains in the temp area.
    leftovers = list(Path(tempfile.gettempdir()).glob("water-backup-*"))
    assert leftovers == []

    # The online database is untouched and still fully serviceable.
    events = client.get("/devices/sensor-a/events").json()
    assert events["total"] == 1
    follow_up = client.post("/records", json={"records": [
        {"device_id": "sensor-a", "sequence": 3,
         "sampled_at": "2026-09-23T08:10:00Z", "turbidity": 12.0},
        {"device_id": "sensor-a", "sequence": 4,
         "sampled_at": "2026-09-23T08:11:00Z", "turbidity": 4.0},
    ]})
    assert follow_up.status_code == 201

    # A later successful backup proves the path still works.
    monkeypatch.setattr(db, "online_backup", real_online_backup)
    _r, _a, manifest, database_bytes = download_backup(client)
    counts, _max_id = check_snapshot(
        manifest,
        database_bytes,
        extracted=tmp_path / "after.db",
    )
    assert counts["samples"] == 4


def test_integrity_check_failure_cleans_up_and_returns_error(
    client: TestClient, monkeypatch
):
    """A non-ok integrity_check must produce a 500, not a partial file."""

    real_online_backup = db.online_backup

    def corrupt_then_backup(connection, target_path):
        real_online_backup(connection, target_path)
        # Tamper with the freshly made backup database so its
        # integrity_check reports corruption, without touching the online
        # database. Page 2 is the schema_meta root (created first) and
        # always holds rows; corrupting its cell content area at the page
        # tail makes integrity_check report non-ok rather than raise.
        with open(target_path, "r+b") as handle:
            handle.seek(2 * 4096 - 512)
            handle.write(b"\xff" * 512)

    monkeypatch.setattr(db, "online_backup", corrupt_then_backup)
    response = client.post("/maintenance/backup")
    assert response.status_code == 500
    assert "integrity" in response.json()["detail"]
    assert response.headers["content-type"] != "application/zip"
    assert list(Path(tempfile.gettempdir()).glob("water-backup-*")) == []


@pytest.mark.parametrize("method", ["get", "put", "delete"])
def test_backup_endpoint_requires_post(client: TestClient, method: str):
    response = getattr(client, method)("/maintenance/backup")
    assert response.status_code in (404, 405)
