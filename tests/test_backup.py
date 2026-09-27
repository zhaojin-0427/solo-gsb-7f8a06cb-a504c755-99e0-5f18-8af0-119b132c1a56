from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import tempfile
import threading
import zipfile

from fastapi.testclient import TestClient

from app import backup, db
from tests.conftest import ack_event, get_events, post_records, strip_acknowledged


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


def read_zip(response) -> dict[str, bytes]:
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        return {name: archive.read(name) for name in archive.namelist()}


def seed_acknowledged_event(client: TestClient) -> None:
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12.0),
            sample(2, "2026-09-23T08:01:00Z", 13.0),
            sample(3, "2026-09-23T08:02:00Z", 4.0),
        ],
    )
    event = get_events(client)["events"][0]
    response = ack_event(client, event)
    assert response.status_code == 200


def test_backup_zip_contains_snapshot_and_manifest(client: TestClient):
    seed_acknowledged_event(client)

    response = client.post("/maintenance/backup")
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    disposition = response.headers["content-disposition"]
    assert disposition.startswith('attachment; filename="water-backup-')
    assert disposition.endswith('.zip"')

    entries = read_zip(response)
    assert set(entries) == {"water.db", "manifest.json"}
    assert entries["water.db"].startswith(b"SQLite format 3\x00")

    manifest = json.loads(entries["manifest.json"])
    assert manifest == {
        "schema_version": db.SCHEMA_VERSION,
        "tables": {
            "samples": 3,
            "events": 1,
            "event_changes": 1,
            "event_acks": 1,
        },
        "max_change_id": 1,
        "water_db_sha256": hashlib.sha256(entries["water.db"]).hexdigest(),
    }

    # The snapshot itself carries the same committed rows and is sound.
    snapshot = sqlite3.connect(":memory:")
    snapshot.deserialize(entries["water.db"])
    try:
        assert snapshot.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
        assert snapshot.execute(
            """
            SELECT device_id, sequence, sampled_at, turbidity
            FROM samples ORDER BY sequence
            """
        ).fetchall() == [
            ("sensor-a", 1, "2026-09-23T08:00:00Z", 12.0),
            ("sensor-a", 2, "2026-09-23T08:01:00Z", 13.0),
            ("sensor-a", 3, "2026-09-23T08:02:00Z", 4.0),
        ]
        assert snapshot.execute(
            "SELECT event_id, status FROM events"
        ).fetchall() == [("evt:sensor-a:1", "recovered")]
        assert snapshot.execute(
            "SELECT change_id, change_type FROM event_changes"
        ).fetchall() == [(1, "upsert")]
        assert snapshot.execute("SELECT event_id FROM event_acks").fetchall() == [
            ("evt:sensor-a:1",)
        ]
        assert snapshot.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone() == (str(db.SCHEMA_VERSION),)
    finally:
        snapshot.close()


def test_backup_of_empty_database(client: TestClient):
    response = client.post("/maintenance/backup")
    assert response.status_code == 200

    entries = read_zip(response)
    manifest = json.loads(entries["manifest.json"])
    assert manifest == {
        "schema_version": db.SCHEMA_VERSION,
        "tables": {
            "samples": 0,
            "events": 0,
            "event_changes": 0,
            "event_acks": 0,
        },
        "max_change_id": 0,
        "water_db_sha256": hashlib.sha256(entries["water.db"]).hexdigest(),
    }

    snapshot = sqlite3.connect(":memory:")
    snapshot.deserialize(entries["water.db"])
    try:
        assert snapshot.execute("PRAGMA integrity_check").fetchall() == [("ok",)]
    finally:
        snapshot.close()


def test_backup_restore_serves_original_data(client: TestClient, tmp_path, monkeypatch):
    seed_acknowledged_event(client)
    original_events = get_events(client)["events"]
    original_changes = client.get("/event-changes", params={"after": 0}).json()
    original_evidence = client.get(
        "/devices/sensor-a/events/evt:sensor-a:1/evidence"
    ).json()

    entries = read_zip(client.post("/maintenance/backup"))
    restored_path = tmp_path / "restored" / "water.db"
    restored_path.parent.mkdir()
    restored_path.write_bytes(entries["water.db"])

    # Placed at WATER_DATABASE_PATH with the original configuration, the
    # downloaded database starts and serves the original data.
    monkeypatch.setenv("WATER_DATABASE_PATH", str(restored_path))
    from app.main import app

    with TestClient(app) as restored:
        events = get_events(restored)["events"]
        assert strip_acknowledged(events) == strip_acknowledged(original_events)
        assert [event["acknowledged"] for event in events] == [True]
        assert restored.get("/event-changes", params={"after": 0}).json() == (
            original_changes
        )
        assert restored.get(
            "/devices/sensor-a/events/evt:sensor-a:1/evidence"
        ).json() == original_evidence
        assert restored.get("/thresholds").json() == {
            "alarm_threshold": 10.0,
            "recovery_threshold": 5.0,
            "max_sample_gap_seconds": 600,
        }

        # The restored database is fully writable: new samples extend the
        # preserved change stream instead of restarting it.
        response = post_records(
            restored, [sample(4, "2026-09-23T08:03:00Z", 12.0)]
        )
        assert response.status_code == 201
        head = restored.get("/event-changes", params={"after": 0}).json()[
            "head_change_id"
        ]
        assert head == original_changes["head_change_id"] + 1


def test_backup_does_not_modify_live_database(client: TestClient):
    seed_acknowledged_event(client)
    before_events = get_events(client)
    before_changes = client.get("/event-changes", params={"after": 0}).json()

    response = client.post("/maintenance/backup")
    assert response.status_code == 200

    assert get_events(client) == before_events
    assert client.get("/event-changes", params={"after": 0}).json() == (
        before_changes
    )

    # The live database file still holds exactly the pre-backup state.
    live = sqlite3.connect(os.environ["WATER_DATABASE_PATH"])
    try:
        assert live.execute("SELECT COUNT(*) FROM samples").fetchone() == (3,)
        assert live.execute("SELECT COUNT(*) FROM event_acks").fetchone() == (1,)
        assert live.execute(
            "SELECT MAX(change_id) FROM event_changes"
        ).fetchone() == (1,)
    finally:
        live.close()


def test_backup_integrity_failure_returns_error_and_cleans_up(
    client: TestClient, monkeypatch, tmp_path
):
    seed_acknowledged_event(client)
    before_changes = client.get("/event-changes", params={"after": 0}).json()

    monkeypatch.setattr(
        backup, "_integrity_errors", lambda connection: ["simulated corruption"]
    )
    leftover_before = set(os.listdir(tempfile.gettempdir()))

    response = client.post("/maintenance/backup")
    assert response.status_code == 500
    assert "integrity check failed" in response.json()["detail"]

    # No half-finished archive or temporary file is left behind.
    assert set(os.listdir(tempfile.gettempdir())) == leftover_before

    # The live database is untouched and keeps serving writes and reads.
    assert client.get("/event-changes", params={"after": 0}).json() == (
        before_changes
    )
    assert post_records(client, [sample(4, "2026-09-23T08:03:00Z", 3.0)])
    assert get_events(client)["total"] == 1


def test_backup_is_serialized_with_concurrent_writes(
    client: TestClient, monkeypatch
):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12.0)])

    entered = threading.Event()
    release = threading.Event()
    original = backup._snapshot_to_file

    def blocking_snapshot(source, db_path):
        entered.set()
        assert release.wait(timeout=10)
        return original(source, db_path)

    monkeypatch.setattr(backup, "_snapshot_to_file", blocking_snapshot)

    backup_responses: list = []
    backup_thread = threading.Thread(
        target=lambda: backup_responses.append(
            client.post("/maintenance/backup")
        )
    )
    backup_thread.start()
    assert entered.wait(timeout=10)

    write_done = threading.Event()

    def write() -> None:
        post_records(client, [sample(2, "2026-09-23T08:01:00Z", 13.0)])
        write_done.set()

    write_thread = threading.Thread(target=write)
    write_thread.start()
    # The concurrent write stays blocked while the backup holds the lock.
    assert not write_done.wait(timeout=0.5)

    release.set()
    backup_thread.join(timeout=10)
    write_thread.join(timeout=10)
    assert write_done.is_set()

    assert backup_responses[0].status_code == 200
    manifest = json.loads(read_zip(backup_responses[0])["manifest.json"])
    # The snapshot is exactly the committed state before the write: the
    # concurrent ingest neither leaks in nor mixes partially.
    assert manifest["tables"] == {
        "samples": 1,
        "events": 1,
        "event_changes": 1,
        "event_acks": 0,
    }
    assert manifest["max_change_id"] == 1

    # Afterwards the live database reflects the completed write.
    events = get_events(client)["events"]
    assert len(events) == 1
    assert events[0]["sample_count"] == 2
