from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import db
from tests.conftest import post_records


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


# The events table as schema version 2 created it: sequence numbers were
# wrongly treated as chronological, so CHECK constraints rejected events
# whose peak or end carried an out-of-order sequence.
LEGACY_EVENTS_DDL = """
CREATE TABLE events (
    event_id TEXT NOT NULL PRIMARY KEY,
    device_id TEXT NOT NULL,
    status TEXT NOT NULL
        CHECK (status IN ('active', 'interrupted', 'recovered')),
    start_sequence INTEGER NOT NULL,
    end_sequence INTEGER,
    start_at TEXT NOT NULL,
    end_at TEXT,
    peak_turbidity REAL NOT NULL,
    peak_sequence INTEGER NOT NULL,
    sample_count INTEGER NOT NULL CHECK (sample_count > 0),
    UNIQUE (device_id, start_sequence),
    CHECK (end_sequence IS NULL OR end_sequence >= start_sequence),
    CHECK (end_at IS NULL OR end_at >= start_at),
    CHECK ((end_sequence IS NULL) = (end_at IS NULL)),
    CHECK ((status = 'active') = (end_at IS NULL)),
    CHECK (peak_sequence >= start_sequence),
    CHECK (end_sequence IS NULL OR peak_sequence <= end_sequence)
) WITHOUT ROWID;
"""


def _create_legacy_database(path) -> None:
    """Build a schema-version-2 database with raw samples and derived events."""
    connection = sqlite3.connect(str(path))
    try:
        connection.executescript(
            """
            CREATE TABLE schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );

            CREATE TABLE samples (
                device_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                sampled_at TEXT NOT NULL,
                turbidity REAL NOT NULL CHECK (turbidity >= 0.0),
                PRIMARY KEY (device_id, sequence)
            ) WITHOUT ROWID;

            CREATE INDEX idx_samples_device_time
                ON samples (device_id, sampled_at, sequence);
            """
        )
        connection.execute(LEGACY_EVENTS_DDL)
        connection.executemany(
            "INSERT INTO schema_meta(key, value) VALUES (?, ?)",
            [
                ("schema_version", "2"),
                ("alarm_threshold", "10.0"),
                ("recovery_threshold", "5.0"),
                ("max_sample_gap_seconds", "600"),
            ],
        )
        connection.executemany(
            """
            INSERT INTO samples (device_id, sequence, sampled_at, turbidity)
            VALUES (?, ?, ?, ?)
            """,
            [
                ("sensor-a", 1, "2026-09-23T00:00:00Z", 12.0),
                ("sensor-a", 3, "2026-09-23T00:08:00Z", 13.0),
                ("sensor-a", 7, "2026-09-23T00:30:00Z", 14.0),
            ],
        )
        # Derived rows exactly as the old service persisted them.
        connection.executemany(
            """
            INSERT INTO events (
                event_id, device_id, status, start_sequence, end_sequence,
                start_at, end_at, peak_turbidity, peak_sequence, sample_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    "evt:sensor-a:1", "sensor-a", "interrupted", 1, 3,
                    "2026-09-23T00:00:00Z", "2026-09-23T00:08:00Z",
                    13.0, 3, 2,
                ),
                (
                    "evt:sensor-a:7", "sensor-a", "active", 7, None,
                    "2026-09-23T00:30:00Z", None,
                    14.0, 7, 1,
                ),
            ],
        )
        connection.commit()
    finally:
        connection.close()


def test_legacy_database_migrates_and_rebuilds_events(tmp_path, monkeypatch):
    db_path = tmp_path / "water.db"
    _create_legacy_database(db_path)

    monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
    monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
    monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", "600")

    from app.main import app

    with TestClient(app) as client:
        # The migration dropped the legacy derived table and rebuilt it from
        # the raw samples; the previously derived rows reappear unchanged.
        body = client.get("/devices/sensor-a/events").json()
        assert body["total"] == 2
        assert [event["event_id"] for event in body["events"]] == [
            "evt:sensor-a:1",
            "evt:sensor-a:7",
        ]
        assert [event["status"] for event in body["events"]] == [
            "interrupted",
            "active",
        ]
        assert body["events"][0]["end_sequence"] == 3
        assert body["events"][0]["peak_sequence"] == 3

        # The late sample the old schema rejected now persists: it carries
        # the highest sequence and the peak, yet the interrupted event still
        # ends (by sample time) at 00:08 with the lower sequence 3.
        response = post_records(
            client, [sample(9, "2026-09-23T00:05:00Z", 20)]
        )
        assert response.status_code == 201
        events = response.json()["events"][0]["events"]
        interrupted = events[0]
        assert interrupted["status"] == "interrupted"
        assert interrupted["end_sequence"] == 3
        assert interrupted["end_at"] == "2026-09-23T00:08:00Z"
        assert interrupted["peak_sequence"] == 9
        assert interrupted["peak_turbidity"] == 20.0
        assert interrupted["sample_count"] == 3
        assert events[1]["status"] == "active"

        # Replays stay idempotent after the migration.
        replay = post_records(
            client, [sample(9, "2026-09-23T00:05:00Z", 20)], replay=True
        ).json()
        assert replay["inserted"] == 0
        assert replay["duplicate_identical"] == 1
        assert replay["events"] == response.json()["events"]

    # Every raw record survived the migration and the schema version moved.
    connection = sqlite3.connect(str(db_path))
    try:
        rows = connection.execute(
            """
            SELECT device_id, sequence, sampled_at, turbidity
            FROM samples
            ORDER BY sampled_at, sequence
            """
        ).fetchall()
        assert rows == [
            ("sensor-a", 1, "2026-09-23T00:00:00Z", 12.0),
            ("sensor-a", 9, "2026-09-23T00:05:00Z", 20.0),
            ("sensor-a", 3, "2026-09-23T00:08:00Z", 13.0),
            ("sensor-a", 7, "2026-09-23T00:30:00Z", 14.0),
        ]
        version = connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        assert version == (str(db.SCHEMA_VERSION),)
    finally:
        connection.close()


EXPECTED_EVENTS = [
    ("evt:sensor-a:1", "interrupted"),
    ("evt:sensor-a:7", "active"),
]


def _monkeypatch_settings(monkeypatch, db_path) -> None:
    monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
    monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
    monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", "600")


def _stored_meta(db_path, key: str) -> str | None:
    connection = sqlite3.connect(str(db_path))
    try:
        row = connection.execute(
            "SELECT value FROM schema_meta WHERE key = ?", (key,)
        ).fetchone()
        return None if row is None else str(row[0])
    finally:
        connection.close()


def _event_rows(db_path) -> list[tuple[str, str]]:
    connection = sqlite3.connect(str(db_path))
    try:
        return [
            (str(event_id), str(status))
            for event_id, status in connection.execute(
                "SELECT event_id, status FROM events ORDER BY start_sequence"
            ).fetchall()
        ]
    finally:
        connection.close()


def _assert_events_served(client: TestClient) -> None:
    body = client.get("/devices/sensor-a/events").json()
    assert body["total"] == 2
    assert [(event["event_id"], event["status"]) for event in body["events"]] == (
        EXPECTED_EVENTS
    )


def test_interrupted_upgrade_recovers_events_from_raw_samples(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "water.db"
    _create_legacy_database(db_path)
    _monkeypatch_settings(monkeypatch, db_path)

    # Simulate a crash inside the upgrade window: the schema was rebuilt and
    # the derived events dropped, but the process exited before the service
    # recomputed the events from the raw samples.
    connection = db.connect(db_path)
    try:
        assert db.initialize(connection) is True
    finally:
        connection.close()

    # The interrupted upgrade must not look complete: the stored version is
    # still the legacy one and the events table is empty.
    assert _stored_meta(db_path, "schema_version") == "2"
    assert _event_rows(db_path) == []

    from app.main import app

    # The next start replays the migration and rebuilds every derived event
    # from the preserved raw samples.
    with TestClient(app) as client:
        _assert_events_served(client)

    assert _stored_meta(db_path, "schema_version") == str(db.SCHEMA_VERSION)
    assert _event_rows(db_path) == EXPECTED_EVENTS

    # A further restart must not drop or recompute the recovered events.
    with TestClient(app) as client:
        _assert_events_served(client)
    assert _event_rows(db_path) == EXPECTED_EVENTS


def test_failed_event_rebuild_does_not_complete_upgrade(tmp_path, monkeypatch):
    db_path = tmp_path / "water.db"
    _create_legacy_database(db_path)
    _monkeypatch_settings(monkeypatch, db_path)

    from app.main import app
    from app.service import WaterQualityService

    original = WaterQualityService._rebuild_device

    def crash(self, device_id):
        raise RuntimeError("simulated power loss during event rebuild")

    monkeypatch.setattr(WaterQualityService, "_rebuild_device", crash)
    with pytest.raises(RuntimeError):
        with TestClient(app):
            pass

    # Nothing of the half-finished upgrade committed: the stored version is
    # still the legacy one, so the next start must replay the migration.
    assert _stored_meta(db_path, "schema_version") == "2"

    monkeypatch.setattr(WaterQualityService, "_rebuild_device", original)
    with TestClient(app) as client:
        _assert_events_served(client)
    assert _stored_meta(db_path, "schema_version") == str(db.SCHEMA_VERSION)
    assert _event_rows(db_path) == EXPECTED_EVENTS
