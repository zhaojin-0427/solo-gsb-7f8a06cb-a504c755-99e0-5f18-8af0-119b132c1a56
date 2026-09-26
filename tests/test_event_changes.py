from __future__ import annotations

import sqlite3

import pytest
from fastapi.testclient import TestClient

from app import db
from app.service import WaterQualityService
from tests.conftest import post_records


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


def get_changes(client: TestClient, after: int = 0, limit: int | None = None):
    url = f"/event-changes?after={after}"
    if limit is not None:
        url += f"&limit={limit}"
    response = client.get(url)
    assert response.status_code == 200
    return response.json()


def post_correction(
    client: TestClient,
    sequence: int,
    old_turbidity: float,
    new_turbidity: float,
    *,
    device: str = "sensor-a",
    old_at: str = "2026-09-23T08:00:00Z",
    new_at: str | None = None,
):
    return client.post(
        "/records/correction",
        json={
            "device_id": device,
            "sequence": sequence,
            "old_sampled_at": old_at,
            "old_turbidity": old_turbidity,
            "new_sampled_at": new_at if new_at is not None else old_at,
            "new_turbidity": new_turbidity,
        },
    )


def test_empty_change_stream_reports_zero_head_and_keeps_cursor(client):
    body = get_changes(client)
    assert body == {
        "after": 0,
        "limit": 100,
        "next_cursor": 0,
        "head_change_id": 0,
        "changes": [],
    }

    # A cursor past the head is echoed unchanged: nothing exists yet.
    body = get_changes(client, after=42)
    assert body["changes"] == []
    assert body["next_cursor"] == 42
    assert body["head_change_id"] == 0


def test_first_write_emits_ordered_upserts_with_full_events(client):
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 4),
            sample(2, "2026-09-23T08:01:00Z", 12),
            sample(3, "2026-09-23T08:02:00Z", 5),
            sample(4, "2026-09-23T08:30:00Z", 20),
        ],
    )
    events = response.json()["events"][0]["events"]
    assert [event["event_id"] for event in events] == [
        "evt:sensor-a:2",
        "evt:sensor-a:4",
    ]

    body = get_changes(client)
    assert [change["change_id"] for change in body["changes"]] == [1, 2]
    assert body["next_cursor"] == 2
    assert body["head_change_id"] == 2
    first, second = body["changes"]
    assert first["change_type"] == "upsert"
    assert first["event"] == events[0]
    assert second["change_type"] == "upsert"
    assert second["event"] == events[1]
    # No delete payloads leak into upsert rows.
    assert all(change["event"] is not None for change in body["changes"])


def test_identical_replay_emits_no_changes(client):
    payload = [
        sample(1, "2026-09-23T08:00:00Z", 12),
        sample(2, "2026-09-23T08:01:00Z", 5),
    ]
    post_records(client, payload)
    assert get_changes(client)["head_change_id"] == 1

    post_records(client, payload, replay=True)
    body = get_changes(client, after=1)
    assert body["changes"] == []
    assert body["next_cursor"] == 1
    assert body["head_change_id"] == 1

    # Re-submitting through the write endpoint is likewise a no-op.
    post_records(client, payload)
    assert get_changes(client, after=1)["changes"] == []


def test_late_recompute_emits_upsert_for_changed_and_delete_for_gone(client):
    # Two high samples far apart: interrupted + active (two events).
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(4, "2026-09-23T00:30:00Z", 15),
        ],
    )
    assert get_changes(client)["head_change_id"] == 2

    # Filling the gap merges the two events into one: the second event_id
    # disappears (delete), the surviving event carries new fields (upsert).
    response = post_records(
        client,
        [
            sample(2, "2026-09-23T00:10:00Z", 11),
            sample(3, "2026-09-23T00:20:00Z", 8),
        ],
    )
    merged = response.json()["events"][0]["events"]
    assert len(merged) == 1
    assert merged[0]["event_id"] == "evt:sensor-a:1"

    body = get_changes(client, after=2)
    assert body["next_cursor"] == 4
    upsert, delete = body["changes"]
    # Sorted by event_id: evt:sensor-a:1 before evt:sensor-a:4.
    assert upsert["change_id"] == 3
    assert upsert["change_type"] == "upsert"
    assert upsert["event_id"] == "evt:sensor-a:1"
    assert upsert["event"] == merged[0]
    assert upsert["event"]["sample_count"] == 4
    assert delete["change_id"] == 4
    assert delete["change_type"] == "delete"
    assert delete["event_id"] == "evt:sensor-a:4"
    assert delete["event"] is None


def test_correction_that_removes_event_emits_delete_only(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12),
            sample(2, "2026-09-23T08:01:00Z", 5),
        ],
    )
    head_after_write = get_changes(client)["head_change_id"]
    assert head_after_write == 1

    response = post_correction(client, sequence=1, old_turbidity=12, new_turbidity=4)
    assert response.status_code == 200
    assert response.json()["events"] == []

    body = get_changes(client, after=head_after_write)
    assert len(body["changes"]) == 1
    change = body["changes"][0]
    assert change["change_id"] == head_after_write + 1
    assert change["change_type"] == "delete"
    assert change["event_id"] == "evt:sensor-a:1"
    assert change["event"] is None


def test_correction_that_revives_event_emits_full_upsert(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 4)])
    assert get_changes(client)["changes"] == []

    response = post_correction(client, sequence=1, old_turbidity=4, new_turbidity=12)
    assert response.status_code == 200
    event = response.json()["events"][0]

    body = get_changes(client)
    assert body["head_change_id"] == 1
    change = body["changes"][0]
    assert change["change_type"] == "upsert"
    assert change["event_id"] == "evt:sensor-a:1"
    assert change["event"] == event


def test_unchanged_correction_and_rejected_corrections_emit_nothing(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])
    head = get_changes(client)["head_change_id"]
    assert head == 1

    # New values identical to stored values: allowed, but the derived event
    # does not change, so no change-stream entry is appended.
    response = post_correction(client, sequence=1, old_turbidity=12, new_turbidity=12)
    assert response.status_code == 200
    assert get_changes(client, after=head)["changes"] == []
    assert get_changes(client)["head_change_id"] == head

    # 404: unknown record.
    assert (
        post_correction(
            client, sequence=99, old_turbidity=12, new_turbidity=20
        ).status_code
        == 404
    )
    # 409: stale old value.
    assert (
        post_correction(client, sequence=1, old_turbidity=11, new_turbidity=20).status_code
        == 409
    )
    assert get_changes(client)["head_change_id"] == head
    assert get_changes(client, after=head)["changes"] == []


def test_change_ids_are_sorted_by_device_then_event(client):
    # Devices are mentioned in reverse order within the batch; the resulting
    # change rows must still be assigned by (device_id, event_id).
    response = client.post(
        "/records",
        json={
            "records": [
                sample(2, "2026-09-23T08:00:00Z", 12, "sensor-c"),
                sample(1, "2026-09-23T08:00:00Z", 12, "sensor-a"),
                sample(1, "2026-09-23T08:00:00Z", 12, "sensor-b"),
            ]
        },
    )
    assert response.status_code == 201

    body = get_changes(client)
    assert [(c["change_id"], c["device_id"], c["event_id"]) for c in body["changes"]] == [
        (1, "sensor-a", "evt:sensor-a:1"),
        (2, "sensor-b", "evt:sensor-b:1"),
        (3, "sensor-c", "evt:sensor-c:2"),
    ]


def test_pagination_follows_cursors_and_empty_page_keeps_cursor(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:05:00Z", 5),
            sample(3, "2026-09-23T00:30:00Z", 20),
            sample(4, "2026-09-23T00:35:00Z", 5),
        ],
    )
    head = get_changes(client)["head_change_id"]
    assert head == 2

    first = get_changes(client, after=0, limit=1)
    assert [c["change_id"] for c in first["changes"]] == [1]
    assert first["next_cursor"] == 1
    assert first["head_change_id"] == head

    second = get_changes(client, after=first["next_cursor"], limit=1)
    assert [c["change_id"] for c in second["changes"]] == [2]
    assert second["next_cursor"] == 2

    # Exhausted: the same cursor is echoed even though head is 2.
    empty = get_changes(client, after=2, limit=10)
    assert empty["changes"] == []
    assert empty["next_cursor"] == 2
    assert empty["head_change_id"] == 2

    # A newer commit becomes visible from the retained cursor.
    post_records(client, [sample(5, "2026-09-23T01:00:00Z", 30)])
    after_wait = get_changes(client, after=2, limit=10)
    assert [c["change_id"] for c in after_wait["changes"]] == [3]
    assert after_wait["next_cursor"] == 3
    assert after_wait["head_change_id"] == 3


def test_query_parameter_validation(client):
    assert client.get("/event-changes?after=-1").status_code == 422
    assert client.get("/event-changes?after=1.5").status_code == 422
    assert client.get("/event-changes?limit=0").status_code == 422
    assert client.get("/event-changes?limit=1001").status_code == 422
    assert client.get("/event-changes?limit=50&after=0").status_code == 200


def test_failed_batch_rolls_back_any_changes(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])
    head = get_changes(client)["head_change_id"]
    assert head == 1

    # A conflicting key within a second batch fails the whole request; the
    # batch also contains a brand-new device whose changes must not survive.
    response = client.post(
        "/records",
        json={
            "records": [
                sample(1, "2026-09-23T08:00:00Z", 20),
                sample(1, "2026-09-23T08:00:00Z", 20, "sensor-new"),
            ]
        },
    )
    assert response.status_code == 409

    body = get_changes(client)
    assert body["head_change_id"] == head
    assert body["changes"][-1]["event_id"] == "evt:sensor-a:1"

    # The conflicting sample was not stored either.
    replay = post_records(
        client, [sample(1, "2026-09-23T08:00:00Z", 12)], replay=True
    ).json()
    assert replay["duplicate_identical"] == 1


def test_failed_rebuild_rolls_back_change_rows(client, monkeypatch):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])
    head = get_changes(client)["head_change_id"]

    def crash(self, device_id):
        raise RuntimeError("simulated failure during event rebuild")

    monkeypatch.setattr(WaterQualityService, "_rebuild_device", crash)
    with pytest.raises(RuntimeError):
        post_records(client, [sample(2, "2026-09-23T08:01:00Z", 13)])
    monkeypatch.undo()

    # The rolled-back row id is not visible; the next successful append
    # reuses that sequence, so the committed stream stays gap-free.
    body = get_changes(client)
    assert body["head_change_id"] == head

    post_records(client, [sample(2, "2026-09-23T08:01:00Z", 5)])
    body = get_changes(client, after=head)
    assert [c["change_id"] for c in body["changes"]] == [head + 1]
    assert body["changes"][0]["change_type"] == "upsert"
    assert body["changes"][0]["event"]["status"] == "recovered"


def test_disappeared_event_can_reappear_append_only(client):
    # First the two far-apart high samples create two events ...
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(4, "2026-09-23T00:30:00Z", 15),
        ],
    )
    # ... then the connecting samples merge evt:...:4 away.
    post_records(
        client,
        [
            sample(2, "2026-09-23T00:10:00Z", 11),
            sample(3, "2026-09-23T00:20:00Z", 8),
        ],
    )
    deleted = get_changes(client)["changes"]
    assert any(c["event_id"] == "evt:sensor-a:4" and c["change_type"] == "delete"
               for c in deleted)

    # ... then a later correction opens the wide gap again, re-creating
    # exactly that event_id. It is a new upsert with a fresh change_id,
    # never an in-place edit of the old log row.
    response = post_correction(
        client, sequence=3, old_turbidity=8, new_turbidity=8,
        old_at="2026-09-23T00:20:00Z", new_at="2026-09-23T00:25:00Z",
    )
    assert response.status_code == 200
    events = response.json()["events"]
    assert [event["event_id"] for event in events] == [
        "evt:sensor-a:1",
        "evt:sensor-a:4",
    ]

    history = get_changes(client)["changes"]
    ids_for_4 = [c["change_id"] for c in history if c["event_id"] == "evt:sensor-a:4"]
    types_for_4 = [c["change_type"] for c in history if c["event_id"] == "evt:sensor-a:4"]
    assert types_for_4 == ["upsert", "delete", "upsert"]
    assert ids_for_4 == sorted(ids_for_4)
    assert len(ids_for_4) == len(set(ids_for_4))
    reappeared = history[-1]
    assert reappeared["event"] == events[1]


def test_snapshot_then_consume_protocol_converges(client):
    # Baseline events exist before a consumer starts.
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12),
            sample(2, "2026-09-23T08:01:00Z", 5),
        ],
    )

    # 1) Read the current head.
    head = get_changes(client)["head_change_id"]

    # 2) Take a snapshot of the events as of that water mark.
    snapshot = {
        event["event_id"]: {
            key: value for key, value in event.items() if key != "acknowledged"
        }
        for event in client.get("/devices/sensor-a/events").json()["events"]
    }

    # 3) Writes that race with the snapshot all become reachable from head.
    post_correction(client, sequence=1, old_turbidity=12, new_turbidity=4)
    post_correction(client, sequence=1, old_turbidity=4, new_turbidity=12)

    # Replay the stream from the water mark into the snapshot.
    cursor = head
    replica = dict(snapshot)
    while True:
        page = get_changes(client, after=cursor, limit=1)
        for change in page["changes"]:
            if change["change_type"] == "upsert":
                replica[change["event_id"]] = change["event"]
            else:
                replica.pop(change["event_id"], None)
        cursor = page["next_cursor"]
        if cursor >= page["head_change_id"] and not page["changes"]:
            break

    current = {
        event["event_id"]: {
            key: value for key, value in event.items() if key != "acknowledged"
        }
        for event in client.get("/devices/sensor-a/events").json()["events"]
    }
    assert replica == current
    assert "evt:sensor-a:1" in replica


# The events table as schema version 3 created it (same shape as v2 without
# the sequence-magnitude CHECKs).
LEGACY_V3_EVENTS_DDL = """
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
    CHECK (end_at IS NULL OR end_at >= start_at),
    CHECK ((end_sequence IS NULL) = (end_at IS NULL)),
    CHECK ((status = 'active') = (end_at IS NULL))
) WITHOUT ROWID;
"""


def _create_v3_database(path) -> None:
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
        connection.execute(LEGACY_V3_EVENTS_DDL)
        connection.executemany(
            "INSERT INTO schema_meta(key, value) VALUES (?, ?)",
            [
                ("schema_version", "3"),
                ("alarm_threshold", "10.0"),
                ("recovery_threshold", "5.0"),
                ("max_sample_gap_seconds", "600"),
            ],
        )
        connection.execute(
            """
            INSERT INTO samples (device_id, sequence, sampled_at, turbidity)
            VALUES ('sensor-a', 1, '2026-09-23T08:00:00Z', 12.0)
            """
        )
        connection.execute(
            """
            INSERT INTO events (
                event_id, device_id, status, start_sequence, end_sequence,
                start_at, end_at, peak_turbidity, peak_sequence, sample_count
            ) VALUES ('evt:sensor-a:1', 'sensor-a', 'active', 1, NULL,
                      '2026-09-23T08:00:00Z', NULL, 12.0, 1, 1)
            """
        )
        connection.commit()
    finally:
        connection.close()


def test_upgrade_bootstraps_empty_stream_then_emits_fresh_changes(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "water.db"
    _create_v3_database(db_path)
    monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
    monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
    monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", "600")

    from app.main import app

    with TestClient(app) as client:
        # The rebuild done by the upgrade itself is not a write through the
        # API, so the new change stream starts empty.
        body = get_changes(client)
        assert body["head_change_id"] == 0
        assert body["changes"] == []

        # Derived events were rebuilt from the preserved raw samples.
        events = client.get("/devices/sensor-a/events").json()["events"]
        assert [event["event_id"] for event in events] == ["evt:sensor-a:1"]

        # Only API writes begin the stream.
        post_records(client, [sample(2, "2026-09-23T08:01:00Z", 5)])
        body = get_changes(client)
        assert [c["change_id"] for c in body["changes"]] == [1]
        assert body["changes"][0]["change_type"] == "upsert"
        assert body["changes"][0]["event"]["status"] == "recovered"

    version = sqlite3.connect(str(db_path)).execute(
        "SELECT value FROM schema_meta WHERE key = 'schema_version'"
    ).fetchone()
    assert version == (str(db.SCHEMA_VERSION),)


def _create_v4_database_with_ack(path) -> None:
    """Build a v4 database and insert an acknowledgement row directly."""
    _create_v3_database(path)  # same events/event_changes shape, then bump it
    connection = sqlite3.connect(str(path))
    try:
        connection.execute(
            """
            CREATE TABLE event_changes (
                change_id INTEGER PRIMARY KEY,
                device_id TEXT NOT NULL,
                event_id TEXT NOT NULL,
                change_type TEXT NOT NULL
                    CHECK (change_type IN ('upsert', 'delete')),
                event_json TEXT,
                CHECK (
                    (change_type = 'upsert' AND event_json IS NOT NULL)
                    OR (change_type = 'delete' AND event_json IS NULL)
                )
            )
            """
        )
        connection.execute(
            "UPDATE schema_meta SET value = '4' WHERE key = 'schema_version'"
        )
        connection.execute(
            "CREATE TABLE event_acks (event_id TEXT NOT NULL PRIMARY KEY)"
        )
        connection.execute(
            "INSERT INTO event_acks (event_id) VALUES ('evt:sensor-a:1')"
        )
        connection.commit()
    finally:
        connection.close()


def test_upgrade_v4_to_v5_keeps_events_and_acknowledgements(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "water.db"
    _create_v4_database_with_ack(db_path)
    monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
    monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
    monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", "600")

    from app.main import app

    with TestClient(app) as client:
        # The events table shape is current since v3, so the upgrade does
        # not rebuild events: the v4 acknowledgement survives untouched and
        # the startup recomputation itself writes no change-stream rows.
        events = client.get("/devices/sensor-a/events").json()["events"]
        assert [event["event_id"] for event in events] == ["evt:sensor-a:1"]
        assert events[0]["acknowledged"] is True
        body = get_changes(client)
        assert body["head_change_id"] == 0
        assert body["changes"] == []

    # The marker advanced to v5 and the acknowledgement row is persistent.
    connection = sqlite3.connect(str(db_path))
    try:
        version = connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone()
        assert version == (str(db.SCHEMA_VERSION),)
        assert connection.execute("SELECT event_id FROM event_acks").fetchall() == [
            ("evt:sensor-a:1",)
        ]
    finally:
        connection.close()


def test_upgrade_v4_with_changed_settings_invalidates_stale_ack(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "water.db"
    _create_v4_database_with_ack(db_path)
    monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
    monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
    monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
    # The single active event stays the same under any positive gap, so to
    # change a field at startup lower the alarm threshold instead: the
    # stored event was computed at 10.0 and fields are unchanged for that
    # one sample either way. Use a gap change plus a second sample instead.
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", "600")

    from app.main import app

    with TestClient(app) as client:
        # Add a far-apart high sample under the v5 code (gap 600 splits),
        # acknowledge the resulting second event, then restart with a wider
        # gap so the startup recomputation merges it away.
        post_records(client, [sample(2, "2026-09-23T08:30:00Z", 13)])
        events = client.get("/devices/sensor-a/events").json()["events"]
        assert [event["event_id"] for event in events] == [
            "evt:sensor-a:1",
            "evt:sensor-a:2",
        ]
        for event in events:
            payload = {k: v for k, v in event.items() if k != "acknowledged"}
            response = client.post(
                f"/devices/sensor-a/events/{event['event_id']}/ack",
                json={"expected_event": payload},
            )
            assert response.status_code == 200

    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", "3600")
    with TestClient(app) as client:
        events = client.get("/devices/sensor-a/events").json()["events"]
        assert len(events) == 1
        assert events[0]["status"] == "active"
        # Both pre-change acks were revoked inside the startup transaction.
        assert all(event["acknowledged"] is False for event in events)
        rows = sqlite3.connect(str(db_path)).execute(
            "SELECT event_id FROM event_acks"
        ).fetchall()
        assert rows == []
