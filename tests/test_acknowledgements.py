from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import (
    ack_event,
    get_events,
    post_records,
    strip_acknowledged,
)


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


def make_events(client: TestClient, device: str = "sensor-a") -> list[dict]:
    # One recovered event (seq 1..3) and one active event (seq 5).
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12, device),
            sample(2, "2026-09-23T08:01:00Z", 8, device),
            sample(3, "2026-09-23T08:02:00Z", 5, device),
            sample(5, "2026-09-23T08:30:00Z", 13, device),
        ],
    )
    return get_events(client, device)["events"]


def ack_rows(client: TestClient) -> list[str]:
    rows = client.app.state.connection.execute(
        "SELECT event_id FROM event_acks ORDER BY event_id"
    ).fetchall()
    return [str(row[0]) for row in rows]


def test_events_carry_false_acknowledged_by_default(client):
    events = make_events(client)
    assert len(events) == 2
    assert all(event["acknowledged"] is False for event in events)


def test_acknowledge_sets_flag_and_is_idempotent(client):
    events = make_events(client)
    target = events[0]
    assert target["event_id"] == "evt:sensor-a:1"

    response = ack_event(client, target)
    assert response.status_code == 200
    body = response.json()
    assert body["acknowledged"] is True
    # The response carries the complete event, same as the query.
    assert body["event_id"] == target["event_id"]
    assert strip_acknowledged([body]) == strip_acknowledged([target])

    # Repeating the exact same version is idempotent, still 200.
    repeat = ack_event(client, target)
    assert repeat.status_code == 200
    assert repeat.json()["acknowledged"] is True

    events = get_events(client)["events"]
    assert [event["acknowledged"] for event in events] == [True, False]


def test_acknowledge_unknown_event_returns_404(client):
    make_events(client)
    expected = {
        "event_id": "evt:sensor-a:99",
        "device_id": "sensor-a",
        "status": "active",
        "start_sequence": 99,
        "end_sequence": None,
        "start_at": "2026-09-23T09:00:00Z",
        "end_at": None,
        "peak_turbidity": 42.0,
        "peak_sequence": 99,
        "sample_count": 1,
    }
    response = client.post(
        "/devices/sensor-a/events/evt:sensor-a:99/ack",
        json={"expected_event": expected},
    )
    assert response.status_code == 404


def test_acknowledge_unknown_device_returns_404(client):
    make_events(client)
    target = get_events(client)["events"][0]
    response = ack_event(client, target, device="sensor-other")
    assert response.status_code == 404


def test_acknowledge_with_stale_version_returns_409(client):
    events = make_events(client)
    target = events[1]  # active event evt:sensor-a:5

    # A new sample extends the still-active event: same event_id, changed
    # fields (peak / count / status / end).
    post_records(client, [sample(6, "2026-09-23T08:31:00Z", 5)])

    response = ack_event(client, target)
    assert response.status_code == 409
    # No acknowledgement was stored for the stale attempt.
    refreshed = get_events(client)["events"]
    assert all(event["acknowledged"] is False for event in refreshed)

    # Re-reading and acknowledging the current version succeeds.
    current = next(
        event for event in refreshed if event["event_id"] == "evt:sensor-a:5"
    )
    assert current["status"] == "recovered"
    assert ack_event(client, current).status_code == 200


def test_acknowledge_rejects_path_body_identity_mismatch(client):
    events = make_events(client)
    other = events[1]

    # Body identifies a different event than the path.
    response = ack_event(
        client,
        other,
        event_id="evt:sensor-a:1",
    )
    assert response.status_code == 409
    assert all(
        event["acknowledged"] is False for event in get_events(client)["events"]
    )

    response = ack_event(client, other, device="sensor-b")
    assert response.status_code == 404


def test_acknowledge_rejects_extra_or_missing_fields(client):
    events = make_events(client)
    target = events[0]

    # Unknown top-level / event fields are rejected.
    response = client.post(
        f"/devices/sensor-a/events/{target['event_id']}/ack",
        json={"expected_event": {**strip_acknowledged([target])[0], "x": 1}},
    )
    assert response.status_code == 422

    partial = {
        key: value
        for key, value in target.items()
        if key not in ("acknowledged", "sample_count")
    }
    response = client.post(
        f"/devices/sensor-a/events/{target['event_id']}/ack",
        json={"expected_event": partial},
    )
    assert response.status_code == 422

    # Sending the acknowledged flag back inside expected_event is itself an
    # unknown field: the expected event must carry event fields only.
    response = client.post(
        f"/devices/sensor-a/events/{target['event_id']}/ack",
        json={"expected_event": target},
    )
    assert response.status_code == 422


def test_acknowledge_accepts_equivalent_utc_offset(client):
    target = make_events(client)[0]
    expected = strip_acknowledged([target])[0]
    # 10:00:00+02:00 is the same instant as 08:00:00Z.
    expected["start_at"] = "2026-09-23T10:00:00+02:00"
    response = client.post(
        f"/devices/sensor-a/events/{target['event_id']}/ack",
        json={"expected_event": expected},
    )
    assert response.status_code == 200
    assert response.json()["acknowledged"] is True


def test_acknowledgement_produces_no_change_stream_entries(client):
    target = make_events(client)[0]

    changes = client.get("/event-changes").json()
    head = changes["head_change_id"]
    assert head > 0

    assert ack_event(client, target).status_code == 200
    body = client.get(f"/event-changes?after={head}").json()
    assert body["changes"] == []
    assert body["head_change_id"] == head
    assert body["next_cursor"] == head


def test_changed_event_invalidates_ack_in_same_transaction(client):
    target = make_events(client)[1]  # active evt:...:5
    assert ack_event(client, target).status_code == 200
    assert get_events(client)["events"][1]["acknowledged"] is True

    # A recovery sample changes that event's fields in the write
    # transaction; the acknowledgement must be revoked in that same
    # transaction, so the committed query already shows it unacknowledged.
    post_records(client, [sample(6, "2026-09-23T08:31:00Z", 5)])
    events = get_events(client)["events"]
    changed = next(event for event in events if event["event_id"] == "evt:sensor-a:5")
    assert changed["acknowledged"] is False
    # The untouched recovered event was never acknowledged anyway.
    assert all(event["acknowledged"] is False for event in events)


def test_disappeared_event_invalidates_ack_but_survivors_keep_theirs(client):
    events = make_events(client)
    first, second = events
    assert ack_event(client, first).status_code == 200
    assert ack_event(client, second).status_code == 200

    # Correcting the seed high sample below the alarm threshold removes
    # evt:sensor-a:1 entirely ...
    response = client.post(
        "/records/correction",
        json={
            "device_id": "sensor-a",
            "sequence": 1,
            "old_sampled_at": "2026-09-23T08:00:00Z",
            "old_turbidity": 12,
            "new_sampled_at": "2026-09-23T08:00:00Z",
            "new_turbidity": 4,
        },
    )
    assert response.status_code == 200
    remaining = get_events(client)["events"]
    assert [event["event_id"] for event in remaining] == ["evt:sensor-a:5"]
    # ... only that acknowledgement is deleted; the surviving event of the
    # same device was unchanged and stays acknowledged.
    assert remaining[0]["acknowledged"] is True
    assert ack_rows(client) == ["evt:sensor-a:5"]


def test_other_device_acknowledgements_are_unaffected(client):
    events_a = make_events(client, "sensor-a")
    events_b = make_events(client, "sensor-b")
    assert ack_event(client, events_a[0]).status_code == 200
    assert ack_event(client, events_b[0]).status_code == 200

    # Rewrite sensor-a's event via correction; sensor-b must stay acked.
    response = client.post(
        "/records/correction",
        json={
            "device_id": "sensor-a",
            "sequence": 1,
            "old_sampled_at": "2026-09-23T08:00:00Z",
            "old_turbidity": 12,
            "new_sampled_at": "2026-09-23T08:00:00Z",
            "new_turbidity": 4,
        },
    )
    assert response.status_code == 200
    a_events = get_events(client, "sensor-a")["events"]
    # evt:sensor-a:1 was deleted and the surviving evt:sensor-a:5 was never
    # acknowledged, so sensor-a reports no acknowledgements at all.
    assert all(event["acknowledged"] is False for event in a_events)
    # sensor-b's acknowledged event is untouched; its other (never acked)
    # event is unaffected too.
    b_events = {
        event["event_id"]: event for event in get_events(client, "sensor-b")["events"]
    }
    assert b_events["evt:sensor-b:1"]["acknowledged"] is True
    assert b_events["evt:sensor-b:5"]["acknowledged"] is False


def test_ack_stays_invalid_when_same_event_id_reappears(client):
    # Far-apart highs create two events; acknowledge the later one.
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(4, "2026-09-23T00:30:00Z", 15),
        ],
    )
    later = get_events(client)["events"][1]
    assert later["event_id"] == "evt:sensor-a:4"
    assert ack_event(client, later).status_code == 200

    # Filling the gap merges the later event away: ack invalidated.
    post_records(
        client,
        [
            sample(2, "2026-09-23T00:10:00Z", 11),
            sample(3, "2026-09-23T00:20:00Z", 8),
        ],
    )
    assert get_events(client)["total"] == 1
    assert get_events(client)["events"][0]["acknowledged"] is False

    # A correction reopens the gap and the same event_id reappears: it
    # must come back unacknowledged.
    response = client.post(
        "/records/correction",
        json={
            "device_id": "sensor-a",
            "sequence": 3,
            "old_sampled_at": "2026-09-23T00:20:00Z",
            "old_turbidity": 8,
            "new_sampled_at": "2026-09-23T00:25:00Z",
            "new_turbidity": 8,
        },
    )
    assert response.status_code == 200
    events = get_events(client)["events"]
    assert [event["event_id"] for event in events] == [
        "evt:sensor-a:1",
        "evt:sensor-a:4",
    ]
    assert all(event["acknowledged"] is False for event in events)

    # It can be acknowledged afresh now that it visibly exists again.
    reappeared = events[1]
    assert ack_event(client, reappeared).status_code == 200
    assert get_events(client)["events"][1]["acknowledged"] is True


def test_identical_replay_keeps_acknowledgement(client):
    target = make_events(client)[0]
    assert ack_event(client, target).status_code == 200

    payload = [
        sample(1, "2026-09-23T08:00:00Z", 12),
        sample(2, "2026-09-23T08:01:00Z", 8),
        sample(3, "2026-09-23T08:02:00Z", 5),
        sample(5, "2026-09-23T08:30:00Z", 13),
    ]
    post_records(client, payload, replay=True)
    events = get_events(client)["events"]
    assert [event["acknowledged"] for event in events] == [True, False]


def test_acknowledgement_persists_across_restart(tmp_path, monkeypatch):
    db_path = tmp_path / "water.db"

    def fresh_client() -> TestClient:
        monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
        monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
        monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
        monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", "600")
        from app.main import app

        return TestClient(app)

    with fresh_client() as first:
        target = make_events(first)[0]
        assert ack_event(first, target).status_code == 200

    # Restart with the same configuration: no event fields change, so the
    # acknowledgement survives and the query reports it.
    with fresh_client() as second:
        events = get_events(second)["events"]
        assert [event["acknowledged"] for event in events] == [True, False]


def test_startup_config_change_invalidates_affected_acks(tmp_path, monkeypatch):
    db_path = tmp_path / "water.db"

    def client_with(gap_seconds: int) -> TestClient:
        monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
        monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
        monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
        monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", str(gap_seconds))
        from app.main import app

        return TestClient(app)

    payload = [
        sample(1, "2026-09-23T00:00:00Z", 12),
        sample(2, "2026-09-23T00:30:00Z", 15),
    ]
    with client_with(600) as first:
        events = post_records(first, payload).json()["events"][0]["events"]
        assert [event["status"] for event in events] == [
            "interrupted",
            "active",
        ]
        for event in events:
            assert ack_event(first, event).status_code == 200
        assert all(
            event["acknowledged"] is True
            for event in get_events(first)["events"]
        )

    # Widening the gap at startup merges the two segments: the events
    # change/disappear during the startup recomputation, so their acks must
    # be revoked in that same transaction, before any request is served.
    with client_with(3600) as second:
        events = get_events(second)["events"]
        assert len(events) == 1
        assert events[0]["status"] == "active"
        assert events[0]["acknowledged"] is False
        # Both invalidated ack rows were deleted, not just unflagged.
        assert ack_rows(second) == []


def test_status_filter_reports_acknowledged_flag(client):
    target = make_events(client)[0]
    assert target["status"] == "recovered"
    assert ack_event(client, target).status_code == 200

    body = get_events(client, status="recovered")
    assert body["total"] == 1
    assert body["events"][0]["event_id"] == target["event_id"]
    assert body["events"][0]["acknowledged"] is True

    body = get_events(client, status="active")
    assert body["events"][0]["acknowledged"] is False


def test_ack_then_another_change_still_no_change_stream_rows_for_ack(client):
    target = make_events(client)[0]
    assert ack_event(client, target).status_code == 200
    head = client.get("/event-changes").json()["head_change_id"]

    # Changing the event appends the usual event upsert, but never an
    # acknowledgement record; the payload still has no acknowledged field.
    response = client.post(
        "/records/correction",
        json={
            "device_id": "sensor-a",
            "sequence": 2,
            "old_sampled_at": "2026-09-23T08:01:00Z",
            "old_turbidity": 8,
            "new_sampled_at": "2026-09-23T08:01:00Z",
            "new_turbidity": 20,
        },
    )
    assert response.status_code == 200
    changes = client.get(f"/event-changes?after={head}").json()["changes"]
    assert changes, "the field change itself must still be replicated"
    upserts = [change for change in changes if change["event"] is not None]
    assert upserts
    for change in changes:
        if change["event"] is not None:
            assert "acknowledged" not in change["event"]
        assert change["change_type"] in ("upsert", "delete")
