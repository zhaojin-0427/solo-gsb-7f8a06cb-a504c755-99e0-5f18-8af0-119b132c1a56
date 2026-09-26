from __future__ import annotations

import pytest

from app.service import WaterQualityService
from tests.conftest import post_records, strip_acknowledged


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


def correction(
    device: str = "sensor-a",
    sequence: int = 1,
    old_at: str = "2026-09-23T08:00:00Z",
    old_turbidity: float = 12.0,
    new_at: str = "2026-09-23T08:00:00Z",
    new_turbidity: float = 12.0,
) -> dict:
    return {
        "device_id": device,
        "sequence": sequence,
        "old_sampled_at": old_at,
        "old_turbidity": old_turbidity,
        "new_sampled_at": new_at,
        "new_turbidity": new_turbidity,
    }


def test_correction_across_alarm_threshold_recomputes_events(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12),
            sample(2, "2026-09-23T08:01:00Z", 13),
            sample(3, "2026-09-23T08:02:00Z", 5),
        ],
    )

    # Lower the peak into the hysteresis band: the event keeps its bounds but
    # the peak moves to the remaining above-alarm sample.
    response = client.post(
        "/records/correction",
        json=correction(
            sequence=2,
            old_at="2026-09-23T08:01:00Z",
            old_turbidity=13,
            new_at="2026-09-23T08:01:00Z",
            new_turbidity=8,
        ),
    )
    assert response.status_code == 200
    body = response.json()
    assert body["device_id"] == "sensor-a"
    assert body["sequence"] == 2
    assert body["sampled_at"] == "2026-09-23T08:01:00Z"
    assert body["turbidity"] == 8.0
    events = body["events"]
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "recovered"
    assert event["start_sequence"] == 1
    assert event["end_sequence"] == 3
    assert event["peak_sequence"] == 1
    assert event["peak_turbidity"] == 12.0
    assert event["sample_count"] == 3

    # The query path reflects the recomputation immediately (GET adds the
    # acknowledged flag, which write responses do not carry).
    assert strip_acknowledged(
        client.get("/devices/sensor-a/events").json()["events"]
    ) == events

    # Dropping the last above-alarm sample below the alarm threshold removes
    # the derived event entirely.
    response = client.post(
        "/records/correction",
        json=correction(
            sequence=1,
            old_at="2026-09-23T08:00:00Z",
            old_turbidity=12,
            new_at="2026-09-23T08:00:00Z",
            new_turbidity=4,
        ),
    )
    assert response.status_code == 200
    assert response.json()["events"] == []
    assert client.get("/devices/sensor-a/events").json()["total"] == 0


def test_correction_across_recovery_threshold_splits_event(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12),
            sample(2, "2026-09-23T08:01:00Z", 8),
            sample(3, "2026-09-23T08:02:00Z", 14),
        ],
    )

    # Correcting the hysteresis-band sample to a recovery observation splits
    # the single active event at the newly exposed recovery boundary.
    response = client.post(
        "/records/correction",
        json=correction(
            sequence=2,
            old_at="2026-09-23T08:01:00Z",
            old_turbidity=8,
            new_at="2026-09-23T08:01:00Z",
            new_turbidity=4,
        ),
    )
    assert response.status_code == 200
    events = response.json()["events"]
    assert [event["status"] for event in events] == ["recovered", "active"]
    assert events[0]["start_sequence"] == 1
    assert events[0]["end_sequence"] == 2
    assert events[0]["end_at"] == "2026-09-23T08:01:00Z"
    assert events[0]["sample_count"] == 2
    assert events[1]["start_sequence"] == 3
    assert events[1]["peak_sequence"] == 3
    assert events[1]["peak_turbidity"] == 14.0
    assert events[1]["sample_count"] == 1


def test_correction_moves_sample_time_and_resegments(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:30:00Z", 13),
        ],
    )
    # The 30-minute gap exceeds the 600-second limit: interrupted + active.

    # Moving the second sample earlier closes the gap; the tentative segments
    # merge into one continuous event.
    response = client.post(
        "/records/correction",
        json=correction(
            sequence=2,
            old_at="2026-09-23T00:30:00Z",
            old_turbidity=13,
            new_at="2026-09-23T00:05:00Z",
            new_turbidity=13,
        ),
    )
    assert response.status_code == 200
    events = response.json()["events"]
    assert len(events) == 1
    merged = events[0]
    assert merged["status"] == "active"
    assert merged["event_id"] == "evt:sensor-a:1"
    assert merged["start_sequence"] == 1
    assert merged["end_sequence"] is None
    assert merged["sample_count"] == 2
    assert merged["peak_sequence"] == 2
    assert merged["peak_turbidity"] == 13.0

    # Moving it back out beyond the gap limit splits the event again.
    response = client.post(
        "/records/correction",
        json=correction(
            sequence=2,
            old_at="2026-09-23T00:05:00Z",
            old_turbidity=13,
            new_at="2026-09-23T00:30:00Z",
            new_turbidity=13,
        ),
    )
    assert response.status_code == 200
    events = response.json()["events"]
    assert [event["status"] for event in events] == ["interrupted", "active"]
    assert events[0]["end_sequence"] == 1
    assert events[0]["end_at"] == "2026-09-23T00:00:00Z"
    assert events[1]["start_sequence"] == 2
    assert events[1]["start_at"] == "2026-09-23T00:30:00Z"

    get_body = client.get("/devices/sensor-a/events").json()
    assert get_body["total"] == 2
    assert strip_acknowledged(get_body["events"]) == events


def test_correction_of_unknown_record_returns_404_and_changes_nothing(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])
    before = client.get("/devices/sensor-a/events").json()

    response = client.post(
        "/records/correction",
        json=correction(
            sequence=99,
            old_at="2026-09-23T08:00:00Z",
            old_turbidity=12,
            new_at="2026-09-23T08:00:00Z",
            new_turbidity=20,
        ),
    )
    assert response.status_code == 404

    # Derived events and the stored record are untouched.
    assert client.get("/devices/sensor-a/events").json() == before
    replay = post_records(
        client, [sample(1, "2026-09-23T08:00:00Z", 12)], replay=True
    ).json()
    assert replay["inserted"] == 0
    assert replay["duplicate_identical"] == 1


def test_correction_with_mismatched_old_values_returns_409_and_keeps_data(
    client,
):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])
    before = client.get("/devices/sensor-a/events").json()

    wrong_turbidity = client.post(
        "/records/correction",
        json=correction(
            sequence=1,
            old_at="2026-09-23T08:00:00Z",
            old_turbidity=11,
            new_at="2026-09-23T08:00:00Z",
            new_turbidity=20,
        ),
    )
    assert wrong_turbidity.status_code == 409

    wrong_time = client.post(
        "/records/correction",
        json=correction(
            sequence=1,
            old_at="2026-09-23T08:00:01Z",
            old_turbidity=12,
            new_at="2026-09-23T08:00:00Z",
            new_turbidity=20,
        ),
    )
    assert wrong_time.status_code == 409

    # Neither failed attempt modified the record or the derived events.
    assert client.get("/devices/sensor-a/events").json() == before
    replay = post_records(
        client, [sample(1, "2026-09-23T08:00:00Z", 12)], replay=True
    ).json()
    assert replay["duplicate_identical"] == 1


def test_correction_compares_old_values_in_utc(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])

    # 10:00:00+02:00 is the same instant as the stored 08:00:00Z, so the old
    # value matches under the UTC comparison used by ingest dedup.
    response = client.post(
        "/records/correction",
        json=correction(
            sequence=1,
            old_at="2026-09-23T10:00:00+02:00",
            old_turbidity=12,
            new_at="2026-09-23T09:05:00+02:00",
            new_turbidity=12,
        ),
    )
    assert response.status_code == 200
    # The new timestamp is persisted normalized to UTC.
    assert response.json()["sampled_at"] == "2026-09-23T07:05:00Z"

    event = client.get("/devices/sensor-a/events").json()["events"][0]
    assert event["start_at"] == "2026-09-23T07:05:00Z"


def test_correction_requires_timezone_aware_timestamps(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])

    payload = correction(sequence=1, new_at="2026-09-23T08:05:00")
    assert client.post("/records/correction", json=payload).status_code == 422

    payload = correction(sequence=1, old_at="2026-09-23T08:00:00")
    assert client.post("/records/correction", json=payload).status_code == 422


def test_correction_rejects_unknown_fields_and_invalid_turbidity(client):
    payload = correction(sequence=1)
    payload["new_device_id"] = "sensor-b"
    assert client.post("/records/correction", json=payload).status_code == 422

    payload = correction(sequence=1, new_turbidity=-1)
    assert client.post("/records/correction", json=payload).status_code == 422


def test_corrected_values_become_the_dedup_reference(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])
    response = client.post(
        "/records/correction",
        json=correction(
            sequence=1,
            old_at="2026-09-23T08:00:00Z",
            old_turbidity=12,
            new_at="2026-09-23T08:00:00Z",
            new_turbidity=20,
        ),
    )
    assert response.status_code == 200

    # The correction is not a write: resubmitting the superseded observation
    # through the normal ingest path is a conflict, not a duplicate.
    stale = client.post(
        "/records",
        json={"records": [sample(1, "2026-09-23T08:00:00Z", 12)]},
    )
    assert stale.status_code == 409

    # Replaying the corrected values stays idempotent.
    replay = post_records(
        client, [sample(1, "2026-09-23T08:00:00Z", 20)], replay=True
    ).json()
    assert replay["inserted"] == 0
    assert replay["duplicate_identical"] == 1


def test_correction_leaves_other_devices_untouched(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12, "sensor-a"),
            sample(1, "2026-09-23T08:00:00Z", 11, "sensor-b"),
        ],
    )
    before_b = client.get("/devices/sensor-b/events").json()

    response = client.post(
        "/records/correction",
        json=correction(
            device="sensor-a",
            sequence=1,
            old_at="2026-09-23T08:00:00Z",
            old_turbidity=12,
            new_at="2026-09-23T08:00:00Z",
            new_turbidity=3,
        ),
    )
    assert response.status_code == 200
    assert response.json()["events"] == []

    assert client.get("/devices/sensor-a/events").json()["total"] == 0
    assert client.get("/devices/sensor-b/events").json() == before_b


def test_correction_with_unchanged_values_is_allowed(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])

    response = client.post(
        "/records/correction",
        json=correction(
            sequence=1,
            old_at="2026-09-23T08:00:00Z",
            old_turbidity=12,
            new_at="2026-09-23T08:00:00Z",
            new_turbidity=12,
        ),
    )
    assert response.status_code == 200
    assert len(response.json()["events"]) == 1


def test_failed_event_rebuild_rolls_back_correction(client, monkeypatch):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12)])

    def crash(self, device_id):
        raise RuntimeError("simulated failure during event rebuild")

    monkeypatch.setattr(WaterQualityService, "_rebuild_device", crash)
    with pytest.raises(RuntimeError):
        client.post(
            "/records/correction",
            json=correction(
                sequence=1,
                old_at="2026-09-23T08:00:00Z",
                old_turbidity=12,
                new_at="2026-09-23T08:00:00Z",
                new_turbidity=20,
            ),
        )
    monkeypatch.undo()

    # The UPDATE rolled back together with the failed rebuild: the original
    # record is still stored and remains the dedup reference.
    replay = post_records(
        client, [sample(1, "2026-09-23T08:00:00Z", 12)], replay=True
    ).json()
    assert replay["inserted"] == 0
    assert replay["duplicate_identical"] == 1
    events = client.get("/devices/sensor-a/events").json()["events"]
    assert len(events) == 1
    assert events[0]["peak_turbidity"] == 12.0
