from __future__ import annotations

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.config import Settings
from tests.conftest import post_records, strip_acknowledged


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


def test_late_higher_sequence_peak_inside_interrupted_event(client):
    # Regression: sequence numbers only identify samples for dedup and event
    # identity; they say nothing about observation order. The late 00:05
    # sample carries the highest sequence AND the peak, while the recomputed
    # event still tentatively ends (by sample time) at 00:08, whose sequence
    # is lower than the peak's. Persisting that event must not be rejected.
    first = post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(3, "2026-09-23T00:08:00Z", 13),
            sample(7, "2026-09-23T00:30:00Z", 14),
        ],
    ).json()
    assert [event["status"] for event in first["events"][0]["events"]] == [
        "interrupted",
        "active",
    ]

    response = post_records(
        client,
        [sample(9, "2026-09-23T00:05:00Z", 20)],
    )
    assert response.status_code == 201
    body = response.json()
    assert body["inserted"] == 1

    events = body["events"][0]["events"]
    assert [event["status"] for event in events] == ["interrupted", "active"]
    interrupted = events[0]
    # Boundary semantics are unchanged: the event tentatively ends at the
    # last observed sample before the gap, and the segment after the gap
    # stays a separate active event.
    assert interrupted["event_id"] == "evt:sensor-a:1"
    assert interrupted["start_sequence"] == 1
    assert interrupted["end_sequence"] == 3
    assert interrupted["start_at"] == "2026-09-23T00:00:00Z"
    assert interrupted["end_at"] == "2026-09-23T00:08:00Z"
    assert interrupted["peak_sequence"] == 9
    assert interrupted["peak_turbidity"] == 20.0
    assert interrupted["sample_count"] == 3
    assert events[1]["event_id"] == "evt:sensor-a:7"
    assert events[1]["start_sequence"] == 7
    assert events[1]["sample_count"] == 1

    # Persistence and the query path agree with the write response.
    get_response = client.get("/devices/sensor-a/events").json()
    assert get_response["total"] == 2
    assert strip_acknowledged(get_response["events"]) == events

    # Replaying the late batch stays idempotent.
    replay = post_records(
        client, [sample(9, "2026-09-23T00:05:00Z", 20)], replay=True
    ).json()
    assert replay["inserted"] == 0
    assert replay["duplicate_identical"] == 1
    assert replay["events"] == body["events"]


def test_wide_gap_without_recovery_interrupts_at_previous_sample(client):
    # The scenario from the bug report: high samples at 00:00 and 00:30 with
    # nothing observed between them. Continuity cannot be claimed, so the
    # alarm tentatively ends at the last sample before the gap instead of
    # being reported as one event spanning the unobserved interval.
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:30:00Z", 13),
        ],
    ).json()

    events = response["events"][0]["events"]
    assert [event["status"] for event in events] == [
        "interrupted",
        "active",
    ]
    first, second = events
    assert first["start_sequence"] == 1
    assert first["end_sequence"] == 1
    assert first["start_at"] == "2026-09-23T00:00:00Z"
    assert first["end_at"] == "2026-09-23T00:00:00Z"
    assert first["sample_count"] == 1
    assert second["start_sequence"] == 2
    assert second["end_at"] is None
    assert second["sample_count"] == 1

    # Persistence and the query path agree with the write response.
    get_response = client.get("/devices/sensor-a/events").json()
    assert get_response["total"] == 2
    assert strip_acknowledged(get_response["events"]) == events


def test_late_samples_that_fill_the_gap_merge_interrupted_segments(client):
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(4, "2026-09-23T00:30:00Z", 15),
        ],
    ).json()
    events = response["events"][0]["events"]
    assert [event["status"] for event in events] == [
        "interrupted",
        "active",
    ]

    # A single connecting sample is not enough: gaps of 10 min and 20 min
    # remain, so the first segment is still interrupted.
    response = post_records(
        client,
        [sample(2, "2026-09-23T00:10:00Z", 11)],
    ).json()
    events = response["events"][0]["events"]
    assert [event["status"] for event in events] == [
        "interrupted",
        "active",
    ]
    assert events[0]["end_sequence"] == 2
    assert events[0]["sample_count"] == 2

    # Filling every gap up to the configured limit reconnects the segments;
    # peak, count, start and end are recomputed for the merged event.
    response = post_records(
        client,
        [sample(3, "2026-09-23T00:20:00Z", 8)],
    ).json()
    events = response["events"][0]["events"]
    assert len(events) == 1
    merged = events[0]
    assert merged["status"] == "active"
    assert merged["start_sequence"] == 1
    assert merged["end_sequence"] is None
    assert merged["sample_count"] == 4
    assert merged["peak_sequence"] == 4
    assert merged["peak_turbidity"] == 15.0
    assert merged["event_id"] == "evt:sensor-a:1"


def test_gap_equal_to_limit_keeps_event_continuous(client):
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:10:00Z", 13),
        ],
    ).json()
    events = response["events"][0]["events"]
    assert len(events) == 1
    assert events[0]["status"] == "active"
    assert events[0]["sample_count"] == 2


def test_gap_one_second_over_limit_interrupts(client):
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:10:01Z", 13),
        ],
    ).json()
    events = response["events"][0]["events"]
    assert [event["status"] for event in events] == [
        "interrupted",
        "active",
    ]
    assert events[0]["end_at"] == "2026-09-23T00:00:00Z"


def test_recovery_sample_within_gap_is_still_uncrossable_boundary(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:05:00Z", 4),
            sample(3, "2026-09-23T00:10:00Z", 20),
        ],
    )

    # A later high observation cannot bridge backwards over an observed value
    # at or below the recovery threshold.
    response = post_records(
        client,
        [sample(4, "2026-09-23T00:15:00Z", 21)],
    ).json()
    events = response["events"][0]["events"]
    assert [event["start_sequence"] for event in events] == [1, 3]
    assert events[0]["status"] == "recovered"
    assert events[0]["end_sequence"] == 2
    assert events[0]["end_at"] == "2026-09-23T00:05:00Z"
    assert events[0]["sample_count"] == 2
    assert events[1]["status"] == "active"
    assert events[1]["peak_sequence"] == 4


def test_recovery_sample_after_wide_gap_does_not_extend_event(client):
    # The gap interrupts the alarm at the last high sample; the late low
    # sample cannot supply a recovered end to an event that already lost
    # continuity, so no recovery sample is invented across the gap.
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:30:00Z", 3),
        ],
    ).json()
    events = response["events"][0]["events"]
    assert len(events) == 1
    assert events[0]["status"] == "interrupted"
    assert events[0]["end_sequence"] == 1
    assert events[0]["end_at"] == "2026-09-23T00:00:00Z"
    assert events[0]["sample_count"] == 1


def test_hysteresis_value_after_gap_starts_no_new_event(client):
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:30:00Z", 8),
        ],
    ).json()
    events = response["events"][0]["events"]
    assert len(events) == 1
    assert events[0]["status"] == "interrupted"
    assert events[0]["sample_count"] == 1


def test_status_filter_distinguishes_three_states(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12),
            sample(2, "2026-09-23T00:05:00Z", 4),   # recovered event
            sample(3, "2026-09-23T00:30:00Z", 12),  # new event starts
            sample(4, "2026-09-23T01:00:00Z", 13),  # gap: interrupted + active
        ],
    )

    body = client.get("/devices/sensor-a/events").json()
    assert body["total"] == 3
    assert [event["status"] for event in body["events"]] == [
        "recovered",
        "interrupted",
        "active",
    ]

    for expected_status, expected_end in (
        ("recovered", 2),
        ("interrupted", 3),
        ("active", None),
    ):
        body = client.get(
            f"/devices/sensor-a/events?status={expected_status}"
        ).json()
        assert body["total"] == 1
        event = body["events"][0]
        assert event["status"] == expected_status
        assert event["end_sequence"] == expected_end

    bad = client.get("/devices/sensor-a/events?status=closed")
    assert bad.status_code == 422


def test_interrupted_segments_remain_idempotent_on_replay(client):
    payload = [
        sample(1, "2026-09-23T00:00:00Z", 12),
        sample(2, "2026-09-23T00:30:00Z", 13),
    ]
    first = post_records(client, payload).json()
    replay = post_records(client, payload, replay=True).json()

    assert replay["inserted"] == 0
    assert replay["duplicate_identical"] == 2
    assert replay["affected_devices"] == ["sensor-a"]
    assert replay["events"] == first["events"]

    get_response = client.get("/devices/sensor-a/events").json()
    assert strip_acknowledged(get_response["events"]) == first["events"][0][
        "events"
    ]


def test_thresholds_response_exposes_max_sample_gap(client):
    body = client.get("/thresholds").json()
    assert body == {
        "alarm_threshold": 10.0,
        "recovery_threshold": 5.0,
        "max_sample_gap_seconds": 600,
    }


def _client_with_settings(db_path, monkeypatch, gap_seconds: int) -> TestClient:
    monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
    monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
    monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", str(gap_seconds))

    from app.main import app

    return TestClient(app)


def test_gap_config_change_recomputes_events_from_raw_samples(
    tmp_path, monkeypatch
):
    db_path = tmp_path / "water.db"
    payload = [
        sample(1, "2026-09-23T00:00:00Z", 12),
        sample(2, "2026-09-23T00:30:00Z", 15),
    ]

    with _client_with_settings(db_path, monkeypatch, 600) as first_client:
        events = post_records(first_client, payload).json()["events"][0][
            "events"
        ]
        assert [event["status"] for event in events] == [
            "interrupted",
            "active",
        ]

    # Widening the limit on restart merges the segments from raw records.
    with _client_with_settings(db_path, monkeypatch, 3600) as wider_client:
        body = wider_client.get("/devices/sensor-a/events").json()
        assert body["total"] == 1
        merged = body["events"][0]
        assert merged["status"] == "active"
        assert merged["start_sequence"] == 1
        assert merged["sample_count"] == 2
        assert merged["peak_turbidity"] == 15.0

    # Narrowing the limit again splits the event back into segments.
    with _client_with_settings(db_path, monkeypatch, 300) as narrower_client:
        body = narrower_client.get("/devices/sensor-a/events").json()
        assert body["total"] == 2
        assert [event["status"] for event in body["events"]] == [
            "interrupted",
            "active",
        ]


@pytest.mark.parametrize("bad_value", ["0", "-60", "1.5", "not-a-number"])
def test_max_sample_gap_must_be_positive_integer(monkeypatch, bad_value):
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", bad_value)
    with pytest.raises(ValidationError):
        Settings()
