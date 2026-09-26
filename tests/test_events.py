from __future__ import annotations

from tests.conftest import post_records


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


def test_lifecycle_deduplicates_and_allows_idempotent_replay(client):
    first_response = post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 4),
            sample(2, "2026-09-23T08:01:00Z", 10),
            sample(3, "2026-09-23T08:02:00Z", 12),
            sample(4, "2026-09-23T08:03:00Z", 7),
            sample(5, "2026-09-23T08:04:00Z", 5),
        ],
    ).json()

    assert first_response["inserted"] == 5
    assert first_response["duplicate_identical"] == 0
    event = first_response["events"][0]["events"][0]
    assert event["status"] == "recovered"
    assert event["start_sequence"] == 2
    assert event["end_sequence"] == 5
    assert event["peak_sequence"] == 3
    assert event["peak_turbidity"] == 12.0
    assert event["sample_count"] == 4

    replay_response = post_records(
        client,
        [
            sample(2, "2026-09-23T08:01:00Z", 10),
            sample(3, "2026-09-23T08:02:00Z", 12),
        ],
        replay=True,
    ).json()
    assert replay_response["inserted"] == 0
    assert replay_response["duplicate_identical"] == 2
    assert replay_response["events"][0]["events"][0]["event_id"] == event["event_id"]

    get_response = client.get("/devices/sensor-a/events")
    assert get_response.status_code == 200
    assert get_response.json()["total"] == 1
    assert get_response.json()["events"][0]["event_id"] == event["event_id"]


def test_late_records_recompute_merge_and_split_hysteresis_intervals(client):
    # Initially only the two high observations are known. Recomputation must
    # not invent a recovered boundary merely because the observations arrived
    # apart; both are one continuous alarm.
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 11),
            sample(3, "2026-09-23T08:02:00Z", 14),
        ],
    ).json()
    assert len(response["events"][0]["events"]) == 1
    merged = response["events"][0]["events"][0]
    assert merged["start_sequence"] == 1
    assert merged["sample_count"] == 2
    assert merged["peak_sequence"] == 3

    # A late in-hysteresis/high sample fills the gap and remains in the same
    # event instead of producing a second event.
    response = post_records(
        client,
        [sample(2, "2026-09-23T08:01:00Z", 8)],
    ).json()
    assert len(response["events"][0]["events"]) == 1
    assert response["events"][0]["events"][0]["sample_count"] == 3

    # A late recovery observation exposes the true boundary and splits the
    # derived interval. A subsequent high value then starts a second event.
    response = post_records(
        client,
        [
            sample(4, "2026-09-23T08:03:00Z", 5),
            sample(5, "2026-09-23T08:04:00Z", 13),
        ],
    ).json()
    events = response["events"][0]["events"]
    assert [event["start_sequence"] for event in events] == [1, 5]
    assert events[0]["end_sequence"] == 4
    assert events[1]["status"] == "active"
    assert events[1]["sample_count"] == 1


def test_late_sample_before_existing_event_can_change_start(client):
    post_records(
        client,
        [
            sample(10, "2026-09-23T08:10:00Z", 6),
            sample(11, "2026-09-23T08:11:00Z", 12),
        ],
    )

    response = post_records(
        client,
        [sample(9, "2026-09-23T08:09:00Z", 15)],
    ).json()
    event = response["events"][0]["events"][0]
    assert event["start_sequence"] == 9
    assert event["peak_sequence"] == 9
    assert event["sample_count"] == 3
    assert event["status"] == "active"


def test_recovery_observation_is_deterministic_event_separator(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12),
            sample(2, "2026-09-23T08:01:00Z", 4),
            sample(3, "2026-09-23T08:02:00Z", 11),
        ],
    )

    # A later high observation cannot bridge backwards over an observed value
    # at or below the recovery threshold.
    response = post_records(
        client,
        [sample(4, "2026-09-23T08:03:00Z", 20)],
    ).json()
    events = response["events"][0]["events"]
    assert [event["start_sequence"] for event in events] == [1, 3]
    assert events[0]["end_sequence"] == 2
    assert events[1]["peak_sequence"] == 4


def test_out_of_order_sequences_do_not_constrain_event_bounds(client):
    # Sequence numbers are dedup keys, not a clock: a late peak may carry a
    # sequence below the event start, and the recovery sample may too. The
    # event bounds follow sample time only.
    post_records(client, [sample(5, "2026-09-23T08:00:00Z", 12)])

    response = post_records(
        client,
        [
            sample(2, "2026-09-23T08:00:30Z", 15),
            sample(3, "2026-09-23T08:01:00Z", 4),
        ],
    ).json()
    events = response["events"][0]["events"]
    assert len(events) == 1
    event = events[0]
    assert event["status"] == "recovered"
    assert event["start_sequence"] == 5
    assert event["peak_sequence"] == 2
    assert event["peak_turbidity"] == 15.0
    assert event["end_sequence"] == 3
    assert event["start_at"] == "2026-09-23T08:00:00Z"
    assert event["end_at"] == "2026-09-23T08:01:00Z"
    assert event["sample_count"] == 3


def test_deduplication_is_scoped_by_device(client):
    response = post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 11, "sensor-a"),
            sample(1, "2026-09-23T08:00:00Z", 2, "sensor-b"),
        ],
    ).json()

    assert response["affected_devices"] == ["sensor-a", "sensor-b"]
    events = {item["device_id"]: item["events"] for item in response["events"]}
    assert len(events["sensor-a"]) == 1
    assert events["sensor-b"] == []


def test_conflicting_observation_for_same_key_returns_409(client):
    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 11)])

    response = client.post(
        "/records",
        json={
            "records": [
                sample(1, "2026-09-23T08:00:00Z", 20),
            ]
        },
    )
    assert response.status_code == 409


def test_reject_timestamp_without_timezone(client):
    response = client.post(
        "/records",
        json={
            "records": [
                {
                    "device_id": "sensor-a",
                    "sequence": 1,
                    "sampled_at": "2026-09-23T08:00:00",
                    "turbidity": 12,
                }
            ]
        },
    )
    assert response.status_code == 422
