from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import get_events, post_records


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


def get_evidence(client: TestClient, event: dict, *, device: str | None = None):
    return client.get(
        f"/devices/{device or event['device_id']}/events/"
        f"{event['event_id']}/evidence"
    )


def _facts(samples: list[dict]) -> list[tuple[int, str, float]]:
    return [
        (item["sequence"], item["sampled_at"], item["turbidity"])
        for item in samples
    ]


def test_recovered_evidence_includes_recovery_sample_and_roles(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 4.0),
            sample(2, "2026-09-23T08:01:00Z", 10.0),
            sample(3, "2026-09-23T08:02:00Z", 12.0),
            sample(4, "2026-09-23T08:03:00Z", 8.0),
            sample(5, "2026-09-23T08:04:00Z", 5.0),
        ],
    )
    event = get_events(client)["events"][0]
    assert event["status"] == "recovered"

    response = get_evidence(client, event)
    assert response.status_code == 200
    body = response.json()

    # The embedded event is the plain event without an acknowledged flag.
    assert set(body["event"]) == {
        "event_id",
        "device_id",
        "status",
        "start_sequence",
        "end_sequence",
        "start_at",
        "end_at",
        "peak_turbidity",
        "peak_sequence",
        "sample_count",
    }
    # The embedded event equals the listed event without the
    # query-only acknowledged flag.
    assert body["event"] == {
        key: value for key, value in event.items() if key != "acknowledged"
    }
    assert body["alarm_threshold"] == 10.0
    assert body["recovery_threshold"] == 5.0
    assert body["max_sample_gap_seconds"] == 600

    samples = body["samples"]
    assert len(samples) == body["event"]["sample_count"] == 4
    assert _facts(samples) == [
        (2, "2026-09-23T08:01:00Z", 10.0),
        (3, "2026-09-23T08:02:00Z", 12.0),
        (4, "2026-09-23T08:03:00Z", 8.0),
        (5, "2026-09-23T08:04:00Z", 5.0),
    ]
    assert [item["role"] for item in samples] == [
        "trigger",
        "continuing",
        "continuing",
        "recovery",
    ]
    # Exactly one peak, on the first hit of the maximum value.
    assert [item["peak"] for item in samples] == [
        False,
        True,
        False,
        False,
    ]
    # A recovered event carries no interruption evidence.
    assert body["interruption"] is None


def test_tied_peak_marks_first_hit_in_sample_order(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12.0),
            sample(2, "2026-09-23T08:01:00Z", 12.0),
            sample(3, "2026-09-23T08:02:00Z", 7.0),
        ],
    )
    event = get_events(client)["events"][0]
    assert event["peak_sequence"] == 1
    assert event["peak_turbidity"] == 12.0

    body = get_evidence(client, event).json()
    peaks = [item["sequence"] for item in body["samples"] if item["peak"]]
    assert peaks == [1]


def test_active_evidence_has_no_ending_evidence(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 11.0),
            sample(2, "2026-09-23T08:01:00Z", 8.0),
        ],
    )
    event = get_events(client)["events"][0]
    assert event["status"] == "active"

    body = get_evidence(client, event).json()
    assert len(body["samples"]) == event["sample_count"] == 2
    assert [item["role"] for item in body["samples"]] == [
        "trigger",
        "continuing",
    ]
    assert body["interruption"] is None


def test_interrupted_evidence_attaches_next_sample_and_gap(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12.0),
            sample(2, "2026-09-23T00:05:00Z", 11.0),
            sample(3, "2026-09-23T00:30:00Z", 13.0),
        ],
    )
    events = get_events(client)["events"]
    interrupted = next(
        event for event in events if event["status"] == "interrupted"
    )
    assert interrupted["sample_count"] == 2

    body = get_evidence(client, interrupted).json()
    members = body["samples"]
    assert len(members) == 2
    assert [item["role"] for item in members] == [
        "trigger",
        "continuing",
    ]
    # The last observed member ends the interrupted event; the following
    # sample is attached separately with the over-limit interval.
    interruption = body["interruption"]
    assert interruption is not None
    assert interruption["sample"] == {
        "sequence": 3,
        "sampled_at": "2026-09-23T00:30:00Z",
        "turbidity": 13.0,
    }
    assert interruption["gap_seconds"] == 25 * 60
    assert interruption["gap_seconds"] > body["max_sample_gap_seconds"]

    # The second (active) event starts exactly at the interrupting sample.
    active = next(event for event in events if event["status"] == "active")
    active_body = get_evidence(client, active).json()
    assert active_body["samples"][0]["sequence"] == 3
    assert active_body["interruption"] is None


def test_single_sample_interrupted_evidence(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12.0),
            sample(2, "2026-09-23T00:30:00Z", 4.0),
        ],
    )
    event = get_events(client, status="interrupted")["events"][0]
    body = get_evidence(client, event).json()
    assert len(body["samples"]) == 1
    assert body["samples"][0]["role"] == "trigger"
    assert body["interruption"]["sample"]["sequence"] == 2
    assert body["interruption"]["gap_seconds"] == 1800


def test_same_timestamp_boundary_located_by_sequence(client):
    # The recovery sample and the next trigger share one timestamp. A
    # time-only slice could not tell which event they belong to; the
    # (sampled_at, sequence) ordering must place them on opposite sides.
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12.0),
            sample(2, "2026-09-23T08:05:00Z", 4.0),
            sample(3, "2026-09-23T08:05:00Z", 11.0),
        ],
    )
    events = {event["status"]: event for event in get_events(client)["events"]}

    recovered = get_evidence(client, events["recovered"]).json()
    assert _facts(recovered["samples"]) == [
        (1, "2026-09-23T08:00:00Z", 12.0),
        (2, "2026-09-23T08:05:00Z", 4.0),
    ]
    assert recovered["samples"][-1]["role"] == "recovery"

    active = get_evidence(client, events["active"]).json()
    assert _facts(active["samples"]) == [
        (3, "2026-09-23T08:05:00Z", 11.0),
    ]
    assert active["samples"][0]["role"] == "trigger"


def test_event_not_found_returns_404(client):
    # Unknown device and unknown event id.
    response = client.get(
        "/devices/ghost/events/evt:ghost:1/evidence"
    )
    assert response.status_code == 404

    post_records(client, [sample(1, "2026-09-23T08:00:00Z", 12.0)])
    # An event id of another device must not resolve here.
    response = client.get(
        "/devices/other/events/evt:sensor-a:1/evidence"
    )
    assert response.status_code == 404
    # A fabricated event id on a known device is also 404.
    response = client.get(
        "/devices/sensor-a/events/evt:sensor-a:999/evidence"
    )
    assert response.status_code == 404


def test_late_write_changes_evidence_without_manual_step(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12.0),
            sample(4, "2026-09-23T00:30:00Z", 15.0),
        ],
    )
    interrupted = get_events(client, status="interrupted")["events"][0]
    first = get_evidence(client, interrupted).json()
    assert [item["sequence"] for item in first["samples"]] == [1]

    # Late samples fill the gap; the interrupted event merges into one
    # active event and the next evidence query reflects the new members.
    post_records(
        client,
        [
            sample(2, "2026-09-23T00:10:00Z", 11.0),
            sample(3, "2026-09-23T00:20:00Z", 8.0),
        ],
    )
    merged = get_events(client)["events"][0]
    assert merged["status"] == "active"
    assert merged["event_id"] == interrupted["event_id"]

    body = get_evidence(client, merged).json()
    assert [item["sequence"] for item in body["samples"]] == [
        1,
        2,
        3,
        4,
    ]
    assert body["interruption"] is None


def test_evidence_query_is_read_only(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T00:00:00Z", 12.0),
            sample(2, "2026-09-23T00:30:00Z", 13.0),
        ],
    )
    interrupted = get_events(client, status="interrupted")["events"][0]

    head_before = client.get("/event-changes?after=0").json()["head_change_id"]
    for _ in range(2):
        response = get_evidence(client, interrupted)
        assert response.status_code == 200
    head_after = client.get("/event-changes?after=0").json()["head_change_id"]
    assert head_after == head_before

    # Neither a successful query nor a 404 mutates events or samples.
    before = get_events(client)["total"]
    client.get("/devices/sensor-a/events/evt:sensor-a:999/evidence")
    assert get_events(client)["total"] == before


def test_evidence_ignores_acknowledgement_state(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12.0),
            sample(2, "2026-09-23T08:01:00Z", 4.0),
        ],
    )
    event = get_events(client)["events"][0]
    expected = {
        key: value for key, value in event.items() if key != "acknowledged"
    }
    ack_response = client.post(
        f"/devices/sensor-a/events/{event['event_id']}/ack",
        json={"expected_event": expected},
    )
    assert ack_response.status_code == 200

    body = get_evidence(client, event).json()
    assert "acknowledged" not in body["event"]
    assert body["event"] == expected


def test_correction_reflects_in_next_evidence_query(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12.0),
            sample(2, "2026-09-23T08:01:00Z", 13.0),
            sample(3, "2026-09-23T08:02:00Z", 6.0),
        ],
    )
    event = get_events(client)["events"][0]
    assert event["peak_sequence"] == 2
    body = get_evidence(client, event).json()
    assert [item["peak"] for item in body["samples"]] == [
        False,
        True,
        False,
    ]

    # Correcting the first sample to become the new peak changes the
    # derived event; the next evidence query answers from the new state.
    response = client.post(
        "/records/correction",
        json={
            "device_id": "sensor-a",
            "sequence": 1,
            "old_sampled_at": "2026-09-23T08:00:00Z",
            "old_turbidity": 12.0,
            "new_sampled_at": "2026-09-23T08:00:00Z",
            "new_turbidity": 20.0,
        },
    )
    assert response.status_code == 200
    updated = get_events(client)["events"][0]
    assert updated["peak_sequence"] == 1

    body = get_evidence(client, updated).json()
    assert [item["peak"] for item in body["samples"]] == [
        True,
        False,
        False,
    ]


def test_correction_that_removes_event_makes_evidence_404(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12.0),
            sample(2, "2026-09-23T08:01:00Z", 13.0),
        ],
    )
    event = get_events(client)["events"][0]
    assert get_evidence(client, event).status_code == 200

    # Lowering the sole would-be trigger below the alarm threshold removes
    # the derived event entirely; its evidence can no longer resolve.
    response = client.post(
        "/records/correction",
        json={
            "device_id": "sensor-a",
            "sequence": 1,
            "old_sampled_at": "2026-09-23T08:00:00Z",
            "old_turbidity": 12.0,
            "new_sampled_at": "2026-09-23T08:00:00Z",
            "new_turbidity": 2.0,
        },
    )
    assert response.status_code == 200
    assert get_evidence(client, event).status_code == 404


def test_members_always_match_sample_count(client):
    post_records(
        client,
        [
            sample(10, "2026-09-23T08:00:00Z", 4.0),
            sample(20, "2026-09-23T08:01:00Z", 11.0),
            sample(30, "2026-09-23T08:02:00Z", 14.0),
            sample(40, "2026-09-23T08:03:00Z", 6.0),
            sample(50, "2026-09-23T08:04:00Z", 13.0),
        ],
    )
    for event in get_events(client)["events"]:
        body = get_evidence(client, event).json()
        assert len(body["samples"]) == body["event"]["sample_count"]
