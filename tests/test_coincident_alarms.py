from __future__ import annotations

from datetime import datetime, timedelta, timezone

from tests.conftest import ack_event, get_events, post_records

UTC = timezone.utc
BASE = datetime(2026, 9, 23, 8, 0, tzinfo=UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def sample(device: str, sequence: int, at: datetime, turbidity: float):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": iso(at),
        "turbidity": turbidity,
    }


def recovered_event(
    device: str, start: datetime, end: datetime, seq_start: int = 1
) -> list[dict]:
    """High samples every 5 minutes from start, recovery sample at end."""
    records = []
    sequence = seq_start
    instant = start
    while instant < end:
        records.append(sample(device, sequence, instant, 12.0))
        sequence += 1
        instant += timedelta(minutes=5)
    records.append(sample(device, sequence, end, 4.0))
    return records


def active_event(device: str, start: datetime, seq_start: int = 1) -> list[dict]:
    return [sample(device, seq_start, start, 12.0)]


def valid_payload() -> dict:
    return {
        "device_ids": ["sensor-a", "sensor-b"],
        "quorum": 2,
        "from": "2026-09-23T08:00:00Z",
        "to": "2026-09-23T09:00:00Z",
    }


def query(
    client,
    device_ids: list[str],
    quorum: int,
    window_from: str,
    window_to: str,
) -> dict:
    response = client.post(
        "/coincident-alarms",
        json={
            "device_ids": device_ids,
            "quorum": quorum,
            "from": window_from,
            "to": window_to,
        },
    )
    assert response.status_code == 200
    return response.json()


def test_segments_split_where_participating_device_set_changes(client):
    post_records(
        client, recovered_event("sensor-a", BASE, BASE + timedelta(minutes=30))
    )
    post_records(
        client,
        recovered_event(
            "sensor-b", BASE + timedelta(minutes=10), BASE + timedelta(minutes=20)
        ),
    )
    post_records(
        client,
        recovered_event(
            "sensor-c", BASE + timedelta(minutes=15), BASE + timedelta(minutes=18)
        ),
    )

    body = query(
        client,
        ["sensor-a", "sensor-b", "sensor-c"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )

    # The first and third segments share the same device set but are not
    # adjacent, so they must not merge.
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:10:00Z",
            "end": "2026-09-23T08:15:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        },
        {
            "start": "2026-09-23T08:15:00Z",
            "end": "2026-09-23T08:18:00Z",
            "device_ids": ["sensor-a", "sensor-b", "sensor-c"],
        },
        {
            "start": "2026-09-23T08:18:00Z",
            "end": "2026-09-23T08:20:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        },
    ]

    body = query(
        client,
        ["sensor-a", "sensor-b", "sensor-c"],
        3,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:15:00Z",
            "end": "2026-09-23T08:18:00Z",
            "device_ids": ["sensor-a", "sensor-b", "sensor-c"],
        }
    ]


def test_shared_endpoint_is_not_counted_as_overlap(client):
    # sensor-a's event ends at exactly the instant sensor-b's begins; the
    # half-open intervals never overlap, so quorum 2 is never reached.
    post_records(
        client, recovered_event("sensor-a", BASE, BASE + timedelta(minutes=10))
    )
    post_records(
        client,
        recovered_event(
            "sensor-b", BASE + timedelta(minutes=10), BASE + timedelta(minutes=20)
        ),
    )

    body = query(
        client,
        ["sensor-a", "sensor-b"],
        2,
        iso(BASE),
        iso(BASE + timedelta(minutes=20)),
    )
    assert body["segments"] == []


def test_endpoints_at_the_same_instant_are_applied_together(client):
    # At 08:10 sensor-a leaves and sensor-b joins in the same instant; the
    # participating set switches atomically and the segments stay adjacent.
    post_records(
        client, recovered_event("sensor-a", BASE, BASE + timedelta(minutes=10))
    )
    post_records(
        client,
        recovered_event(
            "sensor-b", BASE + timedelta(minutes=10), BASE + timedelta(minutes=20)
        ),
    )
    post_records(client, active_event("sensor-c", BASE))

    body = query(
        client,
        ["sensor-a", "sensor-b", "sensor-c"],
        2,
        iso(BASE),
        iso(BASE + timedelta(minutes=20)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:00:00Z",
            "end": "2026-09-23T08:10:00Z",
            "device_ids": ["sensor-a", "sensor-c"],
        },
        {
            "start": "2026-09-23T08:10:00Z",
            "end": "2026-09-23T08:20:00Z",
            "device_ids": ["sensor-b", "sensor-c"],
        },
    ]


def test_active_events_extend_to_window_end(client):
    post_records(client, active_event("sensor-a", BASE))
    post_records(client, active_event("sensor-b", BASE + timedelta(minutes=5)))

    body = query(
        client,
        ["sensor-a", "sensor-b"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:05:00Z",
            "end": "2026-09-23T09:00:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]


def test_intervals_are_clipped_and_zero_length_dropped(client):
    # sensor-a alarms across the whole window and beyond both sides.
    post_records(
        client,
        recovered_event(
            "sensor-a", BASE - timedelta(minutes=10), BASE + timedelta(minutes=20)
        ),
    )
    # sensor-b starts inside the window and ends beyond it.
    post_records(
        client,
        recovered_event(
            "sensor-b", BASE + timedelta(minutes=2), BASE + timedelta(minutes=20)
        ),
    )
    # sensor-c's event ends exactly at `from`: clipped to zero length.
    post_records(
        client, recovered_event("sensor-c", BASE - timedelta(minutes=10), BASE)
    )
    # sensor-d's event starts exactly at `to`: clipped to zero length.
    post_records(
        client,
        recovered_event(
            "sensor-d", BASE + timedelta(minutes=10), BASE + timedelta(minutes=15)
        ),
    )

    body = query(
        client,
        ["sensor-a", "sensor-b", "sensor-c", "sensor-d"],
        2,
        iso(BASE),
        iso(BASE + timedelta(minutes=10)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:02:00Z",
            "end": "2026-09-23T08:10:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]


def test_unknown_devices_are_treated_as_having_no_events(client):
    post_records(client, active_event("sensor-a", BASE))
    post_records(client, active_event("sensor-b", BASE + timedelta(minutes=5)))

    body = query(
        client,
        ["sensor-a", "sensor-b", "sensor-unknown"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:05:00Z",
            "end": "2026-09-23T09:00:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]

    # The unknown device contributes nothing, so quorum 3 is unreachable.
    body = query(
        client,
        ["sensor-a", "sensor-b", "sensor-unknown"],
        3,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == []


def test_single_device_is_counted_once_per_instant(client):
    records = recovered_event("sensor-a", BASE, BASE + timedelta(minutes=10))
    records += recovered_event(
        "sensor-a",
        BASE + timedelta(minutes=20),
        BASE + timedelta(minutes=30),
        seq_start=100,
    )
    post_records(client, records)

    # Two events of the same device never add up towards the quorum.
    body = query(
        client,
        ["sensor-a", "sensor-unknown"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == []

    post_records(client, active_event("sensor-b", BASE))
    body = query(
        client,
        ["sensor-a", "sensor-b"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:00:00Z",
            "end": "2026-09-23T08:10:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        },
        {
            "start": "2026-09-23T08:20:00Z",
            "end": "2026-09-23T08:30:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        },
    ]


def test_interrupted_event_ends_at_last_observed_sample(client):
    # The gap before the third sample exceeds the limit, so the first event
    # is interrupted at its last observed sample and the third sample opens
    # a new active event.
    post_records(
        client,
        [
            sample("sensor-a", 1, BASE, 12.0),
            sample("sensor-a", 2, BASE + timedelta(minutes=5), 11.0),
            sample("sensor-a", 3, BASE + timedelta(minutes=30), 12.0),
        ],
    )
    post_records(client, active_event("sensor-b", BASE))

    body = query(
        client,
        ["sensor-a", "sensor-b"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:00:00Z",
            "end": "2026-09-23T08:05:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        },
        {
            "start": "2026-09-23T08:30:00Z",
            "end": "2026-09-23T09:00:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        },
    ]


def test_single_sample_interrupted_event_is_zero_length_and_dropped(client):
    # The first event is interrupted at its only sample, i.e. the zero-length
    # interval [08:00, 08:00), which must not count towards the quorum.
    post_records(
        client,
        [
            sample("sensor-a", 1, BASE, 12.0),
            sample("sensor-a", 2, BASE + timedelta(minutes=20), 12.0),
        ],
    )
    post_records(client, active_event("sensor-b", BASE))

    body = query(
        client,
        ["sensor-a", "sensor-b"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:20:00Z",
            "end": "2026-09-23T09:00:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]


def test_late_writes_are_reflected_in_the_next_query(client):
    post_records(client, active_event("sensor-a", BASE))
    post_records(
        client,
        [
            sample("sensor-b", 1, BASE + timedelta(minutes=30), 12.0),
            sample("sensor-b", 2, BASE + timedelta(minutes=35), 12.0),
        ],
    )

    window = (iso(BASE), iso(BASE + timedelta(hours=1)))
    body = query(client, ["sensor-a", "sensor-b"], 2, *window)
    assert [segment["start"] for segment in body["segments"]] == [
        "2026-09-23T08:30:00Z"
    ]

    # A late sample exactly one max-gap before the previous start extends
    # the alarm backwards; the next query reflects it immediately.
    post_records(
        client, [sample("sensor-b", 0, BASE + timedelta(minutes=20), 12.0)]
    )
    body = query(client, ["sensor-a", "sensor-b"], 2, *window)
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:20:00Z",
            "end": "2026-09-23T09:00:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]


def test_corrections_are_reflected_in_the_next_query(client):
    post_records(
        client, recovered_event("sensor-a", BASE, BASE + timedelta(minutes=10))
    )
    post_records(client, active_event("sensor-b", BASE + timedelta(minutes=5)))

    window = (iso(BASE), iso(BASE + timedelta(hours=1)))
    body = query(client, ["sensor-a", "sensor-b"], 2, *window)
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:05:00Z",
            "end": "2026-09-23T08:10:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]

    response = client.post(
        "/records/correction",
        json={
            "device_id": "sensor-a",
            "sequence": 3,
            "old_sampled_at": "2026-09-23T08:10:00Z",
            "old_turbidity": 4.0,
            "new_sampled_at": "2026-09-23T08:15:00Z",
            "new_turbidity": 4.0,
        },
    )
    assert response.status_code == 200
    body = query(client, ["sensor-a", "sensor-b"], 2, *window)
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:05:00Z",
            "end": "2026-09-23T08:15:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]


def test_query_does_not_modify_events_acks_or_change_stream(client):
    post_records(client, active_event("sensor-a", BASE))
    post_records(client, active_event("sensor-b", BASE + timedelta(minutes=5)))

    events_a = get_events(client, "sensor-a")
    assert ack_event(client, events_a["events"][0]).status_code == 200
    events_a_before = get_events(client, "sensor-a")
    events_b_before = get_events(client, "sensor-b")
    changes_before = client.get(
        "/event-changes", params={"after": 0, "limit": 1000}
    ).json()

    body = query(
        client,
        ["sensor-a", "sensor-b"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert len(body["segments"]) == 1

    assert get_events(client, "sensor-a") == events_a_before
    assert get_events(client, "sensor-b") == events_b_before
    assert (
        client.get("/event-changes", params={"after": 0, "limit": 1000}).json()
        == changes_before
    )


def test_acknowledged_events_still_count(client):
    post_records(client, active_event("sensor-a", BASE))
    post_records(client, active_event("sensor-b", BASE + timedelta(minutes=5)))
    events = get_events(client, "sensor-a")
    assert ack_event(client, events["events"][0]).status_code == 200

    body = query(
        client,
        ["sensor-a", "sensor-b"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:05:00Z",
            "end": "2026-09-23T09:00:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]


def test_window_echo_is_utc_and_segment_device_ids_sorted(client):
    post_records(client, active_event("sensor-b", BASE))
    post_records(client, active_event("sensor-a", BASE))

    body = query(
        client,
        ["sensor-b", "sensor-a"],
        2,
        "2026-09-23T16:00:00+08:00",
        "2026-09-23T17:00:00+08:00",
    )
    assert body["from"] == "2026-09-23T08:00:00Z"
    assert body["to"] == "2026-09-23T09:00:00Z"
    assert body["quorum"] == 2
    assert body["device_ids"] == ["sensor-b", "sensor-a"]
    assert body["segments"] == [
        {
            "start": "2026-09-23T08:00:00Z",
            "end": "2026-09-23T09:00:00Z",
            "device_ids": ["sensor-a", "sensor-b"],
        }
    ]


def test_no_events_yields_empty_segments(client):
    body = query(
        client,
        ["sensor-a", "sensor-b"],
        2,
        iso(BASE),
        iso(BASE + timedelta(hours=1)),
    )
    assert body["segments"] == []


def test_boundary_sizes_are_accepted(client):
    device_ids = [f"sensor-{index:02d}" for index in range(50)]
    body = query(
        client,
        device_ids,
        50,
        "2026-09-23T08:00:00Z",
        "2026-09-23T09:00:00Z",
    )
    assert body["device_ids"] == device_ids
    assert body["segments"] == []


def test_invalid_device_lists_return_422(client):
    base = valid_payload()
    bad_device_lists = [
        ["sensor-a"],  # fewer than two devices
        [f"sensor-{index:02d}" for index in range(51)],  # more than fifty
        ["sensor-a", "sensor-a"],  # exact duplicates
        ["sensor-a", " sensor-a "],  # duplicates after whitespace stripping
        ["sensor-a", "  "],  # empty after whitespace stripping
    ]
    for device_ids in bad_device_lists:
        response = client.post(
            "/coincident-alarms", json={**base, "device_ids": device_ids}
        )
        assert response.status_code == 422, device_ids


def test_invalid_quorum_returns_422(client):
    base = valid_payload()
    for quorum in (1, 3, 2.5, "two"):
        response = client.post(
            "/coincident-alarms", json={**base, "quorum": quorum}
        )
        assert response.status_code == 422, quorum


def test_invalid_window_returns_422(client):
    base = valid_payload()
    bad_windows = [
        ("2026-09-23T09:00:00Z", "2026-09-23T08:00:00Z"),  # from after to
        ("2026-09-23T08:00:00Z", "2026-09-23T08:00:00Z"),  # equal instants
        ("2026-09-23T10:00:00+02:00", "2026-09-23T08:00:00Z"),  # equal in UTC
        ("2026-09-23T08:00:00", "2026-09-23T09:00:00Z"),  # naive from
        ("2026-09-23T08:00:00Z", "2026-09-23T09:00:00"),  # naive to
    ]
    for window_from, window_to in bad_windows:
        response = client.post(
            "/coincident-alarms",
            json={**base, "from": window_from, "to": window_to},
        )
        assert response.status_code == 422, (window_from, window_to)


def test_missing_and_extra_fields_return_422(client):
    base = valid_payload()
    for key in ("device_ids", "quorum", "from", "to"):
        payload = {name: value for name, value in base.items() if name != key}
        response = client.post("/coincident-alarms", json=payload)
        assert response.status_code == 422, key
    response = client.post("/coincident-alarms", json={**base, "extra": 1})
    assert response.status_code == 422
