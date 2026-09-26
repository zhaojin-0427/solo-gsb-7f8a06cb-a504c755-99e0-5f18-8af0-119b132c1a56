from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import ack_event, get_events, post_records


def sample(sequence: int, at: str, turbidity: float, device: str = "sensor-a"):
    return {
        "device_id": device,
        "sequence": sequence,
        "sampled_at": at,
        "turbidity": turbidity,
    }


def query_exposure(
    client: TestClient,
    device: str = "sensor-a",
    window_from: str = "2026-09-23T08:00:00Z",
    window_to: str = "2026-09-23T09:00:00Z",
    threshold: float = 10.0,
) -> dict:
    response = client.post(
        f"/devices/{device}/turbidity-exposure",
        json={"from": window_from, "to": window_to, "threshold": threshold},
    )
    assert response.status_code == 200
    return response.json()


def test_linear_interpolation_splits_at_threshold_crossing(client):
    # 0 -> 20 over 600s crosses the threshold 10 at t=300s; the part above
    # is a triangle of base 300s and height 10.
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 0.0),
            sample(2, "2026-09-23T08:10:00Z", 20.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:10:00Z", threshold=10.0
    )
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 300.0
    assert body["excess_integral"] == 1500.0


def test_segment_fully_above_threshold(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:10:00Z", 30.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:10:00Z", threshold=10.0
    )
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 600.0
    # Trapezoid: average excess (10 + 20) / 2 over 600s.
    assert body["excess_integral"] == 9000.0


def test_segment_fully_below_threshold_still_covers(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 0.0),
            sample(2, "2026-09-23T08:10:00Z", 8.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:10:00Z", threshold=10.0
    )
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 0.0
    assert body["excess_integral"] == 0.0


def test_segments_are_clipped_to_the_window(client):
    # 0 -> 20 over 08:00-08:10; the window keeps only 08:02-08:08, where
    # the interpolated values are 4 and 16 and the crossing is at 08:05.
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 0.0),
            sample(2, "2026-09-23T08:10:00Z", 20.0),
        ],
    )
    body = query_exposure(
        client,
        window_from="2026-09-23T08:02:00Z",
        window_to="2026-09-23T08:08:00Z",
        threshold=10.0,
    )
    assert body["covered_seconds"] == 360.0
    assert body["exceedance_seconds"] == 180.0
    assert body["excess_integral"] == 540.0


def test_no_extrapolation_beyond_first_and_last_sample(client):
    # Samples only span 08:10-08:20; the wider window adds nothing.
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:10:00Z", 20.0),
            sample(2, "2026-09-23T08:20:00Z", 20.0),
        ],
    )
    body = query_exposure(client, threshold=10.0)
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 600.0
    assert body["excess_integral"] == 6000.0


def test_gap_over_limit_is_not_interpolated(client):
    # The 08:05 -> 08:20 gap (900s) exceeds the 600s limit, so only the
    # two 300s flanks are covered.
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:05:00Z", 20.0),
            sample(3, "2026-09-23T08:20:00Z", 20.0),
            sample(4, "2026-09-23T08:25:00Z", 20.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:30:00Z", threshold=10.0
    )
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 600.0
    assert body["excess_integral"] == 6000.0


def test_gap_equal_to_limit_is_interpolated(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:10:00Z", 20.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:10:00Z", threshold=10.0
    )
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 600.0


def test_gap_one_second_over_limit_is_not_interpolated(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:10:01Z", 20.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:10:01Z", threshold=10.0
    )
    assert body["covered_seconds"] == 0.0
    assert body["exceedance_seconds"] == 0.0
    assert body["excess_integral"] == 0.0


def test_same_instant_samples_keep_the_highest_sequence(client):
    # Two observations at 08:00: the sequence-2 value (20.0) wins over the
    # sequence-1 value (0.0), so the whole segment sits above 10.
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 0.0),
            sample(2, "2026-09-23T08:00:00Z", 20.0),
            sample(3, "2026-09-23T08:10:00Z", 20.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:10:00Z", threshold=10.0
    )
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 600.0
    assert body["excess_integral"] == 6000.0


def test_fractional_seconds_are_not_truncated(client):
    # 0 -> 10 over 0.5s crosses the threshold 5 at t=0.25s.
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 0.0),
            sample(2, "2026-09-23T08:00:00.500000Z", 10.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:00:01Z", threshold=5.0
    )
    assert body["covered_seconds"] == 0.5
    assert body["exceedance_seconds"] == 0.25
    assert body["excess_integral"] == 0.625


def test_fractional_window_bounds_are_preserved(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:00:01Z", 20.0),
        ],
    )
    body = query_exposure(
        client,
        window_from="2026-09-23T08:00:00.250000Z",
        window_to="2026-09-23T08:00:00.750000Z",
        threshold=10.0,
    )
    assert body["covered_seconds"] == 0.5
    assert body["exceedance_seconds"] == 0.5
    assert body["excess_integral"] == 5.0


def test_v_shape_crosses_threshold_twice(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:10:00Z", 0.0),
            sample(3, "2026-09-23T08:20:00Z", 20.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:20:00Z", threshold=10.0
    )
    assert body["covered_seconds"] == 1200.0
    assert body["exceedance_seconds"] == 600.0
    # Two triangles, each 300s wide and 10 high.
    assert body["excess_integral"] == 3000.0


def test_threshold_zero_counts_every_positive_value(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 0.0),
            sample(2, "2026-09-23T08:10:00Z", 10.0),
        ],
    )
    body = query_exposure(
        client, window_to="2026-09-23T08:10:00Z", threshold=0.0
    )
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 600.0
    assert body["excess_integral"] == 3000.0


def test_no_samples_returns_zeros(client):
    body = query_exposure(client, device="sensor-unknown")
    assert body == {
        "device_id": "sensor-unknown",
        "from": "2026-09-23T08:00:00Z",
        "to": "2026-09-23T09:00:00Z",
        "threshold": 10.0,
        "covered_seconds": 0.0,
        "exceedance_seconds": 0.0,
        "excess_integral": 0.0,
    }


def test_single_sample_covers_nothing(client):
    post_records(client, [sample(1, "2026-09-23T08:05:00Z", 20.0)])
    body = query_exposure(client)
    assert body["covered_seconds"] == 0.0
    assert body["exceedance_seconds"] == 0.0
    assert body["excess_integral"] == 0.0


def test_window_outside_all_samples_returns_zeros(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T07:00:00Z", 20.0),
            sample(2, "2026-09-23T07:10:00Z", 20.0),
        ],
    )
    body = query_exposure(client)
    assert body["covered_seconds"] == 0.0
    assert body["exceedance_seconds"] == 0.0
    assert body["excess_integral"] == 0.0


def test_window_echo_is_utc_and_device_isolation(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:10:00Z", 20.0),
        ],
    )
    post_records(
        client,
        [sample(1, "2026-09-23T08:00:00Z", 50.0, device="sensor-b")],
    )
    body = query_exposure(
        client,
        window_from="2026-09-23T16:00:00+08:00",
        window_to="2026-09-23T16:10:00+08:00",
        threshold=10.0,
    )
    assert body["device_id"] == "sensor-a"
    assert body["from"] == "2026-09-23T08:00:00Z"
    assert body["to"] == "2026-09-23T08:10:00Z"
    assert body["threshold"] == 10.0
    assert body["covered_seconds"] == 600.0
    # The other device's single sample contributes nothing here.
    other = query_exposure(client, device="sensor-b")
    assert other["covered_seconds"] == 0.0


def test_late_writes_are_reflected_in_the_next_query(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:05:00Z", 20.0),
        ],
    )
    body = query_exposure(client, window_to="2026-09-23T08:30:00Z")
    assert body["covered_seconds"] == 300.0

    # A late sample extends the covered range; the next query reflects it.
    post_records(client, [sample(3, "2026-09-23T08:10:00Z", 20.0)])
    body = query_exposure(client, window_to="2026-09-23T08:30:00Z")
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 600.0
    assert body["excess_integral"] == 6000.0


def test_corrections_are_reflected_in_the_next_query(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 20.0),
            sample(2, "2026-09-23T08:10:00Z", 20.0),
        ],
    )
    body = query_exposure(client, window_to="2026-09-23T08:10:00Z")
    assert body["excess_integral"] == 6000.0

    response = client.post(
        "/records/correction",
        json={
            "device_id": "sensor-a",
            "sequence": 2,
            "old_sampled_at": "2026-09-23T08:10:00Z",
            "old_turbidity": 20.0,
            "new_sampled_at": "2026-09-23T08:10:00Z",
            "new_turbidity": 0.0,
        },
    )
    assert response.status_code == 200
    body = query_exposure(client, window_to="2026-09-23T08:10:00Z")
    # 20 -> 0 crosses 10 at 08:05: a 300s triangle of height 10.
    assert body["covered_seconds"] == 600.0
    assert body["exceedance_seconds"] == 300.0
    assert body["excess_integral"] == 1500.0


def test_query_does_not_modify_events_acks_or_change_stream(client):
    post_records(
        client,
        [
            sample(1, "2026-09-23T08:00:00Z", 12.0),
            sample(2, "2026-09-23T08:05:00Z", 13.0),
        ],
    )
    events = get_events(client, "sensor-a")
    assert ack_event(client, events["events"][0]).status_code == 200
    events_before = get_events(client, "sensor-a")
    changes_before = client.get(
        "/event-changes", params={"after": 0, "limit": 1000}
    ).json()

    body = query_exposure(client, window_to="2026-09-23T08:10:00Z")
    assert body["covered_seconds"] == 300.0

    assert get_events(client, "sensor-a") == events_before
    assert (
        client.get("/event-changes", params={"after": 0, "limit": 1000}).json()
        == changes_before
    )


def _client_with_gap(db_path, monkeypatch, gap_seconds: int) -> TestClient:
    monkeypatch.setenv("WATER_DATABASE_PATH", str(db_path))
    monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
    monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", str(gap_seconds))

    from app.main import app

    return TestClient(app)


def test_exposure_uses_the_current_max_sample_gap(tmp_path, monkeypatch):
    # Two samples 900s apart: not interpolated under a 600s limit, but
    # interpolated after a restart widens the limit to 1200s.
    db_path = tmp_path / "water.db"
    payload = [
        sample(1, "2026-09-23T08:00:00Z", 20.0),
        sample(2, "2026-09-23T08:15:00Z", 20.0),
    ]
    with _client_with_gap(db_path, monkeypatch, 600) as client:
        post_records(client, payload)
        body = query_exposure(client, window_to="2026-09-23T08:15:00Z")
        assert body["covered_seconds"] == 0.0

    with _client_with_gap(db_path, monkeypatch, 1200) as client:
        body = query_exposure(client, window_to="2026-09-23T08:15:00Z")
        assert body["covered_seconds"] == 900.0
        assert body["exceedance_seconds"] == 900.0
        assert body["excess_integral"] == 9000.0


def test_invalid_window_returns_422(client):
    bad_windows = [
        ("2026-09-23T09:00:00Z", "2026-09-23T08:00:00Z"),  # from after to
        ("2026-09-23T08:00:00Z", "2026-09-23T08:00:00Z"),  # equal instants
        ("2026-09-23T10:00:00+02:00", "2026-09-23T08:00:00Z"),  # equal in UTC
        ("2026-09-23T08:00:00", "2026-09-23T09:00:00Z"),  # naive from
        ("2026-09-23T08:00:00Z", "2026-09-23T09:00:00"),  # naive to
    ]
    for window_from, window_to in bad_windows:
        response = client.post(
            "/devices/sensor-a/turbidity-exposure",
            json={"from": window_from, "to": window_to, "threshold": 10.0},
        )
        assert response.status_code == 422, (window_from, window_to)


def test_invalid_threshold_returns_422(client):
    for threshold in (-1, -0.5, "ten"):
        response = client.post(
            "/devices/sensor-a/turbidity-exposure",
            json={
                "from": "2026-09-23T08:00:00Z",
                "to": "2026-09-23T09:00:00Z",
                "threshold": threshold,
            },
        )
        assert response.status_code == 422, threshold


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_threshold_returns_422(client, literal):
    # httpx refuses to serialize non-finite floats, so send the raw JSON
    # literals (Python's parser accepts them) as a strict client would.
    response = client.post(
        "/devices/sensor-a/turbidity-exposure",
        content=(
            '{"from": "2026-09-23T08:00:00Z", "to": "2026-09-23T09:00:00Z",'
            f'"threshold": {literal}}}'
        ),
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 422


def test_missing_and_extra_fields_return_422(client):
    base = {
        "from": "2026-09-23T08:00:00Z",
        "to": "2026-09-23T09:00:00Z",
        "threshold": 10.0,
    }
    for key in ("from", "to", "threshold"):
        payload = {name: value for name, value in base.items() if name != key}
        response = client.post(
            "/devices/sensor-a/turbidity-exposure", json=payload
        )
        assert response.status_code == 422, key
    response = client.post(
        "/devices/sensor-a/turbidity-exposure", json={**base, "extra": 1}
    )
    assert response.status_code == 422
