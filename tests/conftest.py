from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


@pytest.fixture
def client(tmp_path, monkeypatch) -> TestClient:
    monkeypatch.setenv("WATER_DATABASE_PATH", str(tmp_path / "water.db"))
    monkeypatch.setenv("WATER_ALARM_THRESHOLD", "10")
    monkeypatch.setenv("WATER_RECOVERY_THRESHOLD", "5")
    monkeypatch.setenv("WATER_MAX_SAMPLE_GAP_SECONDS", "600")

    from app.main import app

    with TestClient(app) as test_client:
        yield test_client


def post_records(client: TestClient, records: list[dict], replay: bool = False):
    path = "/records/replay" if replay else "/records"
    response = client.post(path, json={"records": records})
    assert response.status_code in (200, 201)
    return response


def strip_acknowledged(events: list[dict]) -> list[dict]:
    """Drop the GET-only acknowledged flag for cross-endpoint comparisons."""
    return [
        {key: value for key, value in event.items() if key != "acknowledged"}
        for event in events
    ]


def get_events(client: TestClient, device: str = "sensor-a", **params) -> dict:
    response = client.get(f"/devices/{device}/events", params=params)
    assert response.status_code == 200
    return response.json()


def ack_event(
    client: TestClient,
    event: dict,
    *,
    device: str | None = None,
    event_id: str | None = None,
):
    """Acknowledge an event, sending back its complete fields except flag."""
    expected = {
        key: value for key, value in event.items() if key != "acknowledged"
    }
    return client.post(
        f"/devices/{device or event['device_id']}"
        f"/events/{event_id or event['event_id']}/ack",
        json={"expected_event": expected},
    )
