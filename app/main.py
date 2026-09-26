from __future__ import annotations

import math
from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Query, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from . import db
from .config import Settings
from .schemas import (
    AcknowledgeIn,
    BatchIn,
    BatchOut,
    CoincidentAlarmsIn,
    CoincidentAlarmsOut,
    CorrectionIn,
    CorrectionOut,
    EventChangeListOut,
    EventEvidenceOut,
    EventListOut,
    EventWithAckOut,
    ThresholdsOut,
    TurbidityExposureIn,
    TurbidityExposureOut,
    ConflictError,
    NotFoundError,
)
from .service import WaterQualityService



@asynccontextmanager
async def lifespan(app: FastAPI) -> Iterator[None]:
    settings = Settings()
    connection = db.connect(settings.database_path)
    service = WaterQualityService(connection, settings)
    service.initialize()
    service.reconcile_settings()
    app.state.settings = settings
    app.state.connection = connection
    app.state.service = service
    try:
        yield
    finally:
        connection.close()


app = FastAPI(
    title="Water Quality Sampling Events API",
    version="1.0.0",
    description=(
        "Deduplicates device samples in SQLite and derives hysteretic "
        "turbidity alarm events, including observation-gap interruption, "
        "late sample recomputation, and explicit sample correction."
    ),
    lifespan=lifespan,
)


def get_service() -> WaterQualityService:
    return app.state.service


@app.exception_handler(ConflictError)
def handle_conflict(_request, exc: ConflictError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_409_CONFLICT,
        content={"detail": exc.detail},
    )


def _json_safe(value):
    """Make a validation-error payload renderable as strict JSON.

    Python's JSON parser accepts the non-standard NaN/Infinity literals,
    so a request can carry a non-finite float that pydantic then rejects;
    the offending value is echoed inside the error details, where the
    strict JSON renderer (allow_nan=False) could not serialize it and the
    422 response itself would fail. Such floats are rendered as their
    string form instead; every other payload is returned unchanged.
    """
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    return value


@app.exception_handler(RequestValidationError)
def handle_request_validation(
    _request, exc: RequestValidationError
) -> JSONResponse:
    return JSONResponse(
        status_code=422,
        content={"detail": _json_safe(jsonable_encoder(exc.errors()))},
    )


@app.exception_handler(NotFoundError)
def handle_not_found(_request, exc: NotFoundError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"detail": exc.detail},
    )


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/thresholds", response_model=ThresholdsOut, tags=["config"])
def thresholds(
    service: Annotated[WaterQualityService, Depends(get_service)],
) -> ThresholdsOut:
    return service.get_thresholds()


@app.post(
    "/records",
    response_model=BatchOut,
    status_code=status.HTTP_201_CREATED,
    tags=["records"],
)
def submit_records(
    batch: BatchIn,
    service: Annotated[WaterQualityService, Depends(get_service)],
) -> BatchOut:
    """Insert one idempotent batch and rebuild each affected device."""
    return service.ingest_batch(batch)


@app.post(
    "/records/replay",
    response_model=BatchOut,
    tags=["records"],
)
def replay_records(
    batch: BatchIn,
    service: Annotated[WaterQualityService, Depends(get_service)],
) -> BatchOut:
    """
    Replay the same batch safely.

    Identical records are idempotent; affected devices are still rebuilt and
    their deterministic event result is returned.
    """
    return service.ingest_batch(batch)


@app.post(
    "/records/correction",
    response_model=CorrectionOut,
    tags=["records"],
)
def correct_record(
    correction: CorrectionIn,
    service: Annotated[WaterQualityService, Depends(get_service)],
) -> CorrectionOut:
    """
    Correct one stored record located by (device_id, sequence).

    Only sampled_at and turbidity may change. The stored values must equal
    the provided old values (compared in UTC): 404 when no such record
    exists, 409 when the old values do not match; neither failure modifies
    any data. On success the update and the recomputed alarm events for the
    device commit in one transaction.
    """
    return service.correct_record(correction)


@app.get(
    "/devices/{device_id}/events",
    response_model=EventListOut,
    tags=["events"],
)
def device_events(
    device_id: str,
    service: Annotated[WaterQualityService, Depends(get_service)],
    event_status: Annotated[
        Literal["active", "interrupted", "recovered"] | None,
        Query(alias="status"),
    ] = None,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> EventListOut:
    if not device_id.strip():
        return EventListOut(
            device_id="",
            status=event_status,
            limit=limit,
            offset=offset,
            total=0,
            events=[],
        )
    events, total = service.list_events(
        device_id=device_id.strip(),
        status=event_status,
        limit=limit,
        offset=offset,
    )
    return EventListOut(
        device_id=device_id.strip(),
        status=event_status,
        limit=limit,
        offset=offset,
        total=total,
        events=events,
    )


@app.get(
    "/devices/{device_id}/events/{event_id}/evidence",
    response_model=EventEvidenceOut,
    tags=["events"],
)
def event_evidence(
    device_id: str,
    event_id: str,
    service: Annotated[WaterQualityService, Depends(get_service)],
) -> EventEvidenceOut:
    """
    Explain which samples formed one current alarm event.

    The stored event, the effective alarm/recovery thresholds and the
    device's raw samples are all read from one consistent snapshot; the
    same deterministic hysteresis state machine as the write path then
    lists the event members in (sampled_at, sequence) order, each marked
    as the trigger, a continuing sample or the recovery sample. A
    recovered event's final recovery sample is included, an interrupted
    event additionally carries the following sample that exposed the
    over-limit gap (with the gap seconds), and an active event carries no
    ending evidence. The embedded event omits the acknowledgement state.
    Returns 404 when the event does not exist or belongs to another
    device. The query is strictly read-only.
    """
    return service.get_event_evidence(
        device_id=device_id.strip(),
        event_id=event_id,
    )


@app.post(
    "/devices/{device_id}/events/{event_id}/ack",
    response_model=EventWithAckOut,
    tags=["events"],
)
def acknowledge_event(
    device_id: str,
    event_id: str,
    payload: AcknowledgeIn,
    service: Annotated[WaterQualityService, Depends(get_service)],
) -> EventWithAckOut:
    """
    Manually acknowledge one alarm event.

    The request body's ``expected_event`` must carry every event field
    exactly as returned by ``GET /devices/{device_id}/events`` (without
    the ``acknowledged`` flag). The acknowledgement is accepted only when
    those fields match the currently stored event field by field: 404 when
    the event does not exist, 409 when any field differs (the client's
    version is stale). Repeating the request for the same stored version
    is idempotent. The acknowledgement never appears in the event change
    stream.
    """
    return service.acknowledge_event(
        device_id=device_id.strip(),
        event_id=event_id,
        payload=payload,
    )


@app.post(
    "/devices/{device_id}/turbidity-exposure",
    response_model=TurbidityExposureOut,
    tags=["measurements"],
)
def turbidity_exposure(
    device_id: str,
    payload: TurbidityExposureIn,
    service: Annotated[WaterQualityService, Depends(get_service)],
) -> TurbidityExposureOut:
    """
    Accumulate above-threshold turbidity exposure over one time window.

    The request carries a timezone-aware [from, to] window (from < to)
    and a non-negative finite threshold. The device's raw samples are
    read in (sampled_at, sequence) order from one consistent read-only
    snapshot, collapsing same-instant samples to the highest sequence.
    Adjacent points at most max_sample_gap_seconds apart are joined by
    linear interpolation; wider gaps and the ranges before the first and
    after the last sample are never inferred. Segments are clipped to
    [from, to] and split at their threshold crossings, yielding
    covered_seconds, exceedance_seconds and excess_integral (the area
    above the threshold, in turbidity*seconds) with fractional seconds
    preserved; all three are 0 when nothing covers the window. The query
    is read-only and never touches samples, events, acknowledgements or
    the change stream.
    """
    return service.turbidity_exposure(
        device_id=device_id.strip(),
        payload=payload,
    )


@app.post(
    "/coincident-alarms",
    response_model=CoincidentAlarmsOut,
    tags=["events"],
)
def coincident_alarms(
    payload: CoincidentAlarmsIn,
    service: Annotated[WaterQualityService, Depends(get_service)],
) -> CoincidentAlarmsOut:
    """
    Find the spans where at least ``quorum`` devices alarm together.

    The request names 2..50 distinct device ids, an integer quorum between
    2 and the device count, and a timezone-aware [from, to) window. The
    answer is computed from one consistent read of the committed events:
    recovered/interrupted events cover [start_at, end_at), active events
    [start_at, to), all clipped to the window with zero-length remainders
    dropped. Each device counts at most once per instant; segments split
    wherever the participating device set changes and only adjacent spans
    with an identical set merge. The query is read-only and never touches
    samples, events, acknowledgements or the change stream.
    """
    return service.list_coincident_alarms(payload)


@app.get(
    "/event-changes",
    response_model=EventChangeListOut,
    tags=["events"],
)
def event_changes(
    service: Annotated[WaterQualityService, Depends(get_service)],
    after: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> EventChangeListOut:
    """
    Incremental replica stream of derived events.

    Returns up to ``limit`` changes with change_id strictly greater than
    the ``after`` cursor, ordered ascending by change_id. Each change is
    either an ``upsert`` (new or changed event, with the full event) or a
    ``delete`` (disappeared event, with event_id only). ``next_cursor``
    is the cursor for the following fetch; an empty page echoes ``after``
    unchanged. ``head_change_id`` is the newest committed change id (0
    before the first change).
    """
    return service.list_event_changes(after=after, limit=limit)
