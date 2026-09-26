from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

EventStatus = Literal["active", "interrupted", "recovered"]


def _stripped_device_id(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("device_id must not be empty")
    return value


def _finite_turbidity(value: float) -> float:
    if not math.isfinite(value):
        raise ValueError("turbidity must be a finite number")
    return value


def _aware_utc_timestamp(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("sampled_at must include a timezone, for example Z")
    return value.astimezone(timezone.utc)


class SampleIn(BaseModel):
    device_id: str = Field(min_length=1, examples=["sensor-01"])
    sequence: int = Field(ge=0, examples=[1])
    sampled_at: datetime = Field(examples=["2026-09-23T08:00:00Z"])
    turbidity: float = Field(ge=0.0, examples=[12.4])

    model_config = ConfigDict(extra="forbid")

    @field_validator("device_id")
    @classmethod
    def reject_empty_device(cls, value: str) -> str:
        return _stripped_device_id(value)

    @field_validator("turbidity")
    @classmethod
    def reject_non_finite(cls, value: float) -> float:
        return _finite_turbidity(value)

    @field_validator("sampled_at")
    @classmethod
    def require_aware_timestamp(cls, value: datetime) -> datetime:
        return _aware_utc_timestamp(value)


class CorrectionIn(BaseModel):
    """Explicit correction of one stored record located by (device_id, sequence).

    Only sampled_at and turbidity may change; the identity fields locate the
    record. The old values must match the stored record (timestamps compared
    in UTC) before the new values are applied.
    """

    device_id: str = Field(min_length=1, examples=["sensor-01"])
    sequence: int = Field(ge=0, examples=[2])
    old_sampled_at: datetime = Field(examples=["2026-09-23T08:01:00Z"])
    old_turbidity: float = Field(ge=0.0, examples=[12.4])
    new_sampled_at: datetime = Field(examples=["2026-09-23T08:01:30Z"])
    new_turbidity: float = Field(ge=0.0, examples=[11.5])

    model_config = ConfigDict(extra="forbid")

    @field_validator("device_id")
    @classmethod
    def reject_empty_device(cls, value: str) -> str:
        return _stripped_device_id(value)

    @field_validator("old_turbidity", "new_turbidity")
    @classmethod
    def reject_non_finite(cls, value: float) -> float:
        return _finite_turbidity(value)

    @field_validator("old_sampled_at", "new_sampled_at")
    @classmethod
    def require_aware_timestamp(cls, value: datetime) -> datetime:
        return _aware_utc_timestamp(value)


class BatchIn(BaseModel):
    records: list[SampleIn] = Field(min_length=1, max_length=10_000)


class CoincidentAlarmsIn(BaseModel):
    """Cross-device coincident-alarm query over one time window.

    ``device_ids`` holds 2..50 distinct device ids (whitespace-stripped and
    required to be non-empty, matching the write path's device id handling,
    and pairwise distinct after that normalization). ``quorum`` is the
    minimum number of devices that must be in alarm at the same instant and
    ranges from 2 to the number of requested devices. ``from``/``to`` bound
    the query window: both must be timezone-aware and are normalized to UTC,
    with ``from`` strictly before ``to``.
    """

    device_ids: list[str] = Field(min_length=2, max_length=50)
    quorum: int = Field(ge=2)
    from_: datetime = Field(alias="from", examples=["2026-09-23T08:00:00Z"])
    to: datetime = Field(examples=["2026-09-23T09:00:00Z"])

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @field_validator("device_ids")
    @classmethod
    def require_distinct_devices(cls, value: list[str]) -> list[str]:
        normalized = [_stripped_device_id(item) for item in value]
        if len(set(normalized)) != len(normalized):
            raise ValueError("device_ids must be distinct")
        return normalized

    @field_validator("from_", "to")
    @classmethod
    def require_aware_timestamp(cls, value: datetime) -> datetime:
        return _aware_utc_timestamp(value)

    @model_validator(mode="after")
    def validate_window_and_quorum(self) -> "CoincidentAlarmsIn":
        if self.from_ >= self.to:
            raise ValueError("from must be before to")
        if self.quorum > len(self.device_ids):
            raise ValueError(
                "quorum must not exceed the number of device_ids"
            )
        return self


class TurbidityExposureIn(BaseModel):
    """Above-threshold turbidity exposure query for one device.

    ``from``/``to`` bound the computation window: both must be
    timezone-aware and are normalized to UTC, with ``from`` strictly
    before ``to``. ``threshold`` is the non-negative finite turbidity
    level the exposure is accumulated against.
    """

    from_: datetime = Field(alias="from", examples=["2026-09-23T08:00:00Z"])
    to: datetime = Field(examples=["2026-09-23T09:00:00Z"])
    threshold: float = Field(ge=0.0, examples=[10.0])

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    @field_validator("from_", "to")
    @classmethod
    def require_aware_timestamp(cls, value: datetime) -> datetime:
        return _aware_utc_timestamp(value)

    @field_validator("threshold")
    @classmethod
    def reject_non_finite(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("threshold must be a finite number")
        return value

    @model_validator(mode="after")
    def validate_window(self) -> "TurbidityExposureIn":
        if self.from_ >= self.to:
            raise ValueError("from must be before to")
        return self


class EventOut(BaseModel):
    event_id: str
    device_id: str
    status: EventStatus
    start_sequence: int
    end_sequence: int | None
    start_at: datetime
    end_at: datetime | None
    peak_turbidity: float
    peak_sequence: int
    sample_count: int


class ExpectedEventIn(EventOut):
    """The complete event version an acknowledgement request is based on.

    Clients send back every event field exactly as returned by the event
    query (apart from ``acknowledged`` itself); no extra fields are
    accepted. Timestamps must remain timezone-aware and are normalized to
    UTC before being compared field by field with the stored event, the
    same comparison ingest dedup and correction use.
    """

    model_config = ConfigDict(extra="forbid")

    @field_validator("start_at", "end_at")
    @classmethod
    def require_aware_timestamp(cls, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        return _aware_utc_timestamp(value)


class AcknowledgeIn(BaseModel):
    expected_event: ExpectedEventIn

    model_config = ConfigDict(extra="forbid")


class EventWithAckOut(EventOut):
    """An event together with its manual acknowledgement state.

    This is the query representation; the change stream continues to use
    :class:`EventOut`, so acknowledgements never alter its payload.
    """

    acknowledged: bool = False


class EventListOut(BaseModel):
    device_id: str
    status: EventStatus | None
    limit: int
    offset: int
    total: int
    events: list[EventWithAckOut]


EvidenceRole = Literal["trigger", "continuing", "recovery"]


class SampleFactOut(BaseModel):
    """A raw device sample referenced outside event membership."""

    sequence: int
    sampled_at: datetime
    turbidity: float


class EvidenceSampleOut(SampleFactOut):
    """One raw sample that participates in an alarm event.

    The samples are ordered exactly as the state machine consumes them:
    sampled_at ascending with sequence as the tie-breaker, so samples at
    the same timestamp are located by sequence rather than sliced apart by
    time alone. ``role`` says how the sample relates to the alarm: the
    first member is the ``trigger``; a recovered event's last member is
    the ``recovery`` sample; every other member is ``continuing``.
    ``peak`` marks the single peak sample (``peak_sequence`` of the
    event), the first hit in (sampled_at, sequence) order when values tie.
    """

    role: EvidenceRole
    peak: bool


class InterruptionEvidenceOut(BaseModel):
    """Why an interrupted event lost observation continuity.

    ``sample`` is the next sample of the device in (sampled_at, sequence)
    order after the event's last member -- the very sample whose arrival
    exposed the over-limit gap; it is not part of the event.
    ``gap_seconds`` is the interval from the event's last member to that
    sample (always strictly greater than max_sample_gap_seconds).
    """

    sample: SampleFactOut
    gap_seconds: int


class EventEvidenceOut(BaseModel):
    """The exact samples that formed one current alarm event.

    The effective thresholds and the device samples all come from one
    consistent read snapshot with the event itself. The event is embedded
    without the query-only ``acknowledged`` flag. ``interruption`` is
    populated only for ``interrupted`` events; an ``active`` event
    carries no ending evidence at all.
    """

    event: EventOut
    alarm_threshold: float
    recovery_threshold: float
    max_sample_gap_seconds: int
    samples: list[EvidenceSampleOut]
    interruption: InterruptionEvidenceOut | None = None


ChangeType = Literal["upsert", "delete"]


class EventChangeOut(BaseModel):
    """One increment in the derived-event change stream.

    An ``upsert`` carries the full recomputed event (both newly created
    events and existing events whose fields changed); a ``delete``
    identifies an event that disappeared from the device's recomputed
    set by its event_id alone.
    """

    change_id: int
    device_id: str
    event_id: str
    change_type: ChangeType
    event: EventOut | None


class EventChangeListOut(BaseModel):
    after: int
    limit: int
    next_cursor: int
    head_change_id: int
    changes: list[EventChangeOut]


class CoincidentSegmentOut(BaseModel):
    """One maximal span with at least ``quorum`` devices in alarm.

    ``start``/``end`` are UTC instants of the half-open span [start, end);
    ``device_ids`` lists the participating devices in lexicographic order.
    """

    start: datetime
    end: datetime
    device_ids: list[str]


class CoincidentAlarmsOut(BaseModel):
    from_: datetime = Field(alias="from")
    to: datetime
    quorum: int
    device_ids: list[str]
    segments: list[CoincidentSegmentOut]

    model_config = ConfigDict(populate_by_name=True)


class TurbidityExposureOut(BaseModel):
    """Cumulative above-threshold turbidity exposure over one window.

    ``covered_seconds`` is the total time within [from, to] covered by
    interpolated segments; ``exceedance_seconds`` is the part of it where
    the interpolated turbidity is above ``threshold``; ``excess_integral``
    is the area between the interpolated curve and the threshold while
    above it, in turbidity*seconds. All three keep fractional seconds and
    are 0 when no segment covers the window.
    """

    device_id: str
    from_: datetime = Field(alias="from")
    to: datetime
    threshold: float
    covered_seconds: float
    exceedance_seconds: float
    excess_integral: float

    model_config = ConfigDict(populate_by_name=True)


class DeviceEvents(BaseModel):
    device_id: str
    events: list[EventOut]


class BatchOut(BaseModel):
    received: int
    inserted: int
    duplicate_identical: int
    affected_devices: list[str]
    events: list[DeviceEvents]


class CorrectionOut(BaseModel):
    device_id: str
    sequence: int
    sampled_at: datetime
    turbidity: float
    events: list[EventOut]


class ThresholdsOut(BaseModel):
    alarm_threshold: float
    recovery_threshold: float
    max_sample_gap_seconds: int


class ConflictError(Exception):
    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class NotFoundError(Exception):
    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail
