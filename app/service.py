from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

from . import db
from .config import Settings
from .schemas import (
    AcknowledgeIn,
    BatchIn,
    BatchOut,
    CoincidentAlarmsIn,
    CoincidentAlarmsOut,
    CoincidentSegmentOut,
    CorrectionIn,
    CorrectionOut,
    DeviceEvents,
    EventChangeListOut,
    EventChangeOut,
    EventEvidenceOut,
    EventOut,
    EventWithAckOut,
    EvidenceSampleOut,
    InterruptionEvidenceOut,
    SampleFactOut,
    SampleIn,
    ThresholdsOut,
    TurbidityExposureIn,
    TurbidityExposureOut,
    ConflictError,
    NotFoundError,
)

META_ALARM = "alarm_threshold"
META_RECOVERY = "recovery_threshold"
META_MAX_SAMPLE_GAP = "max_sample_gap_seconds"
EVENT_ID_PREFIX = "evt:"


@dataclass(frozen=True)
class _RawSample:
    """One raw fact in the state machine's consumption order."""

    device_id: str
    sequence: int
    sampled_at: str
    instant: datetime
    turbidity: float


@dataclass
class _DerivedEvent:
    """An event while the hysteresis state machine builds it.

    ``members`` keeps the raw samples that form the event in
    (sampled_at, sequence) order; ``interruptor`` is set only for an
    event closed as ``interrupted`` and holds the following sample whose
    gap from the last member exceeded the limit. The same derivation runs
    when rebuilding persisted events and when answering an evidence query.
    """

    start_sequence: int
    start_at: str
    members: list[_RawSample]
    peak_turbidity: float
    peak_sequence: int
    status: str
    end_sequence: int | None = None
    end_at: str | None = None
    interruptor: _RawSample | None = None

    @property
    def sample_count(self) -> int:
        return len(self.members)

    def to_event_out(self, device_id: str) -> EventOut:
        return EventOut(
            event_id=_DerivedEvent.event_id(
                device_id, self.start_sequence
            ),
            device_id=device_id,
            status=self.status,  # type: ignore[arg-type]
            start_sequence=self.start_sequence,
            end_sequence=self.end_sequence,
            start_at=WaterQualityService._parse_timestamp(self.start_at),
            end_at=None
            if self.end_at is None
            else WaterQualityService._parse_timestamp(self.end_at),
            peak_turbidity=self.peak_turbidity,
            peak_sequence=self.peak_sequence,
            sample_count=self.sample_count,
        )

    @staticmethod
    def event_id(device_id: str, start_sequence: int) -> str:
        return (
            f"{EVENT_ID_PREFIX}{quote(device_id, safe='')}:{start_sequence}"
        )


class WaterQualityService:
    def __init__(
        self,
        connection: sqlite3.Connection,
        settings: Settings,
    ) -> None:
        self._connection = connection
        self._settings = settings
        # SQLite enforces process-level atomicity; the lock also serializes the
        # read-check-insert-rebuild critical section between request threads.
        self._write_lock = threading.RLock()
        self._schema_migrated = False

    def initialize(self) -> None:
        self._schema_migrated = db.initialize(self._connection)

    def reconcile_settings(self) -> None:
        """Store effective settings and rebuild all events if they changed."""
        with self._write_lock, db.write_transaction(self._connection):
            desired = {
                META_ALARM: str(self._settings.alarm_threshold),
                META_RECOVERY: str(self._settings.recovery_threshold),
                META_MAX_SAMPLE_GAP: str(
                    self._settings.max_sample_gap_seconds
                ),
            }
            changed = False
            for key, value in desired.items():
                if db.get_meta(self._connection, key) != value:
                    db.set_meta(self._connection, key, value)
                    changed = True

            # A schema migration dropped the derived events table, so it must
            # be rebuilt even when the settings themselves are unchanged.
            if changed or self._schema_migrated:
                for device_id in self._all_device_ids():
                    self._rebuild_device(device_id)

            if self._schema_migrated:
                # The upgrade completes only when the recomputed events and
                # the new schema_version commit in this same transaction. A
                # crash before the commit leaves the old version stored, so
                # the next start replays the migration and rebuilds the
                # events from the preserved raw samples again.
                db.set_meta(
                    self._connection, "schema_version", str(db.SCHEMA_VERSION)
                )
                self._schema_migrated = False

    def get_thresholds(self) -> ThresholdsOut:
        return ThresholdsOut(
            alarm_threshold=self._settings.alarm_threshold,
            recovery_threshold=self._settings.recovery_threshold,
            max_sample_gap_seconds=self._settings.max_sample_gap_seconds,
        )

    def ingest_batch(self, batch: BatchIn) -> BatchOut:
        unique_records: dict[tuple[str, int], SampleIn] = {}
        received = len(batch.records)
        duplicate_identical = 0
        requested_devices = {record.device_id for record in batch.records}

        # Make repeated submission of the exact same client payload safe.
        for record in batch.records:
            key = (record.device_id, record.sequence)
            existing_in_batch = unique_records.get(key)
            if existing_in_batch is not None:
                if self._same_observation(existing_in_batch, record):
                    duplicate_identical += 1
                    continue
                raise ConflictError(
                    f"records in the same batch conflict for device_id="
                    f"{record.device_id!r}, sequence={record.sequence}"
                )
            unique_records[key] = record

        with self._write_lock, db.write_transaction(self._connection):
            inserted = 0

            for record in unique_records.values():
                existing = self._connection.execute(
                    """
                    SELECT sampled_at, turbidity
                    FROM samples
                    WHERE device_id = ? AND sequence = ?
                    """,
                    (record.device_id, record.sequence),
                ).fetchone()

                if existing is not None:
                    if self._matches_existing(record, existing):
                        duplicate_identical += 1
                        continue
                    raise ConflictError(
                        f"existing record for device_id={record.device_id!r}, "
                        f"sequence={record.sequence} has different "
                        "sampled_at or turbidity"
                    )

                self._connection.execute(
                    """
                    INSERT INTO samples
                        (device_id, sequence, sampled_at, turbidity)
                    VALUES (?, ?, ?, ?)
                    """,
                    (
                        record.device_id,
                        record.sequence,
                        self._timestamp_text(record.sampled_at),
                        float(record.turbidity),
                    ),
                )
                inserted += 1

            # Rebuild every device named in the request, even when the whole
            # request was a replay. Recomputing from ordered raw facts is what
            # makes late arrivals, duplicate replays, and out-of-order batches
            # deterministic. Each device's previous event set is diffed
            # against its recomputed set and the differences are appended to
            # the change stream inside this same transaction; identical
            # replays produce an empty diff and therefore no changes.
            result_events: list[DeviceEvents] = []
            pending_changes: list[tuple[str, str, EventOut | None]] = []
            for device_id in sorted(requested_devices):
                device_events, changes = self._rebuild_device_with_changes(
                    device_id
                )
                result_events.append(device_events)
                pending_changes.extend(changes)

            self._append_event_changes(pending_changes)

        return BatchOut(
            received=received,
            inserted=inserted,
            duplicate_identical=duplicate_identical,
            affected_devices=sorted(requested_devices),
            events=result_events,
        )

    def correct_record(self, correction: CorrectionIn) -> CorrectionOut:
        """Overwrite sampled_at/turbidity of one stored record and rebuild.

        The record is located by (device_id, sequence); only the observation
        values may change. The update and the derived-event recomputation for
        that device commit in the same transaction, so a rejected correction
        or a failed rebuild leaves the stored record untouched.
        """
        with self._write_lock, db.write_transaction(self._connection):
            row = self._connection.execute(
                """
                SELECT sampled_at, turbidity
                FROM samples
                WHERE device_id = ? AND sequence = ?
                """,
                (correction.device_id, correction.sequence),
            ).fetchone()

            if row is None:
                raise NotFoundError(
                    f"no record for device_id={correction.device_id!r}, "
                    f"sequence={correction.sequence}"
                )

            # Optimistic concurrency: the caller must prove it saw the current
            # values. Comparison follows the same UTC normalization as ingest
            # dedup, so equivalent instants in different offsets match.
            if not self._stored_row_matches(
                row, correction.old_sampled_at, correction.old_turbidity
            ):
                raise ConflictError(
                    f"stored record for device_id={correction.device_id!r}, "
                    f"sequence={correction.sequence} does not match the "
                    "provided old sampled_at or turbidity"
                )

            self._connection.execute(
                """
                UPDATE samples
                SET sampled_at = ?, turbidity = ?
                WHERE device_id = ? AND sequence = ?
                """,
                (
                    self._timestamp_text(correction.new_sampled_at),
                    float(correction.new_turbidity),
                    correction.device_id,
                    correction.sequence,
                ),
            )

            # Only the corrected device is recomputed; other devices' derived
            # events are untouched. The change stream entries for that device
            # are appended within this transaction, so a rejected correction
            # or a failed rebuild leaves both events and changes untouched.
            device_events, changes = self._rebuild_device_with_changes(
                correction.device_id
            )
            self._append_event_changes(changes)
            events = device_events.events

        return CorrectionOut(
            device_id=correction.device_id,
            sequence=correction.sequence,
            sampled_at=correction.new_sampled_at,
            turbidity=float(correction.new_turbidity),
            events=events,
        )

    def list_events(
        self,
        device_id: str,
        status: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> tuple[list[EventWithAckOut], int]:
        with self._write_lock:
            return self._list_events(device_id, status, limit, offset)

    def get_event_evidence(
        self, device_id: str, event_id: str
    ) -> EventEvidenceOut:
        """Explain which samples formed one current alarm event.

        One consistent read snapshot fetches the stored event, the
        effective thresholds (persisted in schema_meta next to the derived
        events) and every raw sample of the device; the same deterministic
        state machine as the write path then pinpoints the event's member
        samples in (sampled_at, sequence) order rather than slicing by
        time. A missing event (or one that does not belong to the path's
        device) yields 404. Late writes, corrections or setting changes
        are reflected by the next call; this query never writes events,
        acknowledgements or change-stream rows.
        """
        with self._write_lock, db.read_transaction(self._connection):
            row = self._connection.execute(
                "SELECT * FROM events WHERE event_id = ? AND device_id = ?",
                (event_id, device_id),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"no event for device_id={device_id!r}, "
                    f"event_id={event_id!r}"
                )
            stored = self._row_to_event(row)

            alarm_threshold = float(
                db.get_meta(self._connection, META_ALARM)
                or self._settings.alarm_threshold
            )
            recovery_threshold = float(
                db.get_meta(self._connection, META_RECOVERY)
                or self._settings.recovery_threshold
            )
            max_gap_seconds = int(
                db.get_meta(self._connection, META_MAX_SAMPLE_GAP)
                or self._settings.max_sample_gap_seconds
            )

            samples = self._load_raw_samples(device_id)
            derived = self._derive_device_events(
                device_id,
                samples,
                alarm_threshold=alarm_threshold,
                recovery_threshold=recovery_threshold,
                max_sample_gap_seconds=max_gap_seconds,
            )
            match = next(
                (
                    event
                    for event in derived
                    if _DerivedEvent.event_id(
                        device_id, event.start_sequence
                    )
                    == event_id
                ),
                None,
            )
            # Events are a pure derivation of the samples plus the stored
            # settings, so within one snapshot the stored event and the
            # re-derived event must be identical; treating a mismatch as
            # missing keeps a stale/unknown id a 404 instead of answering
            # with partial evidence.
            if match is None or match.to_event_out(device_id) != stored:
                raise NotFoundError(
                    f"no event for device_id={device_id!r}, "
                    f"event_id={event_id!r}"
                )

            members = match.members
            assert len(members) == stored.sample_count

            evidence_samples: list[EvidenceSampleOut] = []
            for index, member in enumerate(members):
                if index == 0:
                    role = "trigger"
                elif (
                    stored.status == "recovered"
                    and index == len(members) - 1
                ):
                    # The recovered event's final member is the observed
                    # sample at or below the recovery threshold.
                    role = "recovery"
                else:
                    role = "continuing"
                evidence_samples.append(
                    EvidenceSampleOut(
                        sequence=member.sequence,
                        sampled_at=member.instant,
                        turbidity=member.turbidity,
                        role=role,  # type: ignore[arg-type]
                        peak=member.sequence == stored.peak_sequence,
                    )
                )

            interruption: InterruptionEvidenceOut | None = None
            if stored.status == "interrupted":
                # An interrupted event is always closed by encountering a
                # later sample, so the interruptor is necessarily present.
                assert match.interruptor is not None
                interruptor = match.interruptor
                last_member = members[-1]
                gap_seconds = int(
                    (
                        interruptor.instant - last_member.instant
                    ).total_seconds()
                )
                interruption = InterruptionEvidenceOut(
                    sample=SampleFactOut(
                        sequence=interruptor.sequence,
                        sampled_at=interruptor.instant,
                        turbidity=interruptor.turbidity,
                    ),
                    gap_seconds=gap_seconds,
                )

        return EventEvidenceOut(
            event=stored,
            alarm_threshold=alarm_threshold,
            recovery_threshold=recovery_threshold,
            max_sample_gap_seconds=max_gap_seconds,
            samples=evidence_samples,
            interruption=interruption,
        )

    def _list_events(
        self,
        device_id: str,
        status: str | None,
        limit: int,
        offset: int,
    ) -> tuple[list[EventWithAckOut], int]:
        clauses = ["e.device_id = ?"]
        parameters: list[object] = [device_id]
        if status is not None:
            clauses.append("e.status = ?")
            parameters.append(status)

        where = " AND ".join(clauses)
        total = int(
            self._connection.execute(
                f"SELECT COUNT(*) FROM events e WHERE {where}", parameters
            ).fetchone()[0]
        )
        rows = self._connection.execute(
            f"""
            SELECT e.*, a.event_id IS NOT NULL AS acknowledged
            FROM events e
            LEFT JOIN event_acks a ON a.event_id = e.event_id
            WHERE {where}
            ORDER BY e.start_sequence, e.start_at, e.event_id
            LIMIT ? OFFSET ?
            """,
            (*parameters, limit, offset),
        ).fetchall()
        return [self._row_to_ack_event(row) for row in rows], total

    def acknowledge_event(
        self,
        device_id: str,
        event_id: str,
        payload: AcknowledgeIn,
    ) -> EventWithAckOut:
        """Acknowledge one event only at its exact expected version.

        The client-supplied event must be identical field by field to the
        stored event addressed by (device_id, event_id). A missing event
        yields 404; an event whose identity differs from the path or whose
        fields differ from the stored version yields 409 without inserting
        anything. A repeated acknowledgement of the same stored version is
        idempotent and also returns 200.
        """
        expected = EventOut.model_validate(payload.expected_event.model_dump())
        with self._write_lock, db.write_transaction(self._connection):
            row = self._connection.execute(
                """
                SELECT e.*, a.event_id IS NOT NULL AS acknowledged
                FROM events e
                LEFT JOIN event_acks a ON a.event_id = e.event_id
                WHERE e.event_id = ? AND e.device_id = ?
                """,
                (event_id, device_id),
            ).fetchone()
            if row is None:
                raise NotFoundError(
                    f"no event for device_id={device_id!r}, "
                    f"event_id={event_id!r}"
                )
            current = self._row_to_event(row)
            if (
                expected.event_id != event_id
                or expected.device_id != device_id
                or expected != current
            ):
                raise ConflictError(
                    f"expected event for device_id={device_id!r}, "
                    f"event_id={event_id!r} does not match the current "
                    "event; re-read the event and acknowledge again"
                )
            self._connection.execute(
                "INSERT OR IGNORE INTO event_acks (event_id) VALUES (?)",
                (event_id,),
            )
        # Success covers both the fresh acknowledgement and the idempotent
        # repeat of an already acknowledged version.
        return EventWithAckOut(**current.model_dump(), acknowledged=True)

    def list_event_changes(
        self,
        after: int = 0,
        limit: int = 100,
    ) -> EventChangeListOut:
        """Read the change stream in change_id order.

        ``after`` is an opaque cursor: only changes strictly above it are
        returned. On an empty page the cursor is echoed unchanged (there
        may simply be no new commits yet); otherwise next_cursor is the
        id of the last returned change. head_change_id is always the id
        of the most recently committed change, or 0 while the stream is
        empty.
        """
        with self._write_lock:
            head_row = self._connection.execute(
                "SELECT MAX(change_id) FROM event_changes"
            ).fetchone()
            head_change_id = 0 if head_row[0] is None else int(head_row[0])

            rows = self._connection.execute(
                """
                SELECT * FROM event_changes
                WHERE change_id > ?
                ORDER BY change_id ASC
                LIMIT ?
                """,
                (after, limit),
            ).fetchall()
            changes = [self._row_to_change(row) for row in rows]

        next_cursor = after if not changes else changes[-1].change_id
        return EventChangeListOut(
            after=after,
            limit=limit,
            next_cursor=next_cursor,
            head_change_id=head_change_id,
            changes=changes,
        )

    def list_coincident_alarms(
        self, payload: CoincidentAlarmsIn
    ) -> CoincidentAlarmsOut:
        """Find spans where at least ``quorum`` devices alarm together.

        A single consistent read of the committed derived events; manual
        acknowledgements are ignored and devices without stored events
        simply contribute nothing. Recovered/interrupted events cover
        [start_at, end_at) and active events [start_at, to); every interval
        is clipped to the [from, to) window and zero-length remainders are
        dropped. The sweep counts each device at most once per instant and
        applies every endpoint at the same timestamp together, so segment
        boundaries fall exactly where the participating device set changes
        and only adjacent spans with an identical set merge. The query is
        read-only: samples, events, acknowledgements and the change stream
        are never modified, and writes committed after the snapshot are
        reflected by the next query.
        """
        window_start = payload.from_
        window_end = payload.to
        placeholders = ", ".join("?" for _ in payload.device_ids)
        with self._write_lock:
            rows = self._connection.execute(
                f"""
                SELECT device_id, start_at, end_at
                FROM events
                WHERE device_id IN ({placeholders})
                """,
                tuple(payload.device_ids),
            ).fetchall()

        # Turn each event into a half-open [low, high) interval clipped to
        # the query window; zero-length remainders are dropped.
        intervals_by_device: dict[str, list[tuple[datetime, datetime]]] = {}
        for row in rows:
            start = self._parse_timestamp(str(row["start_at"]))
            end_at = row["end_at"]
            # An active event has no end sample; it covers up to ``to``.
            end = (
                window_end
                if end_at is None
                else self._parse_timestamp(str(end_at))
            )
            low = max(start, window_start)
            high = min(end, window_end)
            if low < high:
                intervals_by_device.setdefault(
                    str(row["device_id"]), []
                ).append((low, high))

        # Merge each device's intervals into disjoint spans so a device is
        # counted at most once at any instant, then collect the endpoints.
        starts_at: dict[datetime, set[str]] = {}
        ends_at: dict[datetime, set[str]] = {}
        for device_id, spans in intervals_by_device.items():
            spans.sort()
            merged: list[tuple[datetime, datetime]] = []
            for low, high in spans:
                if merged and low <= merged[-1][1]:
                    merged[-1] = (merged[-1][0], max(merged[-1][1], high))
                else:
                    merged.append((low, high))
            for low, high in merged:
                starts_at.setdefault(low, set()).add(device_id)
                ends_at.setdefault(high, set()).add(device_id)

        # Sweep the endpoints chronologically: at each instant apply every
        # departure and arrival together, so the participating set changes
        # atomically at shared endpoints.
        spans: list[tuple[datetime, datetime, frozenset[str]]] = []
        active: set[str] = set()
        previous: datetime | None = None
        for instant in sorted(set(starts_at) | set(ends_at)):
            if (
                previous is not None
                and previous < instant
                and len(active) >= payload.quorum
            ):
                spans.append((previous, instant, frozenset(active)))
            active -= ends_at.get(instant, set())
            active |= starts_at.get(instant, set())
            previous = instant

        # Only adjacent spans with an identical device set merge.
        merged_spans: list[tuple[datetime, datetime, frozenset[str]]] = []
        for start, end, devices in spans:
            if (
                merged_spans
                and merged_spans[-1][2] == devices
                and merged_spans[-1][1] == start
            ):
                merged_spans[-1] = (merged_spans[-1][0], end, devices)
            else:
                merged_spans.append((start, end, devices))

        return CoincidentAlarmsOut(
            from_=window_start,
            to=window_end,
            quorum=payload.quorum,
            device_ids=payload.device_ids,
            segments=[
                CoincidentSegmentOut(
                    start=start, end=end, device_ids=sorted(devices)
                )
                for start, end, devices in merged_spans
            ],
        )

    def turbidity_exposure(
        self, device_id: str, payload: TurbidityExposureIn
    ) -> TurbidityExposureOut:
        """Accumulate above-threshold turbidity exposure over one window.

        The device's raw samples and the effective max sample gap are read
        from one consistent read-only snapshot. Samples sharing an instant
        are collapsed to the one with the highest sequence; adjacent points
        at most max_sample_gap_seconds apart are joined by linear
        interpolation, while wider gaps and the ranges before the first and
        after the last sample stay unobserved. Every segment is clipped to
        [from, to] and split at its threshold crossings, so the exceedance
        time and the excess area follow the interpolated line exactly,
        fractional seconds included. The query never writes samples,
        events, acknowledgements or change-stream rows; writes committed
        after the snapshot are reflected by the next call.
        """
        with self._write_lock, db.read_transaction(self._connection):
            max_gap_seconds = int(
                db.get_meta(self._connection, META_MAX_SAMPLE_GAP)
                or self._settings.max_sample_gap_seconds
            )
            samples = self._load_raw_samples(device_id)

        window_start = payload.from_
        window_end = payload.to
        threshold = float(payload.threshold)
        max_gap = timedelta(seconds=max_gap_seconds)

        # Collapse samples sharing one instant to the highest-sequence
        # one. Sorting by the parsed instant (not the stored text) keeps
        # fractional-second timestamps in their true chronological order.
        points: list[_RawSample] = []
        for sample in sorted(
            samples, key=lambda item: (item.instant, item.sequence)
        ):
            if points and sample.instant == points[-1].instant:
                points[-1] = sample
            else:
                points.append(sample)

        covered_seconds = 0.0
        exceedance_seconds = 0.0
        excess_integral = 0.0
        for first, second in zip(points, points[1:]):
            # The gap check compares exact instants; only the interpolation
            # itself is done in float seconds.
            if second.instant - first.instant > max_gap:
                continue
            low = max(first.instant, window_start)
            high = min(second.instant, window_end)
            if high <= low:
                continue
            span_seconds = (second.instant - first.instant).total_seconds()
            duration = (high - low).total_seconds()
            low_value = first.turbidity + (
                second.turbidity - first.turbidity
            ) * ((low - first.instant).total_seconds() / span_seconds)
            high_value = first.turbidity + (
                second.turbidity - first.turbidity
            ) * ((high - first.instant).total_seconds() / span_seconds)
            covered_seconds += duration

            low_above = low_value > threshold
            high_above = high_value > threshold
            if low_above and high_above:
                exceedance_seconds += duration
                excess_integral += (
                    (low_value - threshold) + (high_value - threshold)
                ) / 2.0 * duration
            elif low_above or high_above:
                # The line crosses the threshold once inside the clipped
                # segment; the above-threshold part is a right triangle.
                cross = (
                    (threshold - low_value)
                    / (high_value - low_value)
                    * duration
                )
                if high_above:
                    above_duration = duration - cross
                    peak_excess = high_value - threshold
                else:
                    above_duration = cross
                    peak_excess = low_value - threshold
                exceedance_seconds += above_duration
                excess_integral += 0.5 * above_duration * peak_excess

        return TurbidityExposureOut(
            device_id=device_id,
            from_=window_start,
            to=window_end,
            threshold=threshold,
            covered_seconds=covered_seconds,
            exceedance_seconds=exceedance_seconds,
            excess_integral=excess_integral,
        )

    def _rebuild_device(
        self, device_id: str
    ) -> tuple[dict[str, EventOut], DeviceEvents]:
        """Replace all derived events for one device with recomputed events.

        Returns the device's event sets before and after the rebuild. The
        pre-rebuild snapshot is taken in this same transaction before the
        events are deleted, so acknowledgement invalidation can compare
        every field and remove only the acknowledgements that were attached
        to an event that disappeared or changed (see
        ``_invalidate_acknowledgements``); an identical replay leaves every
        acknowledgement in place.
        """
        before = {
            event.event_id: event
            for event in self._events_for_device(device_id).events
        }

        samples = self._load_raw_samples(device_id)

        self._connection.execute(
            "DELETE FROM events WHERE device_id = ?", (device_id,)
        )

        derived = self._derive_device_events(
            device_id,
            samples,
            alarm_threshold=self._settings.alarm_threshold,
            recovery_threshold=self._settings.recovery_threshold,
            max_sample_gap_seconds=self._settings.max_sample_gap_seconds,
        )
        for event in derived:
            self._insert_event(device_id, event)

        result = self._events_for_device(device_id)
        self._invalidate_acknowledgements(before, result.events)
        return before, result

    def _load_raw_samples(self, device_id: str) -> list[_RawSample]:
        """Read every raw fact of one device in consumption order.

        The state machine orders by sampled_at and only uses sequence to
        break ties, so same-timestamp samples are located by sequence
        instead of being sliced apart by time alone.
        """
        rows = self._connection.execute(
            """
            SELECT sequence, sampled_at, turbidity
            FROM samples
            WHERE device_id = ?
            ORDER BY sampled_at ASC, sequence ASC
            """,
            (device_id,),
        ).fetchall()
        return [
            _RawSample(
                device_id=device_id,
                sequence=int(row["sequence"]),
                sampled_at=str(row["sampled_at"]),
                instant=self._parse_timestamp(str(row["sampled_at"])),
                turbidity=float(row["turbidity"]),
            )
            for row in rows
        ]

    @staticmethod
    def _derive_device_events(
        device_id: str,
        samples: list[_RawSample],
        *,
        alarm_threshold: float,
        recovery_threshold: float,
        max_sample_gap_seconds: int,
    ) -> list[_DerivedEvent]:
        """Run the deterministic hysteresis state machine for one device.

        Shared by event rebuilding (writes) and evidence queries (reads):
        both consume the same ordered raw facts and apply the same gap and
        hysteresis rules, so the evidence membership cannot drift from the
        persisted events. The peak keeps its first hit in
        (sampled_at, sequence) order -- a later equal value does not
        replace it.
        """
        max_gap = timedelta(seconds=max_sample_gap_seconds)
        events: list[_DerivedEvent] = []
        current: _DerivedEvent | None = None
        previous: _RawSample | None = None

        def close(status: str) -> None:
            nonlocal current
            assert current is not None
            current.status = status
            events.append(current)
            current = None

        for sample in samples:
            if (
                current is not None
                and previous is not None
                and sample.instant - previous.instant > max_gap
            ):
                # The observation gap is too wide to claim continuity, so
                # the open alarm tentatively ends at the last observed
                # sample. No recovery sample is invented; the sample that
                # exposed the gap is remembered as the interruptor and is
                # itself re-judged below (it may open a new event). If
                # late records later fill the gap, recomputation merges
                # the segments back into one event.
                current.end_sequence = previous.sequence
                current.end_at = previous.sampled_at
                current.interruptor = sample
                close("interrupted")

            if current is None:
                if sample.turbidity >= alarm_threshold:
                    current = _DerivedEvent(
                        start_sequence=sample.sequence,
                        start_at=sample.sampled_at,
                        members=[sample],
                        peak_turbidity=sample.turbidity,
                        peak_sequence=sample.sequence,
                        status="active",
                    )
            elif sample.turbidity <= recovery_threshold:
                # The recovery sample belongs to the event and supplies
                # end_at. Thus it remains a stable separator that late
                # high values cannot bridge over; a late low sample can
                # instead expose a separator and split/merge derived
                # ranges after recomputation.
                current.members.append(sample)
                current.end_sequence = sample.sequence
                current.end_at = sample.sampled_at
                close("recovered")
            else:
                current.members.append(sample)
                if sample.turbidity > current.peak_turbidity:
                    current.peak_turbidity = sample.turbidity
                    current.peak_sequence = sample.sequence

            previous = sample

        if current is not None:
            close("active")

        return events

    def _invalidate_acknowledgements(
        self, before: dict[str, EventOut], after_events: list[EventOut]
    ) -> None:
        """Drop acknowledgements invalidated by this rebuild.

        Runs inside the recomputation transaction: an acknowledgement is
        removed only when its event_id is absent from the recomputed set
        (the event disappeared) or when any of the event's fields differs
        (its version changed). Events that survived field-by-field keep
        their acknowledgement, and a removed row is not recreated when the
        same event_id shows up again in a later rebuild.
        """
        after = {event.event_id: event for event in after_events}
        invalid = [
            event_id
            for event_id, old_event in before.items()
            if after.get(event_id) != old_event
        ]
        if invalid:
            self._connection.executemany(
                "DELETE FROM event_acks WHERE event_id = ?",
                [(event_id,) for event_id in invalid],
            )

    def _insert_event(
        self, device_id: str, event: _DerivedEvent
    ) -> None:
        event_id = _DerivedEvent.event_id(
            device_id, event.start_sequence
        )
        self._connection.execute(
            """
            INSERT INTO events (
                event_id, device_id, status, start_sequence, end_sequence,
                start_at, end_at, peak_turbidity, peak_sequence,
                sample_count
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                device_id,
                event.status,
                event.start_sequence,
                event.end_sequence,
                event.start_at,
                event.end_at,
                event.peak_turbidity,
                event.peak_sequence,
                event.sample_count,
            ),
        )

    def _events_for_device(self, device_id: str) -> DeviceEvents:
        rows = self._connection.execute(
            """
            SELECT * FROM events
            WHERE device_id = ?
            ORDER BY start_sequence, start_at, event_id
            """,
            (device_id,),
        ).fetchall()
        return DeviceEvents(
            device_id=device_id,
            events=[self._row_to_event(row) for row in rows],
        )

    def _rebuild_device_with_changes(
        self, device_id: str
    ) -> tuple[DeviceEvents, list[tuple[str, str, EventOut | None]]]:
        """Rebuild one device and derive its change-stream entries.

        Must run inside a write transaction. The device's event set is
        snapshotted before the deterministic rebuild and compared with
        the recomputed set by event_id:

        - event present after but missing before, or with any field
          changed: an ``upsert`` carrying the complete new event;
        - event present before but missing after: a ``delete`` carrying
          only the event_id.

        Returned changes are ordered by event_id within the device; the
        caller sorts across devices by (device_id, event_id) before
        appending, so change_id assignment follows that order.
        """
        before, result = self._rebuild_device(device_id)
        after = {event.event_id: event for event in result.events}

        changes: list[tuple[str, str, EventOut | None]] = []
        for event_id in sorted(after):
            new_event = after[event_id]
            old_event = before.get(event_id)
            if old_event != new_event:
                # Newly created or changed; pydantic compares every field,
                # so this also covers new events (old_event is None).
                changes.append((device_id, event_id, new_event))
        for event_id in sorted(before):
            if event_id not in after:
                changes.append((device_id, event_id, None))
        changes.sort(key=lambda change: change[1])
        return result, changes

    def _append_event_changes(
        self,
        pending: list[tuple[str, str, EventOut | None]],
    ) -> None:
        """Append device event changes ordered by (device_id, event_id).

        change_id is omitted so SQLite assigns consecutive global
        autoincrement rowids within this transaction; hence the order of
        insertion fixes the order of change_id values seen by consumers.
        """
        pending = sorted(pending, key=lambda change: (change[0], change[1]))
        for device_id, event_id, event in pending:
            if event is None:
                change_type = "delete"
                event_json: str | None = None
            else:
                change_type = "upsert"
                event_json = event.model_dump_json()
            self._connection.execute(
                """
                INSERT INTO event_changes (
                    device_id, event_id, change_type, event_json
                ) VALUES (?, ?, ?, ?)
                """,
                (device_id, event_id, change_type, event_json),
            )

    def _all_device_ids(self) -> list[str]:
        rows = self._connection.execute(
            "SELECT DISTINCT device_id FROM samples ORDER BY device_id"
        ).fetchall()
        return [str(row["device_id"]) for row in rows]

    @staticmethod
    def _timestamp_text(value: datetime) -> str:
        # Python emits +00:00; normalize to Z for stable persisted text.
        return value.astimezone(timezone.utc).isoformat().replace(
            "+00:00", "Z"
        )

    @staticmethod
    def _same_observation(left: SampleIn, right: SampleIn) -> bool:
        return (
            left.sampled_at.astimezone(timezone.utc)
            == right.sampled_at.astimezone(timezone.utc)
            and float(left.turbidity) == float(right.turbidity)
        )

    @staticmethod
    def _matches_existing(record: SampleIn, row: sqlite3.Row) -> bool:
        return WaterQualityService._stored_row_matches(
            row, record.sampled_at, record.turbidity
        )

    @staticmethod
    def _stored_row_matches(
        row: sqlite3.Row, sampled_at: datetime, turbidity: float
    ) -> bool:
        # The stored text and the incoming timestamp are compared as UTC
        # instants, not as strings; turbidity compares as a float.
        existing_at = datetime.fromisoformat(
            str(row["sampled_at"]).replace("Z", "+00:00")
        ).astimezone(timezone.utc)
        return (
            existing_at == sampled_at.astimezone(timezone.utc)
            and float(row["turbidity"]) == float(turbidity)
        )

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> EventOut:
        end_at = row["end_at"]
        return EventOut(
            event_id=str(row["event_id"]),
            device_id=str(row["device_id"]),
            status=str(row["status"]),
            start_sequence=int(row["start_sequence"]),
            end_sequence=None if row["end_sequence"] is None
            else int(row["end_sequence"]),
            start_at=WaterQualityService._parse_timestamp(str(row["start_at"])),
            end_at=None if end_at is None
            else WaterQualityService._parse_timestamp(str(end_at)),
            peak_turbidity=float(row["peak_turbidity"]),
            peak_sequence=int(row["peak_sequence"]),
            sample_count=int(row["sample_count"]),
        )

    @staticmethod
    def _row_to_ack_event(row: sqlite3.Row) -> EventWithAckOut:
        event = WaterQualityService._row_to_event(row)
        return EventWithAckOut(
            **event.model_dump(),
            acknowledged=bool(row["acknowledged"]),
        )

    @staticmethod
    def _row_to_change(row: sqlite3.Row) -> EventChangeOut:
        change_type = str(row["change_type"])
        event: EventOut | None = None
        if change_type == "upsert":
            payload = json.loads(str(row["event_json"]))
            event = EventOut.model_validate(payload)
        return EventChangeOut(
            change_id=int(row["change_id"]),
            device_id=str(row["device_id"]),
            event_id=str(row["event_id"]),
            change_type=change_type,  # type: ignore[arg-type]
            event=event,
        )

    @staticmethod
    def _parse_timestamp(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
