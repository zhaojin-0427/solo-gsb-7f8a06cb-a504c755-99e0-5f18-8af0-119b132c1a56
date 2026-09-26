from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

SCHEMA_VERSION = 5
# The events table definition has had this shape since schema version 3.
# Version 4 added the append-only event_changes table and version 5 adds the
# event_acks table, both created with IF NOT EXISTS below; neither step needs
# to rebuild the deterministic derived events, and keeping the events table in
# place across v4 -> v5 is what lets acknowledgement invalidation compare the
# events before and after recomputation instead of treating every event as
# freshly created.
EVENTS_SHAPE_VERSION = 3


def connect(database_path: str | Path) -> sqlite3.Connection:
    path = Path(database_path)
    if path.parent != Path("."):
        path.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(
        str(path),
        check_same_thread=False,
        isolation_level=None,
        timeout=30.0,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute("PRAGMA busy_timeout=30000")
    return connection


def initialize(connection: sqlite3.Connection) -> bool:
    """Initialize derived state (events), the change stream and acks.

    Events are a pure function of the samples table, so a schema upgrade
    from an older events-table shape recreates the events table and lets
    the service rebuild it from the raw samples instead of migrating
    derived rows. The stored schema_version only advances once that
    rebuild commits (see WaterQualityService.reconcile_settings), so
    interrupting the upgrade at any point replays the whole migration on
    the next start.

    event_changes is an append-only log of recomputation results, not a
    cache that can be rebuilt from samples: consumers may still be
    trailing any water mark, so it is created with IF NOT EXISTS and is
    never dropped or rebuilt, even across schema upgrades.

    event_acks holds manual acknowledgements keyed by event_id and is
    likewise user state that cannot be rebuilt from samples. It is
    created with IF NOT EXISTS and only ever loses individual rows as
    part of a device's recomputation transaction when the acknowledged
    event changes or disappears (see
    WaterQualityService._invalidate_acknowledgements).
    """
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS schema_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
        """
    )

    migrated = False
    stored_version = get_meta(connection, "schema_version")
    if (
        stored_version is not None
        and stored_version != str(SCHEMA_VERSION)
        and int(stored_version) < EVENTS_SHAPE_VERSION
    ):
        # Only an older events-table shape forces dropping the derived
        # events. Upgrades from v3/v4 keep both events and acknowledgements
        # in place: their recomputation (when settings changed at startup)
        # then sees the old event set and invalidates only acknowledgements
        # of events whose fields actually changed or that disappeared.
        connection.execute("DROP TABLE IF EXISTS events")
        migrated = True

    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS samples (
            device_id TEXT NOT NULL,
            sequence INTEGER NOT NULL,
            sampled_at TEXT NOT NULL,
            turbidity REAL NOT NULL CHECK (turbidity >= 0.0),
            PRIMARY KEY (device_id, sequence)
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_samples_device_time
            ON samples (device_id, sampled_at, sequence);

        CREATE TABLE IF NOT EXISTS events (
            event_id TEXT NOT NULL PRIMARY KEY,
            device_id TEXT NOT NULL,
            status TEXT NOT NULL
                CHECK (status IN ('active', 'interrupted', 'recovered')),
            start_sequence INTEGER NOT NULL,
            end_sequence INTEGER,
            start_at TEXT NOT NULL,
            end_at TEXT,
            peak_turbidity REAL NOT NULL,
            peak_sequence INTEGER NOT NULL,
            sample_count INTEGER NOT NULL CHECK (sample_count > 0),
            UNIQUE (device_id, start_sequence),
            -- Sequence numbers only identify samples for dedup and event
            -- identity; late arrivals can carry any sequence, so event
            -- start/end/peak ordering is defined by sampled_at alone and
            -- no CHECK may compare sequence magnitudes here.
            CHECK (end_at IS NULL OR end_at >= start_at),
            CHECK ((end_sequence IS NULL) = (end_at IS NULL)),
            CHECK ((status = 'active') = (end_at IS NULL))
        ) WITHOUT ROWID;

        CREATE INDEX IF NOT EXISTS idx_events_device_start
            ON events (device_id, start_sequence);

        -- Append-only incremental stream of derived-event replicas.
        -- Rows are inserted within the very transaction that replaces a
        -- device's events, so the log commits or rolls back together with
        -- them. change_id is a global, gap-free increasing cursor: a write
        -- transaction only appends after the serialized rebuilds, so no
        -- committed change id is ever missing.
        CREATE TABLE IF NOT EXISTS event_changes (
            change_id INTEGER PRIMARY KEY,
            device_id TEXT NOT NULL,
            event_id TEXT NOT NULL,
            change_type TEXT NOT NULL CHECK (change_type IN ('upsert', 'delete')),
            event_json TEXT,
            CHECK (
                (change_type = 'upsert' AND event_json IS NOT NULL)
                OR (change_type = 'delete'
                    AND event_json IS NULL)
            )
        );

        -- Manual operator acknowledgements, keyed by event_id alone. Like
        -- event_changes this is not a cache rebuildable from samples: rows
        -- are created only through the acknowledgement endpoint. Rows are
        -- deleted inside the very transaction that recomputes the device's
        -- events, and only when the acknowledged event disappeared or any
        -- of its fields changed, so an acknowledgement can never silently
        -- keep covering a different version of an event. Once invalidated
        -- the event_id stays unacknowledged even if the same id later
        -- reappears.
        CREATE TABLE IF NOT EXISTS event_acks (
            event_id TEXT NOT NULL PRIMARY KEY
        ) WITHOUT ROWID;
        """
    )

    if not migrated:
        # A fresh or already-current database has nothing to rebuild, so
        # the version marker may be written right away. An upgrade leaves
        # the old version stored until the derived events were recomputed;
        # otherwise a crash in between would look like a finished upgrade.
        set_meta(connection, "schema_version", str(SCHEMA_VERSION))
    return migrated


def get_meta(connection: sqlite3.Connection, key: str) -> str | None:
    row = connection.execute(
        "SELECT value FROM schema_meta WHERE key = ?", (key,)
    ).fetchone()
    return None if row is None else str(row["value"])


def set_meta(connection: sqlite3.Connection, key: str, value: Any) -> None:
    connection.execute(
        """
        INSERT INTO schema_meta(key, value) VALUES (?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """,
        (key, str(value)),
    )


@contextmanager
def write_transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()


@contextmanager
def read_transaction(connection: sqlite3.Connection) -> Iterator[sqlite3.Connection]:
    """One consistent read-only snapshot.

    A deferred transaction under WAL pins a snapshot at the first read, so
    every SELECT inside it sees the same committed state even if another
    thread commits in between. Nothing is ever written; a raised exception
    (including a 404 control-flow error) simply ends the read transaction.
    """
    connection.execute("BEGIN")
    try:
        yield connection
    except Exception:
        connection.rollback()
        raise
    else:
        connection.commit()
