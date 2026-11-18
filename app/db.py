"""SQLite persistence layer (WAL, safe for two processes on one volume)."""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Iterator

from . import config

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=30000;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS jobs (
    job_id          TEXT PRIMARY KEY,
    lane            TEXT NOT NULL UNIQUE,
    status          TEXT NOT NULL CHECK (status IN
                        ('queued','processing','published','failed')),
    input_hash      TEXT NOT NULL,
    barcode_table   TEXT NOT NULL,
    r1_incoming     TEXT NOT NULL,
    r2_incoming     TEXT NOT NULL,
    error           TEXT,
    instance_id     TEXT,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    heartbeat_at    TEXT
);

CREATE TABLE IF NOT EXISTS job_samples (
    job_id      TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    sample_id   TEXT NOT NULL,
    barcode_r1  TEXT NOT NULL,
    barcode_r2  TEXT NOT NULL,
    PRIMARY KEY (job_id, sample_id)
);

CREATE TABLE IF NOT EXISTS lane_publishes (
    lane            TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,
    published_at    TEXT NOT NULL,
    seq             INTEGER NOT NULL UNIQUE
);

-- Withdrawal never deletes a publish/count row: the original paired files,
-- counts and publish record stay in place for traceability. This table only
-- marks the lane as withdrawn, once, with the operator-supplied reason.
CREATE TABLE IF NOT EXISTS lane_withdrawals (
    lane          TEXT PRIMARY KEY REFERENCES lane_publishes(lane),
    reason        TEXT NOT NULL,
    withdrawn_at  TEXT NOT NULL,
    instance_id   TEXT
);

CREATE TABLE IF NOT EXISTS lane_counts (
    lane        TEXT NOT NULL REFERENCES lane_publishes(lane),
    sample_id   TEXT NOT NULL,           -- '' denotes the quarantine bucket
    r1_file     TEXT NOT NULL,
    r2_file     TEXT NOT NULL,
    read_pairs  INTEGER NOT NULL,
    PRIMARY KEY (lane, sample_id)
);

CREATE TABLE IF NOT EXISTS snapshots (
    snapshot_id   TEXT PRIMARY KEY,
    created_at    TEXT NOT NULL,
    lanes_json    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS snapshot_lanes (
    snapshot_id TEXT NOT NULL REFERENCES snapshots(snapshot_id) ON DELETE CASCADE,
    lane        TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    PRIMARY KEY (snapshot_id, lane)
);
"""

_local = threading.local()


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def utcnow() -> str:
    return _now()


def get_conn() -> sqlite3.Connection:
    """One connection per worker thread, configured for cross-process use."""
    conn = getattr(_local, "conn", None)
    if conn is None:
        conn = sqlite3.connect(
            config.DB_PATH, timeout=30, isolation_level=None, check_same_thread=False
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA foreign_keys=ON")
        _local.conn = conn
    return conn


@contextmanager
def tx() -> Iterator[sqlite3.Connection]:
    conn = get_conn()
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def init_db() -> None:
    conn = get_conn()
    conn.executescript(_SCHEMA)


# ---------- job helpers ----------

def create_job(row: dict[str, Any], samples: list[dict[str, str]]) -> None:
    with tx() as conn:
        conn.execute(
            """INSERT INTO jobs (job_id, lane, status, input_hash, barcode_table,
                                  r1_incoming, r2_incoming, error, instance_id,
                                  created_at, updated_at, heartbeat_at)
               VALUES (:job_id,:lane,'queued',:input_hash,:barcode_table,
                       :r1_incoming,:r2_incoming,NULL,NULL,
                       :created_at,:created_at,NULL)""",
            row,
        )
        conn.executemany(
            """INSERT INTO job_samples (job_id, sample_id, barcode_r1, barcode_r2)
               VALUES (:job_id,:sample_id,:barcode_r1,:barcode_r2)""",
            [
                {
                    "job_id": row["job_id"],
                    "sample_id": s["sample_id"],
                    "barcode_r1": s["barcode_r1"],
                    "barcode_r2": s["barcode_r2"],
                }
                for s in samples
            ],
        )


def get_job_by_lane(lane: str) -> sqlite3.Row | None:
    return get_conn().execute("SELECT * FROM jobs WHERE lane=?", (lane,)).fetchone()


def get_job(job_id: str) -> sqlite3.Row | None:
    return get_conn().execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()


def list_jobs() -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM jobs ORDER BY created_at").fetchall()


def get_samples(job_id: str) -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM job_samples WHERE job_id=? ORDER BY rowid", (job_id,)
    ).fetchall()


def claim_next_job(instance_id: str) -> sqlite3.Row | None:
    """Atomically take one queued job. Returns None when the queue is empty."""
    with tx() as conn:
        row = conn.execute(
            "SELECT * FROM jobs WHERE status='queued' ORDER BY created_at LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        conn.execute(
            """UPDATE jobs SET status='processing', instance_id=?,
                               updated_at=?, heartbeat_at=?
               WHERE job_id=?""",
            (instance_id, _now(), _now(), row["job_id"]),
        )
    return get_job(row["job_id"])


def heartbeat(job_id: str, instance_id: str) -> None:
    get_conn().execute(
        "UPDATE jobs SET heartbeat_at=? WHERE job_id=? AND instance_id=?",
        (_now(), job_id, instance_id),
    )


def mark_failed(job_id: str, instance_id: str, error: str) -> None:
    with tx() as conn:
        conn.execute(
            """UPDATE jobs SET status='failed', error=?, updated_at=?
               WHERE job_id=? AND instance_id=?""",
            (error[:2000], _now(), job_id, instance_id),
        )


def requeue_job(job_id: str) -> bool:
    with tx() as conn:
        row = conn.execute(
            "SELECT status FROM jobs WHERE job_id=?", (job_id,)
        ).fetchone()
        if row is None:
            return False
        if row["status"] != "failed":
            return False
        conn.execute(
            """UPDATE jobs SET status='queued', error=NULL, instance_id=NULL,
                               updated_at=?, heartbeat_at=NULL WHERE job_id=?""",
            (_now(), job_id),
        )
    return True


def publish_job(job_id: str, lane: str, counts: list[dict[str, Any]]) -> None:
    """Publish metadata atomically; files are moved in beforehand."""
    with tx() as conn:
        next_seq = conn.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS s FROM lane_publishes").fetchone()["s"]
        conn.execute(
            """INSERT INTO lane_publishes (lane, job_id, published_at, seq)
               VALUES (?,?,?,?)""",
            (lane, job_id, _now(), next_seq),
        )
        conn.executemany(
            """INSERT INTO lane_counts (lane, sample_id, r1_file, r2_file, read_pairs)
               VALUES (:lane,:sample_id,:r1_file,:r2_file,:read_pairs)""",
            [{**c, "lane": lane} for c in counts],
        )
        conn.execute(
            "UPDATE jobs SET status='published', updated_at=? WHERE job_id=?",
            (_now(), job_id),
        )


# ---------- published lanes / snapshots ----------

def published_lanes() -> list[sqlite3.Row]:
    """All publish rows, including withdrawn ones, in publish order.

    Withdrawal metadata is joined on so callers can filter or display the
    withdrawn state; the original publish/count rows are never removed.
    """
    return get_conn().execute(
        """SELECT p.*, w.reason AS withdrawal_reason,
                  w.withdrawn_at AS withdrawn_at
           FROM lane_publishes p
           LEFT JOIN lane_withdrawals w ON w.lane = p.lane
           ORDER BY p.seq""").fetchall()


def get_withdrawal(lane: str) -> sqlite3.Row | None:
    return get_conn().execute(
        "SELECT * FROM lane_withdrawals WHERE lane=?", (lane,)).fetchone()


def withdraw_lane(lane: str, reason: str, instance_id: str | None) -> dict[str, Any] | None:
    """Mark a published lane withdrawn inside one immediate transaction.

    Returns the withdrawal record (with ``already_withdrawn`` set when the
    lane was already withdrawn, in which case the original reason is kept
    verbatim), or None when the lane was never published. Because both this
    write and snapshot creation take a write lock up front, a snapshot
    committed after the withdrawal can never include this lane, and a
    snapshot that committed first is frozen without it being retroactively
    changed.
    """
    now = _now()
    with tx() as conn:
        pub = conn.execute(
            "SELECT lane FROM lane_publishes WHERE lane=?", (lane,)).fetchone()
        if pub is None:
            return None
        existing = conn.execute(
            "SELECT * FROM lane_withdrawals WHERE lane=?", (lane,)).fetchone()
        if existing is not None:
            return {
                "lane": lane,
                "reason": existing["reason"],
                "withdrawn_at": existing["withdrawn_at"],
                "instance_id": existing["instance_id"],
                "already_withdrawn": True,
            }
        conn.execute(
            """INSERT INTO lane_withdrawals (lane, reason, withdrawn_at, instance_id)
               VALUES (?,?,?,?)""",
            (lane, reason, now, instance_id),
        )
    return {
        "lane": lane,
        "reason": reason,
        "withdrawn_at": now,
        "instance_id": instance_id,
        "already_withdrawn": False,
    }


def lane_counts(lane: str) -> list[sqlite3.Row]:
    return get_conn().execute(
        "SELECT * FROM lane_counts WHERE lane=? ORDER BY sample_id", (lane,)
    ).fetchall()


def create_snapshot(snapshot_id: str) -> dict[str, Any]:
    """Freeze the currently available (published, non-withdrawn) lanes.

    Runs in a single ``BEGIN IMMEDIATE`` transaction, so the lane set is read
    while holding the write lock: a withdrawal committed before this
    transaction excludes its lane, and any withdrawal racing this call blocks
    until commit and therefore takes effect afterwards without changing this
    snapshot's fixed list.
    """
    now = _now()
    with tx() as conn:
        rows = conn.execute(
            """SELECT p.lane, p.seq FROM lane_publishes p
               LEFT JOIN lane_withdrawals w ON w.lane = p.lane
               WHERE w.lane IS NULL
               ORDER BY p.seq""").fetchall()
        fixed = [r["lane"] for r in rows]
        conn.execute(
            "INSERT INTO snapshots (snapshot_id, created_at, lanes_json) VALUES (?,?,?)",
            (snapshot_id, now, json.dumps(fixed)),
        )
        conn.executemany(
            "INSERT INTO snapshot_lanes (snapshot_id, lane, seq) VALUES (?,?,?)",
            [(snapshot_id, r["lane"], r["seq"]) for r in rows],
        )
    return _snapshot_view(snapshot_id, now, fixed)


def _withdrawn_lanes(conn: sqlite3.Connection, lanes: list[str]) -> list[sqlite3.Row]:
    if not lanes:
        return []
    return conn.execute(
        "SELECT lane, reason, withdrawn_at FROM lane_withdrawals "
        "WHERE lane IN (%s) ORDER BY withdrawn_at"
        % ",".join("?" * len(lanes)),
        lanes,
    ).fetchall()


def _snapshot_view(snapshot_id: str, created_at: str, lanes: list[str],
                   conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    if conn is None:
        conn = get_conn()
    withdrawn = [dict(r) for r in _withdrawn_lanes(conn, lanes)]
    view = {
        "snapshot_id": snapshot_id,
        "created_at": created_at,
        "lanes": lanes,
        "withdrawn_lanes": [w["lane"] for w in withdrawn],
        "withdrawals": withdrawn,
        "valid": not withdrawn,
    }
    return view


def get_snapshot(snapshot_id: str) -> dict[str, Any] | None:
    row = get_conn().execute(
        "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,)
    ).fetchone()
    if row is None:
        return None
    lanes = json.loads(row["lanes_json"])
    # The stored list is never rewritten: a snapshot keeps its original
    # manifest; withdrawal only flags the snapshot invalid.
    return _snapshot_view(
        snapshot_id, row["created_at"], lanes, conn=get_conn())


def list_snapshots() -> list[dict[str, Any]]:
    conn = get_conn()
    return [
        _snapshot_view(
            r["snapshot_id"], r["created_at"], json.loads(r["lanes_json"]),
            conn=conn)
        for r in conn.execute(
            "SELECT * FROM snapshots ORDER BY created_at").fetchall()
    ]
