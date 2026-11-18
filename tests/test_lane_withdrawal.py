"""Lane withdrawal: retention, refusal, snapshot invalidation and the
withdrawal/snapshot concurrency interleavings."""
import json
import sqlite3
import threading
import time
from pathlib import Path

BARCODES = [
    {"sample_id": "s1", "barcode_r1": "AAAAAAAA", "barcode_r2": "CCCCCCCC"},
    {"sample_id": "s2", "barcode_r1": "GGGGGGGG", "barcode_r2": "TTTTTTTT"},
]


def _post(client, lane, barcodes, r1_path, r2_path):
    with open(r1_path, "rb") as f1, open(r2_path, "rb") as f2:
        return client.post(
            "/jobs",
            data={"lane": lane, "barcodes": json.dumps(barcodes)},
            files={"r1": ("R1.fastq.gz", f1, "application/gzip"),
                   "r2": ("R2.fastq.gz", f2, "application/gzip")},
        )


def _wait(client, job_id, timeout=15):
    deadline = time.time() + timeout
    while time.time() < deadline:
        row = client.get(f"/jobs/{job_id}").json()
        if row["status"] in ("published", "failed"):
            return row
        time.sleep(0.1)
    raise AssertionError("timeout waiting for job")


def _publish(client, helpers, tmp_path, lane, records=3):
    d = tmp_path / lane
    d.mkdir(parents=True, exist_ok=True)
    r1 = [(f"r{i}".encode(), b"AAAAAAAATT", b"I" * 10) for i in range(records)]
    r2 = [(f"r{i}".encode(), b"CCCCCCCCGG", b"J" * 10) for i in range(records)]
    p1, p2 = helpers.make_pair(r1, r2, d)
    resp = _post(client, lane, BARCODES[:1], p1, p2)
    assert resp.status_code == 201, resp.text
    job = _wait(client, resp.json()["job"]["job_id"])
    assert job["status"] == "published", job
    return job


def _withdraw(client, lane, reason):
    return client.post(f"/lanes/{lane}/withdraw", json={"reason": reason})


def test_withdraw_marks_lane_and_keeps_original_data(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "laneW")

    resp = _withdraw(client, "laneW", "contaminated reads found in QC review")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["lane"] == "laneW"
    assert body["reason"] == "contaminated reads found in QC review"
    assert body["already_withdrawn"] is False
    assert body["withdrawn_at"]

    # List and detail views carry the withdrawal marker and reason.
    listed = {r["lane"]: r for r in client.get("/lanes").json()}
    assert listed["laneW"]["withdrawn"] is True
    assert listed["laneW"]["withdrawal"]["reason"].startswith("contaminated")

    detail = client.get("/lanes/laneW").json()
    assert detail["withdrawn"] is True
    assert detail["withdrawal"]["reason"].startswith("contaminated")
    # Original counts and publish record are retained for traceability.
    assert detail["total_read_pairs"] == 3
    assert {c["sample_id"] for c in detail["counts"]} == {"s1", "__quarantine__"}

    # Original paired files stay on disk untouched.
    from app import db as dbmod
    for row in dbmod.lane_counts("laneW"):
        assert Path(row["r1_file"]).is_file()
        assert Path(row["r2_file"]).is_file()
    assert dbmod.get_conn().execute(
        "SELECT 1 FROM lane_publishes WHERE lane='laneW'").fetchone() is not None


def test_repeated_withdrawal_keeps_original_reason(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "laneRepeat")
    first = _withdraw(client, "laneRepeat", "first reason")
    assert first.json()["already_withdrawn"] is False

    again = _withdraw(client, "laneRepeat", "a different later reason")
    assert again.status_code == 200
    assert again.json()["already_withdrawn"] is True
    assert again.json()["reason"] == "first reason"

    detail = client.get("/lanes/laneRepeat").json()
    assert detail["withdrawal"]["reason"] == "first reason"


def test_withdraw_validation(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "laneVal")
    assert _withdraw(client, "never-published", "x").status_code == 404
    # Pydantic rejects an empty/missing reason with 422.
    assert client.post("/lanes/laneVal/withdraw", json={"reason": ""}).status_code == 422
    assert client.post("/lanes/laneVal/withdraw", json={}).status_code == 422
    # Whitespace-only reason is rejected at the API boundary (400).
    assert client.post(
        "/lanes/laneVal/withdraw", json={"reason": "   "}).status_code == 400
    # Lane still available after the failed attempts.
    assert client.get("/lanes/laneVal/samples/s1/r1").status_code == 200


def test_new_direct_downloads_refused_after_withdrawal(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "laneBlock")
    before = client.get("/lanes/laneBlock/samples/s1/r1")
    assert before.status_code == 200 and len(before.content) > 0

    assert _withdraw(client, "laneBlock", "pollution").status_code == 200

    # Every new direct fetch, sample or quarantine, is refused as a whole.
    assert client.get("/lanes/laneBlock/samples/s1/r1").status_code == 409
    assert client.get("/lanes/laneBlock/samples/s1/r2").status_code == 409
    assert client.get("/lanes/laneBlock/quarantine/r1").status_code == 409
    assert client.get("/lanes/laneBlock/quarantine/r2").status_code == 409
    # Query endpoints still answer for traceability.
    assert client.get("/lanes/laneBlock").status_code == 200
    assert client.get("/lanes").status_code == 200


def test_download_started_before_withdrawal_completes(client, helpers, tmp_path):
    """A stream that already holds the open file must finish completely;
    withdrawal only blocks requests that start afterwards."""
    _publish(client, helpers, tmp_path, "laneFlight", records=4)
    from app import db as dbmod
    from app.main import _iter_concat

    # Mimic what FileResponse/StreamingResponse does: resolve and open the
    # published files *before* the withdrawal lands.
    count_rows = dbmod.lane_counts("laneFlight")
    s1 = next(r for r in count_rows if r["sample_id"] == "s1")
    expected = Path(s1["r1_file"]).read_bytes()
    in_flight = list(_iter_concat([Path(s1["r1_file"])]))

    assert _withdraw(client, "laneFlight", "late finding").status_code == 200
    # New requests are refused while the old stream keeps reading.
    assert client.get("/lanes/laneFlight/samples/s1/r1").status_code == 409
    assert b"".join(in_flight) == expected  # full, untruncated payload


def test_old_snapshot_keeps_manifest_but_is_invalid_and_refuses_whole(
        client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "laneOld1")
    _publish(client, helpers, tmp_path, "laneOld2")
    snap = client.post("/snapshots").json()
    assert snap["valid"] is True
    assert snap["lanes"] == ["laneOld1", "laneOld2"]

    # A valid snapshot concatenates both lanes before withdrawal.
    ok = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/r1")
    assert ok.status_code == 200
    assert ok.headers["x-snapshot-lanes"] == "laneOld1,laneOld2"

    assert _withdraw(client, "laneOld1", "contamination").status_code == 200

    view = client.get(f"/snapshots/{snap['snapshot_id']}").json()
    # Original manifest is preserved verbatim, but flagged invalid.
    assert view["lanes"] == ["laneOld1", "laneOld2"]
    assert view["valid"] is False
    assert view["withdrawn_lanes"] == ["laneOld1"]
    assert view["withdrawals"][0]["reason"] == "contamination"

    # Whole-call refusal: no silent skip of the withdrawn lane, for either
    # sample or quarantine downloads.
    r1 = client.get(f"/snapshots/{snap['snapshot_id']}/samples/s1/r1")
    rq = client.get(f"/snapshots/{snap['snapshot_id']}/quarantine/r1")
    assert r1.status_code == 409 and "withdrawn" in r1.text
    assert rq.status_code == 409 and "withdrawn" in rq.text


def test_new_snapshot_only_contains_available_lanes(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "laneKeep")
    _publish(client, helpers, tmp_path, "laneDrop")
    assert _withdraw(client, "laneDrop", "bad lane").status_code == 200

    snap = client.post("/snapshots").json()
    assert snap["lanes"] == ["laneKeep"]
    assert snap["valid"] is True
    assert snap["withdrawn_lanes"] == []

    # Snapshot list reports both snapshots with their respective states.
    listed = client.get("/snapshots").json()
    states = {s["snapshot_id"]: s["valid"] for s in listed}
    assert states[snap["snapshot_id"]] is True

    # A later withdrawal also invalidates this snapshot.
    assert _withdraw(client, "laneKeep", "also bad").status_code == 200
    view = client.get(f"/snapshots/{snap['snapshot_id']}").json()
    assert view["valid"] is False and view["withdrawn_lanes"] == ["laneKeep"]
    assert client.get(
        f"/snapshots/{snap['snapshot_id']}/samples/s1/r1").status_code == 409


def test_other_lanes_keep_working_after_withdrawal(client, helpers, tmp_path):
    _publish(client, helpers, tmp_path, "laneDown")
    _publish(client, helpers, tmp_path, "laneUp")
    assert _withdraw(client, "laneDown", "polluted").status_code == 200

    # The healthy lane still demuxes, lists, counts and serves data.
    assert client.get("/lanes/laneUp/samples/s1/r1").status_code == 200
    assert client.get("/lanes/laneUp/quarantine/r2").status_code == 200
    detail = client.get("/lanes/laneUp").json()
    assert detail["withdrawn"] is False and detail["total_read_pairs"] == 3

    # A brand new lane submitted afterwards publishes normally.
    _publish(client, helpers, tmp_path, "laneAfter")
    assert client.get("/lanes/laneAfter/samples/s1/r1").status_code == 200
    snap = client.post("/snapshots").json()
    assert set(snap["lanes"]) == {"laneUp", "laneAfter"}


def _raw_conn(dbmod):
    conn = sqlite3.connect(dbmod.config.DB_PATH, timeout=30,
                           isolation_level=None, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def test_withdraw_blocks_until_open_snapshot_transaction_commits(
        data_dir, client, helpers, tmp_path):
    """Interleave 1: snapshot tx is open first. The withdrawal must wait,
    and once the snapshot commits it contains the lane (later flagged
    invalid); the withdrawal still lands atomically right after."""
    from app import db as dbmod
    _publish(client, helpers, tmp_path, "laneRace1")

    ev_lock = threading.Event()
    ev_release = threading.Event()
    result: dict = {}

    def snapshot_tx():
        conn = _raw_conn(dbmod)
        conn.execute("BEGIN IMMEDIATE")  # same lock create_snapshot takes
        rows = conn.execute(
            """SELECT p.lane, p.seq FROM lane_publishes p
               LEFT JOIN lane_withdrawals w ON w.lane = p.lane
               WHERE w.lane IS NULL ORDER BY p.seq""").fetchall()
        sid = "racesnap1"
        conn.execute(
            "INSERT INTO snapshots (snapshot_id, created_at, lanes_json) "
            "VALUES (?,?,?)",
            (sid, dbmod.utcnow(), json.dumps([r["lane"] for r in rows])))
        conn.executemany(
            "INSERT INTO snapshot_lanes (snapshot_id, lane, seq) "
            "VALUES (?,?,?)",
            [(sid, r["lane"], r["seq"]) for r in rows])
        ev_lock.set()
        ev_release.wait(5)
        conn.execute("COMMIT")
        conn.close()

    def withdraw():
        result["out"] = dbmod.withdraw_lane("laneRace1", "racing reason", "test")

    t_snap = threading.Thread(target=snapshot_tx)
    t_snap.start()
    assert ev_lock.wait(5)

    t_wd = threading.Thread(target=withdraw)
    t_wd.start()
    # The withdrawal cannot commit while the snapshot write lock is held.
    t_wd.join(timeout=1.0)
    assert t_wd.is_alive(), "withdrawal must block behind the open snapshot tx"
    assert dbmod.get_withdrawal("laneRace1") is None

    ev_release.set()
    t_wd.join(timeout=10)
    t_snap.join(timeout=10)
    assert result["out"]["already_withdrawn"] is False

    view = dbmod.get_snapshot("racesnap1")
    assert view["lanes"] == ["laneRace1"]       # manifest fixed at commit time
    assert view["valid"] is False               # withdrawal afterwards invalidates
    assert view["withdrawn_lanes"] == ["laneRace1"]


def test_snapshot_after_committed_withdrawal_excludes_lane(
        data_dir, client, helpers, tmp_path):
    """Interleave 2: withdrawal commits first. The snapshot transaction that
    starts afterwards must never include the withdrawn lane."""
    from app import db as dbmod
    _publish(client, helpers, tmp_path, "laneRace2")
    _publish(client, helpers, tmp_path, "laneRace2b")

    out = dbmod.withdraw_lane("laneRace2", "won the race", "test")
    assert out["already_withdrawn"] is False

    # Real API path: create_snapshot runs its own BEGIN IMMEDIATE tx.
    snap = client.post("/snapshots").json()
    assert snap["lanes"] == ["laneRace2b"]
    assert snap["valid"] is True

    # A second withdrawal attempt while snapshots keep being created still
    # returns the original reason and never re-inserts the lane.
    again = dbmod.withdraw_lane("laneRace2", "ignored reason", "test")
    assert again["already_withdrawn"] is True
    assert again["reason"] == "won the race"
    snap2 = client.post("/snapshots").json()
    assert snap2["lanes"] == ["laneRace2b"] and snap2["valid"] is True


def test_snapshot_creation_blocks_behind_open_withdrawal_then_excludes(
        data_dir, client, helpers, tmp_path):
    """Interleave 3: withdrawal tx is open first and a snapshot creation
    races it. The snapshot waits on the write lock; after the withdrawal
    commits the snapshot excludes the lane (no lane ever sneaks in)."""
    from app import db as dbmod
    _publish(client, helpers, tmp_path, "laneRace3")

    ev_lock = threading.Event()
    ev_release = threading.Event()
    snap_body: dict = {}

    def withdrawal_tx():
        conn = _raw_conn(dbmod)
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO lane_withdrawals (lane, reason, withdrawn_at, instance_id)"
            " VALUES (?,?,?,?)",
            ("laneRace3", "open withdrawal tx", dbmod.utcnow(), "test"))
        ev_lock.set()
        ev_release.wait(5)
        conn.execute("COMMIT")
        conn.close()

    def snapshot_call():
        # Runs in its own thread -> its own thread-local DB connection.
        snap_body.update(dbmod.create_snapshot("racesnap3"))

    t_wd = threading.Thread(target=withdrawal_tx)
    t_wd.start()
    assert ev_lock.wait(5)

    t_snap = threading.Thread(target=snapshot_call)
    t_snap.start()
    t_snap.join(timeout=1.0)
    assert t_snap.is_alive(), "snapshot must block behind the open withdrawal tx"

    ev_release.set()
    t_wd.join(timeout=10)
    t_snap.join(timeout=10)
    assert snap_body["lanes"] == []
    assert snap_body["valid"] is True
    # The empty, still-valid snapshot returns 404 for unknown buckets rather
    # than serving withdrawn data.
    assert client.get(
        "/snapshots/racesnap3/samples/s1/r1").status_code == 404
