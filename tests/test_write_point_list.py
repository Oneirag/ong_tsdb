"""Regression tests for server.write_point_list and the /influx endpoint.

The /influx and /influx_binary endpoints both go through write_point_list
(src/ong_tsdb/server.py). In commit 1eef9a7 ("refactor: make OngTSDB
initialization lazy in server to avoid import-time side effects") a
mass find/replace renamed the private `_db` symbol to `_get_db()` and
accidentally corrupted the local variable on line 277, turning
`metrics_db.index(m)` into a call to a nonexistent `metrics_get_db()`.
The endpoints then raise NameError on every row.

These tests exercise that path directly so the regression cannot reappear
silently. They are hermetic: an in-memory OngTSDB rooted in tmp_path is
bound to the server module's `_db` global, and Flask's test_client is
used for the HTTP-layer case (no live server, no network).
"""

import base64
import time

import pytest

from ong_tsdb import DTYPE
from ong_tsdb.database import OngTSDB
from ong_tsdb.server import app, write_point_list
from ong_tsdb.server_utils import split_influx

WRITE = "write"
READ = "read"


@pytest.fixture
def server_db(tmp_path, monkeypatch):
    """An OngTSDB rooted in tmp_path, bound to server._db so write_point_list
    and the Flask routes resolve to the same instance."""
    d = OngTSDB(path=str(tmp_path))
    with open(d.FU.path_config()) as f:
        admin = f.readline().strip()
    d.create_db(admin, "test")
    d.create_sensor(admin, "test", "s1", "1s", WRITE, READ, ["active", "reactive"])
    d._admin_key = admin
    monkeypatch.setattr("ong_tsdb.server._db", d)
    return d


def _ns(t: float) -> int:
    return int(t * 1e9)


def _basic_auth_header(token: str) -> dict:
    """Flask uses HTTP Basic auth; username is ignored, the token is the password."""
    raw = f"x:{token}".encode()
    return {"Authorization": "Basic " + base64.b64encode(raw).decode()}


# -----------------------------------------------------------------------
# Test 1: direct call to write_point_list with known metrics.
# This is the canonical repro of the bug.
# -----------------------------------------------------------------------


def test_write_point_list_does_not_raise_nameerror(server_db):
    """Regression for 1eef9a7: writing rows with already-known metrics
    must not NameError on metrics_get_db()."""
    db = server_db
    now = time.time()
    line1 = split_influx(f"test,sensor=s1 active=1.0,reactive=2.0 {_ns(now)}")
    line2 = split_influx(f"test,sensor=s1 active=3.0,reactive=4.0 {_ns(now + 1)}")

    write_point_list(WRITE, [line1, line2])

    dates, values = db.read(WRITE, "test", "s1", start_ts=0, end_ts=now + 10)
    assert dates is not None
    assert values.shape[0] >= 1


# -----------------------------------------------------------------------
# Test 2: a row that adds a new metric. After add_new_metrics, metrics_db
# is re-read and .index() is called on it for every row in the chunk.
# -----------------------------------------------------------------------


def test_write_point_list_with_new_metric(server_db):
    """Regression for 1eef9a7: a row that introduces a new metric must
    also work."""
    db = server_db
    now = time.time()
    line = split_influx(f"test,sensor=s1 active=1.0,new_metric=42.0 {_ns(now)}")

    write_point_list(WRITE, [line])

    metrics = db.get_metrics(WRITE, "test", "s1")
    assert "new_metric" in metrics


# -----------------------------------------------------------------------
# Test 3: full HTTP path through Flask's test client. This is the exact
# path that produced the NameError in the user's traceback (POST /influx).
# -----------------------------------------------------------------------


def test_post_influx_endpoint_does_not_raise(server_db):
    """Regression for 1eef9a7: a real POST /influx through Flask's
    test client must succeed."""
    db = server_db
    now = time.time()
    body = "\n".join(
        [
            f"test,sensor=s1 active=1.0,reactive=2.0 {_ns(now)}",
            f"test,sensor=s1 active=3.0,reactive=4.0 {_ns(now + 1)}",
        ]
    ).encode()

    app.config["TESTING"] = True
    try:
        client = app.test_client()
        resp = client.post("/influx", data=body, headers=_basic_auth_header(WRITE))
        assert resp.status_code == 200, resp.get_data(as_text=True)
        assert resp.get_json()["ok"] is True
    finally:
        app.config["TESTING"] = False

    dates, values = db.read(WRITE, "test", "s1", start_ts=0, end_ts=now + 10)
    assert values.shape[0] >= 1
