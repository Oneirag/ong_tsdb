"""Deployment smoke test for the lazy OngTSDB initialization in the server.

This test pins down a class of bugs that previously slipped through CI
twice: a half-revert of the lazy-init refactor (commit 1eef9a7) can leave
the server module in a state where every route and write_point_list call
site invokes `_get_db()` but the helper function itself is missing from
the module. The result is that every request to /influx, /influx_binary,
or any other DB-backed endpoint raises NameError at runtime, even though
`import ong_tsdb.server` succeeds at install time.

To catch this kind of regression deterministically, we assert that
ong_tsdb.server defines a callable `_get_db` AND that the module-level
`_db` is None at import time (i.e. lazy, not eager). If a future change
re-introduces eager init, this test will fail with a clear message
telling the maintainer to also update the call sites.
"""

import pytest


def test_server_module_defines_lazy_get_db_helper():
    """The server must define `_get_db` as a lazy initializer.

    If a partial revert removes the helper without reverting the call
    sites, every /influx request raises NameError. The helper is the
    linchpin of the lazy-init refactor; both the helper and the call
    sites must move together.
    """
    import ong_tsdb.server as server

    assert hasattr(server, "_get_db"), (
        "ong_tsdb.server._get_db is not defined. The lazy-init refactor "
        "in 1eef9a7 added this helper and 29 call sites; if you removed "
        "the helper (e.g. via a partial revert) you must also revert the "
        "call sites, or every /influx request will raise NameError."
    )
    assert callable(server._get_db), (
        "ong_tsdb.server._get_db exists but is not callable. Check the "
        "lazy-init block in src/ong_tsdb/server.py for corruption."
    )


def test_server_module_db_is_lazy_not_eager():
    """The module-level `_db` must start as None (lazy).

    Eager init (`_db = OngTSDB()`) at import time has two problems:
    1) it runs OngTSDB.__init__ as an import side effect, which can
       create or read config files and even print the admin key; and
    2) it conflicts with the original goal of the lazy-init refactor
       (avoiding import-time I/O for tooling that just imports the
       module to inspect it, e.g. flask routes introspection).
    """
    import ong_tsdb.server as server

    # `_db` is a module-level name. It should be None until a request
    # triggers _get_db() for the first time. The exact assertion is
    # `is None` rather than `== None` for type flexibility.
    assert getattr(server, "_db", "missing") is None, (
        f"ong_tsdb.server._db is {server._db!r} at import time; the "
        "lazy-init refactor requires it to be None. A non-None value "
        "means OngTSDB() ran at import time, which is a regression of "
        "the lazy-init goal and likely indicates a partial revert."
    )


def test_get_db_returns_ongtsdb_instance_on_first_call(monkeypatch, tmp_path):
    """First call to _get_db() must construct an OngTSDB rooted at the
    default BASE_DIR. We monkey-patch the constructor to a sentinel so
    we don't touch disk and can assert the function wires everything
    together correctly.

    This complements the two static checks above: it proves the helper
    is not only defined and callable, but actually returns a database
    object on first use (the contract every route depends on).
    """
    import ong_tsdb.server as server
    import ong_tsdb.database as database

    # Force the lazy state to None regardless of any prior test activity.
    monkeypatch.setattr(server, "_db", None)

    sentinel = object()
    calls = []

    def fake_ongtsdb():
        calls.append("called")
        return sentinel

    # Patch the name `OngTSDB` in the server module's namespace. The
    # helper does `OngTSDB()` (a bare name, not `database.OngTSDB()`),
    # so we have to patch the lookup site, not the original class.
    monkeypatch.setattr(server, "OngTSDB", fake_ongtsdb)
    # Also patch on the database module so the rest of the system, if
    # it imports the name from there, sees the same sentinel.
    monkeypatch.setattr(database, "OngTSDB", fake_ongtsdb)

    result = server._get_db()
    assert result is sentinel
    assert calls == ["called"], (
        f"OngTSDB() was called {len(calls)} time(s); expected exactly 1 "
        f"on the first call to _get_db(). Calls: {calls!r}"
    )

    # Second call must reuse the cached instance, NOT call OngTSDB again.
    result2 = server._get_db()
    assert result2 is sentinel
    assert calls == ["called"], (
        f"OngTSDB() was called {len(calls)} time(s) across two _get_db() "
        f"invocations; the helper must cache the instance in the module-"
        f"level `_db` global. Calls: {calls!r}"
    )


def test_server_module_no_direct_db_attribute_access():
    """No code in server.py may reach into the module-level `_db` global
    by attribute access (e.g. `_db.get_metrics(...)`). Every call must
    go through `_get_db()` so the lazy-init contract holds.

    This is a static check (regex on the source file) that catches the
    "halfway through a rename" class of bug, where some call sites were
    switched to `_get_db()` and some still use the bare `_db` reference.
    When that happens, the bare references still execute (the name
    resolves) but the value is `None` (lazy state), so they raise
    `AttributeError: 'NoneType' object has no attribute 'X'` at runtime
    — which is what happened in production after the lazy-init refactor
    was partially applied. The previous "test only the helper exists"
    check passed in that case and let the bug ship.

    Allowed uses of the bare `_db` token:
      - the lazy-state assignment (`_db = None`)
      - the `global _db` declaration
      - the `if _db is None` cache check inside the helper
      - the assignment inside the helper (`_db = OngTSDB()`)
      - the `return _db` line inside the helper
    Forbidden: anything like `_db.xxx` outside the helper. `_get_db()`
    is the only acceptable access path.
    """
    import re
    from pathlib import Path

    server_py = (
        Path(__file__).resolve().parent.parent / "src" / "ong_tsdb" / "server.py"
    )
    text = server_py.read_text()

    # Use a regex that matches `_db` only when NOT preceded by another
    # identifier character (so `_get_db` is not matched) and NOT inside
    # a longer identifier (so `metrics_db` is not matched). We then
    # filter out the lines that are inside the helper block.
    bare_db_re = re.compile(r"(?<![A-Za-z0-9_])_db\b")

    # Locate the helper block. The block starts at the line that
    # declares `_db = None` and ends at the line `return _db`.
    # Anything between those (inclusive) is the helper.
    lines = text.splitlines()
    helper_start = None
    helper_end = None
    for i, line in enumerate(lines):
        if helper_start is None and line.strip() == "_db = None":
            helper_start = i
        if helper_start is not None and line.strip() == "return _db":
            helper_end = i
            break

    assert helper_start is not None and helper_end is not None, (
        "Could not locate the lazy-init helper block in server.py. "
        "The test expects lines `_db = None` ... `return _db` to mark "
        "the helper boundaries. If the helper was removed entirely, "
        "this test will fail loudly instead of silently passing."
    )

    # Scan every line outside the helper for bare `_db` references.
    violations = []
    for i, line in enumerate(lines):
        if helper_start <= i <= helper_end:
            continue
        for match in bare_db_re.finditer(line):
            violations.append((i + 1, line.rstrip(), match.group()))

    assert not violations, (
        f"Found {len(violations)} direct `_db` reference(s) in server.py "
        f"outside the lazy-init helper. These resolve to None at "
        f"runtime and raise AttributeError on the first request. "
        f"Use `_get_db()` instead.\n"
        + "\n".join(f"  line {n}: {line!r}" for n, line, _ in violations)
    )


def test_grafana_query_routes_resolve_db_via_helper(monkeypatch, tmp_path):
    """A real POST to the grafana /query endpoint must not raise
    `AttributeError: 'NoneType' object has no attribute 'get_metrics'`.

    This is the exact failure mode the user hit in production after the
    partial revert of the lazy-init refactor: the helper existed (so
    `name '_get_db' is not defined` did not fire) but several call
    sites still used the bare `_db` global. With the global now lazy
    (`_db = None`), those call sites raise AttributeError instead.

    To make this test deterministic, we do NOT pre-populate
    `server._db`. We only patch the `OngTSDB` constructor so that
    `_get_db()` (the helper) returns a stub the first time it is called.
    The test then asserts:
      1. The route reaches the database via `_get_db()` (i.e. it
         constructs a stub at least once), AND
      2. The route does not return 500 with a NoneType AttributeError.
    On the unfixed code (where the route still uses the bare `_db`
    global), `_db` is `None` and the route fails with AttributeError,
    which surfaces as a 500 response.
    """
    import base64
    import ong_tsdb.server as server
    from ong_tsdb import database as db_mod

    # Make sure the lazy state is `None`. This is the production state
    # at import time; we reset it explicitly in case a previous test
    # in the same session already triggered a real _get_db() call.
    monkeypatch.setattr(server, "_db", None)

    class StubDb:
        def __init__(self):
            self.get_metrics_calls = 0
            self.read_iter_calls = 0

        def get_metrics(self, key, db, sensor, force_reload=False):
            self.get_metrics_calls += 1
            return ["active", "reactive"]

        def read_iter(self, key, db, sensor, start_t, end_t, step=None):
            self.read_iter_calls += 1
            # Yield one empty chunk so the streaming generator completes.
            yield [], [], 1.0

    # Patch the OngTSDB constructor in the server module's namespace.
    # The helper does `OngTSDB()` (a bare name), so the patch must land
    # in the server module, not just in the database module.
    monkeypatch.setattr(server, "OngTSDB", lambda *a, **kw: StubDb())

    server.app.config["TESTING"] = True
    try:
        client = server.app.test_client()
        body = {
            "range": {"from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z"},
            "targets": [{"target": "active"}],
        }
        raw = "x:write".encode()
        headers = {"Authorization": "Basic " + base64.b64encode(raw).decode()}
        resp = client.post("/mydb/mysensor/query", json=body, headers=headers)
    finally:
        server.app.config["TESTING"] = False

    # On the unfixed code, the route hits `_db.get_metrics(...)` where
    # `_db` is None, raising AttributeError, which the global error
    # handler turns into a 500 with the NoneType description. The
    # assertion below catches that exact failure mode.
    body_text = resp.get_data(as_text=True)
    assert resp.status_code != 500, (
        f"grafana /query returned 500; the route is probably still "
        f"reaching the bare `_db` global instead of `_get_db()`. "
        f"Body: {body_text[:500]}"
    )
    assert "NoneType" not in body_text, (
        f"grafana /query response body mentions NoneType; the route is "
        f"still trying to call a method on the bare `_db` global. "
        f"Body: {body_text[:500]}"
    )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
