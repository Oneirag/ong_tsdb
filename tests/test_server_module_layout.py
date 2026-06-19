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


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
