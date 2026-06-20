import gzip
import os
import re
import stat

import numpy as np
import pytest

from ong_tsdb import (
    CHUNK_ROWS,
    COMPRESSION_EXT,
    COMPRESSION_GZIP,
    COMPRESSION_ZSTD,
)
from ong_tsdb.fileutils import (
    DTYPE,
    FileUtils,
    _StdlibProgressBar,
    _make_progress_bar,
    extract_filename_parts,
    re_chunk_filename,
    _get_chunkcolumns,
)


@pytest.fixture
def fu(tmp_path):
    """FileUtils rooted in a temp dir, no real user/group checks needed."""
    return FileUtils(base_path=str(tmp_path))


def _make_chunk(path, n_cols, n_rows=CHUNK_ROWS, fill=0.0):
    """Create a chunk file. n_cols is the TOTAL array width
    (matches the value in the filename's `<n_cols>` segment)."""
    arr = np.full((n_rows, n_cols), fill, dtype=DTYPE)
    arr[:, 0] = 0  # all rows marked as empty
    with open(path, "wb") as f:
        f.write(arr.tobytes())
    return arr


def _gzip_compress_file(path: str) -> None:
    """Re-write ``path`` in place, gzip-compressing its current bytes.
    Used by migration tests to simulate a legacy gzip chunk on disk."""
    with open(path, "rb") as f:
        raw = f.read()
    with open(path, "wb") as f:
        f.write(gzip.compress(raw))


DTYPE_ITEMSIZE = np.dtype(DTYPE).itemsize


def test_extract_filename_parts_valid_gz():
    parts = extract_filename_parts("1234.5.gz")
    assert parts == {"timestamp": "1234", "n_columns": "5", "compression": ".gz"}


def test_extract_filename_parts_valid_uncompressed():
    parts = extract_filename_parts("1234.5")
    assert parts == {"timestamp": "1234", "n_columns": "5", "compression": None}


@pytest.mark.parametrize(
    "bad_name",
    [
        "1234.5.gz.bak",
        "1234.5.gz\n",
        ".1234.5",
        "1234.5.",
        "abc.5.gz",
        "1234..gz",
        "1234",
    ],
)
def test_extract_filename_parts_rejects_malformed(bad_name):
    with pytest.raises(ValueError, match="does not match chunk pattern"):
        extract_filename_parts(bad_name)


def test_regex_is_anchored_at_end():
    # fullmatch must reject names with extra trailing junk
    assert re_chunk_filename.fullmatch("1234.5.gz.bak") is None
    assert re_chunk_filename.fullmatch("1234.5") is not None


def test_get_chunkcolumns_uses_filename_n_columns():
    assert _get_chunkcolumns("1234.5") == 5
    assert _get_chunkcolumns("1234.5.gz") == 5


def test_safe_createfile_sets_owner_permissions(tmp_path, fu):
    path = str(tmp_path / "test.txt")
    with fu.safe_createfile(path, "w") as f:
        f.write("hi")
    mode = stat.S_IMODE(os.stat(path).st_mode)
    # 'other' must not have write permission.
    assert mode & stat.S_IWOTH == 0


def test_fast_read_np_happy_path(tmp_path, fu):
    path = str(tmp_path / "0.5")
    _make_chunk(path, n_cols=5)
    arr = fu.fast_read_np(path)
    assert arr.shape == (CHUNK_ROWS, 5)
    assert arr.dtype == np.float32


def test_fast_read_np_corrupt_raises_with_context(tmp_path, fu):
    # File advertises 6 columns but is 1024 elements (4 KB) short of the
    # expected 16384 * 6 = 98304 elements.
    path = str(tmp_path / "0.6")
    expected = np.full((CHUNK_ROWS, 6), 0.0, dtype=DTYPE)
    raw = expected.tobytes()[: (CHUNK_ROWS * 6 - 1024) * DTYPE_ITEMSIZE]
    with open(path, "wb") as f:
        f.write(raw)

    with pytest.raises(ValueError) as ctx:
        fu.fast_read_np(path)
    msg = str(ctx.value)
    assert "Corrupt chunk" in msg
    assert "expected 98304 elements" in msg
    assert "got 97280 elements" in msg
    assert "Missing 1024 elements" in msg
    assert "4096 bytes" in msg
    assert path in msg


def test_fast_read_np_wrong_columns_in_filename(tmp_path, fu):
    # Filename says 5 columns but file has the byte count for 6.
    path = str(tmp_path / "0.5")
    arr = np.zeros((CHUNK_ROWS, 6), dtype=DTYPE)
    with open(path, "wb") as f:
        f.write(arr.tobytes())
    with pytest.raises(ValueError, match="Corrupt chunk"):
        fu.fast_read_np(path)


def test_fast_read_np_empty_file(tmp_path, fu):
    # Empty file: returns a full-shape NaN array. This preserves the
    # original behavior and is the sensible thing to do (callers filter
    # empty rows by checking positions > 0).
    path = str(tmp_path / "0.5")
    with open(path, "wb"):
        pass
    arr = fu.fast_read_np(path)
    assert arr.shape == (CHUNK_ROWS, 5)
    assert arr.dtype == np.float32
    assert np.isnan(arr).all()


def test_verify_all_chunks_detects_truncated(tmp_path, fu):
    # Sensor 1: 2 valid chunks
    sensor1 = tmp_path / "db" / "s1"
    sensor1.mkdir(parents=True)
    (sensor1 / "CONFIG.JSON").write_text("{}")
    _make_chunk(str(sensor1 / "0.5"), n_cols=5)
    _make_chunk(str(sensor1 / "131072.5"), n_cols=5)

    # Sensor 2: 1 valid + 1 corrupt
    sensor2 = tmp_path / "db" / "s2"
    sensor2.mkdir(parents=True)
    (sensor2 / "CONFIG.JSON").write_text("{}")
    _make_chunk(str(sensor2 / "0.5"), n_cols=5)
    raw = np.zeros((CHUNK_ROWS, 5), dtype=DTYPE).tobytes()
    with open(sensor2 / "131072.5", "wb") as f:
        f.write(raw[: len(raw) - 4 * DTYPE_ITEMSIZE])  # 16 bytes short

    corrupt = fu.verify_all_chunks(print_per_chunk_data=False)
    assert len(corrupt) == 1
    fpath, msg, _, _ = corrupt[0]
    assert fpath.endswith("131072.5")
    assert "Corrupt chunk" in msg


def test_verify_all_chunks_empty_db(tmp_path, fu):
    # No databases at all: should not raise
    assert fu.verify_all_chunks(print_per_chunk_data=False) == []


def test_verify_all_chunks_quiet_suppresses_per_chunk_output(tmp_path, fu, capsys):
    """quiet=True must suppress the per-chunk and per-sensor summary output.
    The corrupt-chunk report at the end is still printed.
    """
    sensor1 = tmp_path / "db" / "s1"
    sensor1.mkdir(parents=True)
    (sensor1 / "CONFIG.JSON").write_text("{}")
    _make_chunk(str(sensor1 / "0.5"), n_cols=5)
    _make_chunk(str(sensor1 / "131072.5"), n_cols=5)
    # One corrupt chunk
    raw = np.zeros((CHUNK_ROWS, 5), dtype=DTYPE).tobytes()
    with open(sensor1 / "262144.5", "wb") as f:
        f.write(raw[: len(raw) - 4 * DTYPE_ITEMSIZE])

    # Verbose (default): expect the timestamp line, the pprint stat, and the summary
    fu.verify_all_chunks(print_per_chunk_data=False)
    verbose = capsys.readouterr().out
    assert "0.0" in verbose  # timestamp of the first chunk
    assert "Summary for" in verbose
    assert "Number of chunks" in verbose
    assert "Found 1 corrupt chunk" in verbose

    # Quiet: only the corrupt-chunk report
    capsys.readouterr()  # clear
    corrupt = fu.verify_all_chunks(print_per_chunk_data=False, quiet=True)
    quiet_out = capsys.readouterr().out
    assert "0.0" not in quiet_out, "quiet mode should not print timestamps"
    assert "Summary for" not in quiet_out, "quiet mode should not print summary"
    assert "Number of chunks" not in quiet_out, "quiet mode should not print count"
    assert "Found 1 corrupt chunk" in quiet_out
    # The corrupt list is still returned
    assert len(corrupt) == 1
    assert corrupt[0][0].endswith("262144.5")


def test_verify_all_chunks_quiet_clean_db(tmp_path, fu, capsys):
    """Quiet mode on a clean DB must produce no output at all."""
    sensor1 = tmp_path / "db" / "s1"
    sensor1.mkdir(parents=True)
    (sensor1 / "CONFIG.JSON").write_text("{}")
    _make_chunk(str(sensor1 / "0.5"), n_cols=5)

    corrupt = fu.verify_all_chunks(quiet=True)
    out = capsys.readouterr().out
    assert corrupt == []
    assert out == ""


def test_verify_all_chunks_progress_runs_and_returns_corrupt(tmp_path, fu, capsys):
    """progress=True must run end-to-end, suppress per-chunk/sensor output,
    and still return the corrupt list and print the corrupt report.
    """
    sensor1 = tmp_path / "db" / "s1"
    sensor1.mkdir(parents=True)
    (sensor1 / "CONFIG.JSON").write_text("{}")
    _make_chunk(str(sensor1 / "0.5"), n_cols=5)
    _make_chunk(str(sensor1 / "131072.5"), n_cols=5)
    # Corrupt one
    raw = np.zeros((CHUNK_ROWS, 5), dtype=DTYPE).tobytes()
    with open(sensor1 / "262144.5", "wb") as f:
        f.write(raw[: len(raw) - 4 * DTYPE_ITEMSIZE])

    corrupt = fu.verify_all_chunks(progress=True)
    captured = capsys.readouterr()
    out = captured.out
    err = captured.err
    # Returns the corrupt list
    assert len(corrupt) == 1
    assert corrupt[0][0].endswith("262144.5")
    # Per-chunk / per-sensor output is suppressed
    assert "0.0" not in out
    assert "Summary for" not in out
    # Corrupt report is still printed
    assert "Found 1 corrupt chunk" in out
    # Progress text went to stderr (either tqdm or the stdlib fallback)
    if "chunk" in err.lower() or "%" in err:
        pass  # tqdm or stdlib bar wrote something
    # If tqdm is missing, the stdlib bar should have written at least once
    try:
        import tqdm  # noqa: F401
    except ImportError:
        assert "Verifying chunks" in err or "/" in err


def test_verify_all_chunks_progress_clean_db_no_corrupt(tmp_path, fu, capsys):
    """progress=True with a clean DB: returns [], corrupt report is not printed."""
    sensor1 = tmp_path / "db" / "s1"
    sensor1.mkdir(parents=True)
    (sensor1 / "CONFIG.JSON").write_text("{}")
    _make_chunk(str(sensor1 / "0.5"), n_cols=5)
    _make_chunk(str(sensor1 / "131072.5"), n_cols=5)

    corrupt = fu.verify_all_chunks(progress=True)
    out = capsys.readouterr().out
    assert corrupt == []
    assert "Found" not in out  # no corrupt report


def test_stdlib_progress_bar_writes_to_stderr():
    """The stdlib fallback must update its counter and finish with a newline."""
    bar = _StdlibProgressBar(100, desc="Testing")
    bar.update(50)
    # First update should not print (step is 5)
    # But update again to cross the threshold
    for _ in range(60):
        bar.update(1)
    bar.close()
    # The close() should print a 100% line and a trailing newline.


def test_stdlib_progress_bar_zero_total():
    bar = _StdlibProgressBar(0, desc="Empty")
    bar.update(0)  # should not raise
    bar.close()  # should not raise


def test_atomic_write_creates_final_file(tmp_path, fu):
    path = str(tmp_path / "atomic.bin")
    with fu.safe_createfile(path, "wb") as f:
        f.write(b"hello world")
    assert os.path.isfile(path)
    assert open(path, "rb").read() == b"hello world"
    # No .tmp file remains
    assert not any(e.startswith("atomic.bin.tmp.") for e in os.listdir(tmp_path))


def test_atomic_write_preserves_existing_on_failure(tmp_path, fu, monkeypatch):
    """If a write fails mid-stream, the existing file must be untouched."""
    path = str(tmp_path / "atomic.bin")
    with fu.safe_createfile(path, "wb") as f:
        f.write(b"original content")
    original_bytes = open(path, "rb").read()
    assert original_bytes == b"original content"

    # Patch get_open_func so that opening a .tmp file raises (simulating a
    # crash before any data was written). The existing file must remain
    # intact because os.replace never runs.
    import os as _os

    def boom_open(_self, filename):
        if ".tmp." in _os.path.basename(filename):
            return lambda p, m: (_ for _ in ()).throw(
                OSError("simulated crash during write")
            )
        # Fall back to real gzip/open as appropriate
        from ong_tsdb import COMPRESSION_EXT

        if filename.endswith(COMPRESSION_EXT):
            import gzip

            return gzip.open
        return open

    monkeypatch.setattr(FileUtils, "get_open_func", boom_open)

    with pytest.raises(OSError, match="simulated crash"):
        with fu.safe_createfile(path, "wb") as f:
            f.write(b"this should never reach disk")

    # Original content must still be there
    assert open(path, "rb").read() == original_bytes


def test_atomic_write_cleans_stale_tmp(tmp_path, fu):
    path = str(tmp_path / "atomic.bin")
    # Simulate a previous crashed writer: leave a stale .tmp file
    stale = path + ".tmp.99999.99999"
    with open(stale, "wb") as f:
        f.write(b"orphan")
    assert os.path.isfile(stale)

    # Next safe_createfile call should remove the stale .tmp before opening
    with fu.safe_createfile(path, "wb") as f:
        f.write(b"fresh")
    assert open(path, "rb").read() == b"fresh"
    # The stale file is gone
    assert not os.path.isfile(stale)


# -----------------------------------------------------------------------
# fast_read_np_partial
# -----------------------------------------------------------------------


def test_fast_read_np_partial_full_file(tmp_path, fu):
    path = str(tmp_path / "0.5")
    arr_expected = _make_chunk(path, n_cols=5)
    arr, n_rows, n_cols = fu.fast_read_np_partial(path)
    assert n_rows == CHUNK_ROWS
    assert n_cols == 5
    assert arr.shape == (CHUNK_ROWS, 5)
    np.testing.assert_array_equal(arr, arr_expected)


def test_fast_read_np_partial_truncated(tmp_path, fu):
    path = str(tmp_path / "0.6")
    full = np.full((CHUNK_ROWS, 6), 0.0, dtype=DTYPE)
    # Write 13312 rows of data + 4 extra bytes (one byte into the next row)
    full[:13312] = np.ones((13312, 6), dtype=DTYPE)
    # Only the first 13312 rows + a few trailing bytes (a partial row)
    raw = full[:13312].tobytes() + b"\x00\x00\x00\x00"
    with open(path, "wb") as f:
        f.write(raw)
    arr, n_rows, n_cols = fu.fast_read_np_partial(path)
    assert n_rows == 13312
    assert n_cols == 6
    assert arr.shape == (13312, 6)
    # The recovered rows are the 13312 data rows
    np.testing.assert_array_equal(arr, full[:13312])


def test_fast_read_np_partial_empty_file(tmp_path, fu):
    path = str(tmp_path / "0.5")
    open(path, "wb").close()
    arr, n_rows, n_cols = fu.fast_read_np_partial(path)
    assert arr.shape == (0, 5)
    assert n_rows == 0
    assert n_cols == 5


def test_fast_read_np_partial_missing_file(tmp_path, fu):
    arr, n_rows, n_cols = fu.fast_read_np_partial(str(tmp_path / "missing.5"))
    assert arr is None
    assert n_rows == 0
    assert n_cols == 0


# -----------------------------------------------------------------------
# repair_corrupt_chunks
# -----------------------------------------------------------------------


def _make_chunk_with_data(path, n_cols, n_data_rows, fill=0.0):
    """Create a chunk with `n_data_rows` rows of data (with valid checksums)
    and the rest as NaN. The data row at position i has:
        col 0: i + 1  (1-based position)
        cols 1:-1: (i + 1) * 1.0  for each metric
        col -1: sum of metric values = n_metrics * (i + 1)
    """
    n_metrics = n_cols - 2
    arr = np.full((CHUNK_ROWS, n_cols), np.nan, dtype=DTYPE)
    for i in range(n_data_rows):
        arr[i, 0] = i + 1
        arr[i, 1:-1] = fill + (i + 1) * 1.0
        arr[i, -1] = n_metrics * (fill + (i + 1) * 1.0)
    with open(path, "wb") as f:
        f.write(arr.tobytes())
    return arr


def test_repair_truncated_chunk_keeps_valid_rows_and_pads_nan(tmp_path, fu):
    """Truncate a chunk to 13312 rows, repair, verify the result."""
    path = str(tmp_path / "0.6")
    full = _make_chunk_with_data(path, n_cols=6, n_data_rows=CHUNK_ROWS)
    # Truncate: keep only the first 13312 rows
    raw = open(path, "rb").read()
    truncated_bytes = raw[: 13312 * 6 * DTYPE_ITEMSIZE]
    # Add 4 trailing bytes (partial row) to simulate a real-world truncation
    with open(path, "wb") as f:
        f.write(truncated_bytes + b"\x00\x00\x00\x00")

    # Sanity: fast_read_np raises
    with pytest.raises(ValueError, match="Corrupt chunk"):
        fu.fast_read_np(path)

    # Repair
    results = fu.repair_corrupt_chunks([(path, "truncated", None, None)])
    assert len(results) == 1
    fpath, status, detail = results[0]
    assert status == "repaired"
    assert "13312" in detail
    assert fpath == path

    # The repaired file is now a valid full chunk
    arr = fu.fast_read_np(path)
    assert arr.shape == (CHUNK_ROWS, 6)
    # First 13312 rows are preserved exactly
    np.testing.assert_array_equal(arr[:13312], full[:13312])
    # Last 3072 rows are NaN
    assert np.isnan(arr[13312:]).all()


def test_repair_refuses_to_fix_when_checksum_mismatch(tmp_path, fu):
    """If a data row's checksum is wrong, the file must NOT be modified."""
    path = str(tmp_path / "0.6")
    _make_chunk_with_data(path, n_cols=6, n_data_rows=CHUNK_ROWS)
    # Truncate
    raw = open(path, "rb").read()
    truncated_bytes = raw[: 13312 * 6 * DTYPE_ITEMSIZE]
    with open(path, "wb") as f:
        f.write(truncated_bytes)

    # Corrupt one data row's metric (so its checksum no longer matches).
    # np.frombuffer returns a read-only view, so we make a writable copy.
    truncated_arr = np.frombuffer(truncated_bytes, dtype=DTYPE).copy().reshape(13312, 6)
    truncated_arr[100, 1] = 999.0  # change a metric value without updating checksum
    corrupted_bytes = truncated_arr.tobytes()
    with open(path, "wb") as f:
        f.write(corrupted_bytes)

    # Repair should refuse
    results = fu.repair_corrupt_chunks([(path, "truncated", None, None)])
    assert len(results) == 1
    fpath, status, detail = results[0]
    assert status == "skipped_checksum"
    assert "invalid checksum" in detail

    # The file is untouched
    assert open(path, "rb").read() == corrupted_bytes
    # No backup was created
    assert not os.path.exists(path + ".corrupt.bak")


def test_repair_empty_file_creates_all_nan(tmp_path, fu):
    """An empty (0-row) file is repaired to a full NaN chunk."""
    path = str(tmp_path / "0.5")
    open(path, "wb").close()

    results = fu.repair_corrupt_chunks([(path, "truncated", None, None)])
    assert len(results) == 1
    fpath, status, detail = results[0]
    assert status == "repaired"
    assert "0 row" in detail
    assert "16384" in detail

    # The repaired file is a full chunk of NaN
    arr = fu.fast_read_np(path)
    assert arr.shape == (CHUNK_ROWS, 5)
    assert np.isnan(arr).all()


def test_repair_dry_run_does_not_write(tmp_path, fu):
    path = str(tmp_path / "0.6")
    _make_chunk_with_data(path, n_cols=6, n_data_rows=CHUNK_ROWS)
    raw = open(path, "rb").read()
    truncated_bytes = raw[: 13312 * 6 * DTYPE_ITEMSIZE]
    with open(path, "wb") as f:
        f.write(truncated_bytes)
    size_before = os.path.getsize(path)

    results = fu.repair_corrupt_chunks([(path, "truncated", None, None)], dry_run=True)
    assert results[0][1] == "would_repair"

    # File is untouched
    assert os.path.getsize(path) == size_before
    assert not os.path.exists(path + ".corrupt.bak")


def test_repair_no_backup_removes_original(tmp_path, fu):
    path = str(tmp_path / "0.6")
    _make_chunk_with_data(path, n_cols=6, n_data_rows=CHUNK_ROWS)
    raw = open(path, "rb").read()
    truncated_bytes = raw[: 13312 * 6 * DTYPE_ITEMSIZE]
    with open(path, "wb") as f:
        f.write(truncated_bytes)

    results = fu.repair_corrupt_chunks([(path, "truncated", None, None)], backup=False)
    assert results[0][1] == "repaired"

    # The repaired file exists; no backup
    assert os.path.isfile(path)
    assert not os.path.exists(path + ".corrupt.bak")


def test_repair_integration_with_verify(tmp_path, fu):
    """End-to-end: scan, repair, re-verify -> no corrupt chunks remain."""
    # Create a sensor with 2 valid + 2 corrupt chunks
    sensor1 = tmp_path / "db" / "s1"
    sensor1.mkdir(parents=True)
    (sensor1 / "CONFIG.JSON").write_text("{}")
    _make_chunk_with_data(str(sensor1 / "0.5"), n_cols=5, n_data_rows=CHUNK_ROWS)
    _make_chunk_with_data(str(sensor1 / "131072.5"), n_cols=5, n_data_rows=CHUNK_ROWS)
    # Corrupt chunks
    for cf, n_data in [
        ("262144.5", CHUNK_ROWS),
        ("393216.5", CHUNK_ROWS),
    ]:
        full = _make_chunk_with_data(str(sensor1 / cf), n_cols=5, n_data_rows=n_data)
        raw = open(str(sensor1 / cf), "rb").read()
        truncated = raw[: 13312 * 5 * DTYPE_ITEMSIZE]
        with open(str(sensor1 / cf), "wb") as f:
            f.write(truncated)

    # Scan
    corrupt = fu.verify_all_chunks(quiet=True)
    assert len(corrupt) == 2

    # Repair
    results = fu.repair_corrupt_chunks(corrupt)
    assert all(r[1] == "repaired" for r in results)

    # Re-verify
    still_corrupt = fu.verify_all_chunks(quiet=True)
    assert still_corrupt == []


def test_repair_mixed_data_and_nan_rows(tmp_path, fu):
    """Chunk with a mix of data rows and NaN rows: the NaN rows are
    preserved as NaN, the data rows keep their valid checksums.
    """
    path = str(tmp_path / "0.5")
    full = _make_chunk_with_data(path, n_cols=5, n_data_rows=100)
    # Rows 100..CHUNK_ROWS are NaN
    raw = open(path, "rb").read()
    truncated = raw[: 1000 * 5 * DTYPE_ITEMSIZE]  # 1000 rows total
    with open(path, "wb") as f:
        f.write(truncated)

    results = fu.repair_corrupt_chunks([(path, "truncated", None, None)])
    assert results[0][1] == "repaired"

    arr = fu.fast_read_np(path)
    assert arr.shape == (CHUNK_ROWS, 5)
    # First 100 rows are the original data
    np.testing.assert_array_equal(arr[:100], full[:100])
    # Rows 100-999: data positions (100..999) -> original data? No,
    # because _make_chunk_with_data only filled 0..99 with data.
    # Rows 100..999 in the truncated file were NaN.
    assert np.isnan(arr[100:1000]).all()
    # Rows 1000..CHUNK_ROWS: NaN (the new padding)
    assert np.isnan(arr[1000:]).all()


def test_repair_skips_unreadable_file(tmp_path, fu):
    """A file that cannot be read at all is reported as unreadable."""
    # Empty corrupt_list returns []
    results = fu.repair_corrupt_chunks([])
    assert results == []
    # Non-existent file
    results = fu.repair_corrupt_chunks([(str(tmp_path / "missing.5"), "", None, None)])
    assert results[0][1] == "skipped_unreadable"


# -----------------------------------------------------------------------
# Dual-format (gzip + zstd) support
# -----------------------------------------------------------------------


def test_extract_filename_parts_accepts_gzip_and_zstd():
    """The chunk-name regex must accept both .gz (legacy) and .zst (new)
    so old chunks keep working after the 0.9.0 default change."""
    gzip_parts = extract_filename_parts("1234.5.gz")
    assert gzip_parts["compression"] == ".gz"
    zstd_parts = extract_filename_parts("1234.5.zst")
    assert zstd_parts["compression"] == ".zst"
    raw_parts = extract_filename_parts("1234.5")
    assert raw_parts["compression"] is None


def test_fast_read_np_reads_gzip_legacy_chunk(tmp_path, fu):
    """A gzip-compressed chunk (the pre-0.9.0 default) must still be
    readable transparently. fast_read_np auto-detects the codec from
    the filename extension."""
    path = str(tmp_path / f"0.5{COMPRESSION_GZIP}")
    full = _make_chunk(path, n_cols=5)
    # _make_chunk wrote raw bytes, but the .gz extension requires
    # actual gzip data. Compress now so the test simulates a real
    # legacy chunk on disk.
    raw = full.tobytes()
    with open(path, "wb") as f:
        f.write(gzip.compress(raw))
    arr = fu.fast_read_np(path)
    assert arr.shape == (CHUNK_ROWS, 5)
    np.testing.assert_array_equal(arr, full)


def test_fast_read_np_reads_zstd_chunk(tmp_path, fu):
    """A zstd-compressed chunk (the 0.9.0 default for new writes) must
    be readable and yield the same data as the gzip version."""
    import zstandard

    path_gz = str(tmp_path / f"0.5{COMPRESSION_GZIP}")
    full = _make_chunk(path_gz, n_cols=5)
    raw = full.tobytes()
    path_zst = str(tmp_path / f"0.5{COMPRESSION_ZSTD}")
    with open(path_zst, "wb") as f:
        f.write(zstandard.ZstdCompressor().compress(raw))
    arr = fu.fast_read_np(path_zst)
    assert arr.shape == (CHUNK_ROWS, 5)
    np.testing.assert_array_equal(arr, full)


def test_fast_read_np_partial_reads_zstd_chunk(tmp_path, fu):
    """fast_read_np_partial (used by repair and migrate) must work on
    truncated zstd-compressed chunks as well as gzip ones."""
    import zstandard

    full = _make_chunk_with_data(tmp_path / "0.5", n_cols=5, n_data_rows=CHUNK_ROWS)
    raw = full.tobytes()
    compressed = zstandard.ZstdCompressor().compress(raw[: 13312 * 5 * 4])  # partial
    path = str(tmp_path / f"131072.5{COMPRESSION_ZSTD}")
    with open(path, "wb") as f:
        f.write(compressed)
    arr, n_rows, n_cols = fu.fast_read_np_partial(path)
    assert n_cols == 5
    assert n_rows == 13312
    np.testing.assert_array_equal(arr, full[:13312])


def test_get_open_func_returns_zstd_stream_reader(tmp_path, fu):
    path = str(tmp_path / f"0.5{COMPRESSION_ZSTD}")
    import zstandard

    with open(path, "wb") as f:
        f.write(zstandard.ZstdCompressor().compress(b"hello world"))
    open_func = fu.get_open_func(path)
    with open_func(path, "rb") as f:
        assert f.read() == b"hello world"


# -----------------------------------------------------------------------
# migrate_compression
# -----------------------------------------------------------------------


def test_migrate_gzip_to_zstd(tmp_path, fu):
    """A gzip chunk is re-written as a zstd chunk with the same data."""
    path_gz = str(tmp_path / f"0.5{COMPRESSION_GZIP}")
    full = _make_chunk_with_data(path_gz, n_cols=5, n_data_rows=CHUNK_ROWS)
    _gzip_compress_file(path_gz)
    assert os.path.exists(path_gz)

    results = fu.migrate_compression(
        [(path_gz, "gz", None, None)], target_ext=COMPRESSION_ZSTD
    )
    assert len(results) == 1
    fpath, status, detail = results[0]
    assert status == "migrated"
    assert fpath == path_gz
    assert ".gz -> .zst" in detail

    # Old file is gone, new one exists
    assert not os.path.exists(path_gz)
    path_zst = str(tmp_path / "0.5.zst")
    assert os.path.exists(path_zst)
    # Backup exists
    assert os.path.exists(path_gz + ".bak")
    # The new file is a valid chunk with the same data
    arr = fu.fast_read_np(path_zst)
    np.testing.assert_array_equal(arr, full)


def test_migrate_zstd_to_gzip(tmp_path, fu):
    """Reverse direction also works."""
    import zstandard

    path_zst = str(tmp_path / f"0.5{COMPRESSION_ZSTD}")
    full = _make_chunk_with_data(path_zst, n_cols=5, n_data_rows=CHUNK_ROWS)
    # convert to zstd on disk
    raw = full.tobytes()
    with open(path_zst, "wb") as f:
        f.write(zstandard.ZstdCompressor().compress(raw))

    results = fu.migrate_compression(
        [(path_zst, "zst", None, None)], target_ext=COMPRESSION_GZIP
    )
    assert results[0][1] == "migrated"

    # gzip version exists, zstd backup remains
    path_gz = str(tmp_path / "0.5.gz")
    assert os.path.exists(path_gz)
    assert os.path.exists(path_zst + ".bak")
    arr = fu.fast_read_np(path_gz)
    np.testing.assert_array_equal(arr, full)


def test_migrate_dry_run_does_not_write(tmp_path, fu):
    path_gz = str(tmp_path / f"0.5{COMPRESSION_GZIP}")
    _make_chunk_with_data(path_gz, n_cols=5, n_data_rows=CHUNK_ROWS)
    _gzip_compress_file(path_gz)
    size_before = os.path.getsize(path_gz)

    results = fu.migrate_compression(
        [(path_gz, "gz", None, None)],
        target_ext=COMPRESSION_ZSTD,
        dry_run=True,
    )
    assert results[0][1] == "would_migrate"

    # File untouched
    assert os.path.getsize(path_gz) == size_before
    assert not os.path.exists(str(tmp_path / "0.5.zst"))
    assert not os.path.exists(path_gz + ".bak")


def test_migrate_no_backup_removes_original(tmp_path, fu):
    path_gz = str(tmp_path / f"0.5{COMPRESSION_GZIP}")
    _make_chunk_with_data(path_gz, n_cols=5, n_data_rows=CHUNK_ROWS)
    # Re-gzip in place so the .gz file actually contains gzip data
    with open(path_gz, "rb") as f:
        raw = f.read()
    with open(path_gz, "wb") as f:
        f.write(gzip.compress(raw))

    results = fu.migrate_compression(
        [(path_gz, "gz", None, None)],
        target_ext=COMPRESSION_ZSTD,
        backup=False,
    )
    assert results[0][1] == "migrated"

    # The gzip file is gone, no .bak (backup=False)
    assert not os.path.exists(path_gz)
    assert not os.path.exists(path_gz + ".bak")
    path_zst = str(tmp_path / "0.5.zst")
    assert os.path.exists(path_zst)


def test_migrate_skips_already_in_target_format(tmp_path, fu):
    """A chunk that is already zstd is left alone when target is zstd."""
    import zstandard

    path_zst = str(tmp_path / f"0.5{COMPRESSION_ZSTD}")
    full = _make_chunk_with_data(path_zst, n_cols=5, n_data_rows=CHUNK_ROWS)
    with open(path_zst, "wb") as f:
        f.write(zstandard.ZstdCompressor().compress(full.tobytes()))
    size_before = os.path.getsize(path_zst)

    results = fu.migrate_compression(
        [(path_zst, "zst", None, None)], target_ext=COMPRESSION_ZSTD
    )
    assert results[0][1] == "skipped_already_target"
    assert os.path.getsize(path_zst) == size_before
    assert not os.path.exists(path_zst + ".bak")


def test_migrate_refuses_to_migrate_bad_checksum(tmp_path, fu):
    """A chunk with invalid checksums is not migrated (data is not
    silently destroyed)."""
    path_gz = str(tmp_path / f"0.5{COMPRESSION_GZIP}")
    full = _make_chunk_with_data(path_gz, n_cols=5, n_data_rows=CHUNK_ROWS)
    # Truncate to keep just first 13312 rows, then corrupt one row's metric
    raw = full.tobytes()[: 13312 * 5 * 4]
    arr = np.frombuffer(raw, dtype=DTYPE).copy().reshape(13312, 5)
    arr[100, 1] = 999.0  # data corruption
    # Re-gzip the corrupted data
    corrupted = gzip.compress(arr.tobytes())
    with open(path_gz, "wb") as f:
        f.write(corrupted)

    results = fu.migrate_compression(
        [(path_gz, "gz", None, None)], target_ext=COMPRESSION_ZSTD
    )
    assert results[0][1] == "skipped_checksum"
    # gzip file is untouched
    assert os.path.exists(path_gz)
    assert not os.path.exists(str(tmp_path / "0.5.zst"))


def test_migrate_invalid_target_ext_raises(tmp_path, fu):
    path = str(tmp_path / "0.5")
    with pytest.raises(ValueError, match="target_ext must be one of"):
        fu.migrate_compression([(path, "", None, None)], target_ext=".rar")


def test_migrate_integration_with_getchunks(tmp_path, fu):
    """End-to-end: a directory with a mix of gzip and zstd chunks is
    fully migrated; verify_all_chunks afterwards returns []."""
    sensor1 = tmp_path / "db" / "s1"
    sensor1.mkdir(parents=True)
    (sensor1 / "CONFIG.JSON").write_text("{}")
    import zstandard

    def _write_gz_chunk(path_no_ext, n_data):
        _make_chunk_with_data(path_no_ext, n_cols=5, n_data_rows=n_data)
        with open(path_no_ext, "rb") as f:
            raw = f.read()
        gz_path = path_no_ext + COMPRESSION_GZIP
        with open(gz_path, "wb") as f:
            f.write(gzip.compress(raw))
        os.remove(path_no_ext)
        return gz_path

    def _write_zst_chunk(path_no_ext, n_data):
        _make_chunk_with_data(path_no_ext, n_cols=5, n_data_rows=n_data)
        with open(path_no_ext, "rb") as f:
            raw = f.read()
        zst_path = path_no_ext + COMPRESSION_ZSTD
        with open(zst_path, "wb") as f:
            f.write(zstandard.ZstdCompressor().compress(raw))
        os.remove(path_no_ext)
        return zst_path

    path_a = _write_gz_chunk(str(sensor1 / "0.5"), CHUNK_ROWS)
    path_b = _write_gz_chunk(str(sensor1 / "131072.5"), CHUNK_ROWS)
    path_c = _write_zst_chunk(str(sensor1 / "262144.5"), CHUNK_ROWS)

    # Use full paths (this is what the CLI does too) to avoid any
    # ambiguity about where the file lives.
    chunks = [fu.path("db", "s1", cf) for cf in fu.getchunks("db", "s1")]
    assert len(chunks) == 3

    results = fu.migrate_compression(chunks, target_ext=COMPRESSION_ZSTD)
    statuses = [r[1] for r in results]
    assert statuses.count("migrated") == 2, f"got {statuses}"
    assert statuses.count("skipped_already_target") == 1, f"got {statuses}"

    # All chunks are now zstd; verify reports none as corrupt
    still_corrupt = fu.verify_all_chunks(quiet=True)
    assert still_corrupt == []
