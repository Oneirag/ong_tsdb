import os
import re
import stat

import numpy as np
import pytest

from ong_tsdb import CHUNK_ROWS, COMPRESSION_EXT
from ong_tsdb.fileutils import (
    DTYPE,
    FileUtils,
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
