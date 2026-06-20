"""Benchmark compression codecs for ong_tsdb chunks.

Compares gzip (currently used) against zstd (various levels) and lz4
on several representative data profiles:

  * all_nan        -- chunk freshly written, no data yet
  * all_zeros      -- sensor that always reads 0
  * realistic      -- mix of NaN, zeros and real values with noise
  * repetitive     -- a sensor that always reports the same value

For each profile, the script measures:
  * ratio          -- original_size / compressed_size
  * compress_ms    -- time to compress
  * decompress_ms  -- time to decompress
  * compressed_kb  -- final size in KB

Optionally, the script can also benchmark against a sample of real
chunks from a given BASE_DIR:

    python examples/benchmark_compression.py --real /path/to/ong_tsdb

The real-data mode samples up to N (default 3) chunks per database/
sensor, decompresses them, recompresses with each codec and reports
the average. This is the most representative measurement because it
uses the actual distribution of values from the user's data.

Results are printed as a compact text table. The script exits with
a short summary of which codec is fastest / smallest on average.
"""

import argparse
import gzip
import os
import random
import time
from timeit import timeit

import numpy as np


# ---------------------------------------------------------------------------
# Codec wrappers
# ---------------------------------------------------------------------------


def gzip_compress(data: bytes, level: int = 6) -> bytes:
    return gzip.compress(data, compresslevel=level)


def gzip_decompress(data: bytes) -> bytes:
    return gzip.decompress(data)


def zstd_compress(data: bytes, level: int = 3) -> bytes:
    import zstandard

    cctx = zstandard.ZstdCompressor(level=level)
    return cctx.compress(data)


def zstd_decompress(data: bytes) -> bytes:
    import zstandard

    dctx = zstandard.ZstdDecompressor()
    return dctx.decompress(data)


def lz4_compress(data: bytes) -> bytes:
    import lz4.block

    return lz4.block.compress(data, mode="default", store_size=True)


def lz4_decompress(data: bytes) -> bytes:
    import lz4.block

    return lz4.block.decompress(data)


CODECS = [
    ("gzip-1", gzip_compress, gzip_decompress, {"level": 1}),
    ("gzip-6", gzip_compress, gzip_decompress, {"level": 6}),
    ("gzip-9", gzip_compress, gzip_decompress, {"level": 9}),
    ("zstd-1", zstd_compress, zstd_decompress, {"level": 1}),
    ("zstd-3", zstd_compress, zstd_decompress, {"level": 3}),
    ("zstd-9", zstd_compress, zstd_decompress, {"level": 9}),
    ("lz4", lz4_compress, lz4_decompress, {}),
]


# ---------------------------------------------------------------------------
# Synthetic profiles
# ---------------------------------------------------------------------------

CHUNK_ROWS = 2**14
DTYPE = np.float32
DTYPE_ITEMSIZE = np.dtype(DTYPE).itemsize


def _all_nan(n_cols: int) -> np.ndarray:
    arr = np.full((CHUNK_ROWS, n_cols + 2), np.nan, dtype=DTYPE)
    return arr


def _all_zeros(n_cols: int) -> np.ndarray:
    arr = np.zeros((CHUNK_ROWS, n_cols + 2), dtype=DTYPE)
    return arr


def _realistic(n_cols: int) -> np.ndarray:
    """Mix of NaN, zeros, and a noisy real signal around a baseline."""
    arr = np.full((CHUNK_ROWS, n_cols + 2), np.nan, dtype=DTYPE)
    n_metrics = n_cols
    # 70% of rows are populated with data; the rest are NaN (unwritten)
    populated = int(CHUNK_ROWS * 0.7)
    for col in range(1, n_metrics + 1):
        baseline = 100.0 * col
        noise = np.random.normal(0, 5.0, size=populated).astype(DTYPE)
        arr[:populated, col] = baseline + noise
    # A few exact zeros scattered in
    for i in random.sample(range(populated), k=max(1, populated // 20)):
        arr[i, 1] = 0.0
    return arr


def _repetitive(n_cols: int) -> np.ndarray:
    """A sensor that always reports the same value (e.g. a stuck meter)."""
    arr = np.full((CHUNK_ROWS, n_cols + 2), np.nan, dtype=DTYPE)
    n_metrics = n_cols
    for col in range(1, n_metrics + 1):
        arr[:, col] = float(col) * 10.0
    # Position column for ~80% of rows
    populated = int(CHUNK_ROWS * 0.8)
    arr[:populated, 0] = np.arange(1, populated + 1, dtype=DTYPE)
    return arr


PROFILES = [
    ("all_nan", _all_nan, 4),  # 4 metrics
    ("all_zeros", _all_zeros, 4),
    ("realistic", _realistic, 4),
    ("repetitive", _repetitive, 8),  # 8 metrics
]


# ---------------------------------------------------------------------------
# Benchmark loop
# ---------------------------------------------------------------------------


def _bench_codec(cname, compress_fn, decompress_fn, kwargs, data: bytes, number=5):
    # Warm-up
    c = compress_fn(data, **kwargs)
    _ = decompress_fn(c)

    # Compress
    t_compress = timeit(lambda: compress_fn(data, **kwargs), number=number) / number

    # Decompress
    t_decompress = timeit(lambda: decompress_fn(c), number=number) / number

    ratio = len(data) / len(c)
    return {
        "codec": cname,
        "ratio": ratio,
        "compress_ms": t_compress * 1000,
        "decompress_ms": t_decompress * 1000,
        "compressed_kb": len(c) / 1024,
    }


def _run_on_array(arr: np.ndarray) -> list:
    data = arr.tobytes()
    results = []
    for cname, cf, df, kw in CODECS:
        try:
            r = _bench_codec(cname, cf, df, kw, data)
            results.append(r)
        except Exception as e:
            results.append({"codec": cname, "error": str(e)})
    return results


def _print_table(profile_name, results, original_kb):
    print(f"\n=== Profile: {profile_name}  (original {original_kb:.1f} KB) ===")
    hdr = f"  {'codec':<10} {'ratio':>8} {'compressed':>12} {'compress':>12} {'decompress':>12}"
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for r in results:
        if "error" in r:
            print(f"  {r['codec']:<10} ERROR: {r['error']}")
            continue
        print(
            f"  {r['codec']:<10} "
            f"{r['ratio']:>7.2f}x "
            f"{r['compressed_kb']:>10.1f} KB "
            f"{r['compress_ms']:>10.2f} ms "
            f"{r['decompress_ms']:>10.2f} ms"
        )


# ---------------------------------------------------------------------------
# Real data
# ---------------------------------------------------------------------------


def _iter_real_chunks(base_dir, max_per_sensor=3, max_sensors=5):
    """Yield (db, sensor, path, data_bytes) for a sample of real chunks."""
    from ong_tsdb import CHUNK_ROWS, COMPRESSION_EXT
    from ong_tsdb.fileutils import FileUtils

    fu = FileUtils(base_path=base_dir)
    n_sensors = 0
    for db in fu.getdbs():
        if n_sensors >= max_sensors:
            return
        for sensor in fu.getsensors(db):
            if n_sensors >= max_sensors:
                return
            chunks = sorted(fu.getchunks(db, sensor))
            for cf in chunks[:max_per_sensor]:
                fpath = fu.path(db, sensor, cf)
                # Decompress first so we can compare the codecs on the
                # actual raw bytes that the chunk stores
                if fpath.endswith(COMPRESSION_EXT):
                    with gzip.open(fpath, "rb") as f:
                        raw = f.read()
                else:
                    with open(fpath, "rb") as f:
                        raw = f.read()
                yield db, sensor, fpath, raw
            n_sensors += 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--real",
        metavar="BASE_DIR",
        default=None,
        help="Also benchmark against real chunks from BASE_DIR",
    )
    parser.add_argument(
        "--real-max",
        type=int,
        default=3,
        help="Max chunks to sample per sensor in --real mode (default 3)",
    )
    parser.add_argument(
        "--real-max-sensors",
        type=int,
        default=5,
        help="Max sensors to scan in --real mode (default 5)",
    )
    args = parser.parse_args()

    random.seed(42)
    np.random.seed(42)

    print(f"Chunk shape: ({CHUNK_ROWS}, cols+2) x {DTYPE_ITEMSIZE}B per element")
    print(f"  = {CHUNK_ROWS * DTYPE_ITEMSIZE} bytes per metric, 16384 rows")
    print(f"Codecs: {[c[0] for c in CODECS]}")

    # ------------------------------------------------------------------ synthetic
    print("\n" + "=" * 60)
    print(" SYNTHETIC DATA")
    print("=" * 60)
    all_results = {}
    for pname, pfn, n_cols in PROFILES:
        arr = pfn(n_cols)
        results = _run_on_array(arr)
        _print_table(pname, results, arr.nbytes / 1024)
        all_results[pname] = results

    # ------------------------------------------------------------------ real
    if args.real:
        print("\n" + "=" * 60)
        print(f" REAL DATA FROM {args.real}")
        print("=" * 60)
        per_codec = {
            c[0]: {"compress": [], "decompress": [], "ratio": [], "kb": []}
            for c in CODECS
        }
        n = 0
        for db, sensor, fpath, raw in _iter_real_chunks(
            args.real,
            max_per_sensor=args.real_max,
            max_sensors=args.real_max_sensors,
        ):
            print(
                f"\n  {os.path.relpath(fpath, args.real)}  "
                f"({len(raw) / 1024:.1f} KB decompressed)"
            )
            for r in _run_on_array(
                np.frombuffer(raw, dtype=DTYPE).reshape(CHUNK_ROWS, -1)
            ):
                if "error" in r:
                    print(f"    {r['codec']:<10} ERROR: {r['error']}")
                    continue
                per_codec[r["codec"]]["compress"].append(r["compress_ms"])
                per_codec[r["codec"]]["decompress"].append(r["decompress_ms"])
                per_codec[r["codec"]]["ratio"].append(r["ratio"])
                per_codec[r["codec"]]["kb"].append(r["compressed_kb"])
            n += 1
        if n == 0:
            print("  (no chunks found)")
            return
        print(f"\n  --- averages over {n} chunk(s) ---")
        hdr = f"  {'codec':<10} {'ratio':>8} {'compressed':>12} {'compress':>12} {'decompress':>12}"
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for cname, _stats in per_codec.items():
            if not per_codec[cname]["compress"]:
                continue
            avg_ratio = sum(per_codec[cname]["ratio"]) / len(per_codec[cname]["ratio"])
            avg_kb = sum(per_codec[cname]["kb"]) / len(per_codec[cname]["kb"])
            avg_c = sum(per_codec[cname]["compress"]) / len(
                per_codec[cname]["compress"]
            )
            avg_d = sum(per_codec[cname]["decompress"]) / len(
                per_codec[cname]["decompress"]
            )
            print(
                f"  {cname:<10} "
                f"{avg_ratio:>7.2f}x "
                f"{avg_kb:>10.1f} KB "
                f"{avg_c:>10.2f} ms "
                f"{avg_d:>10.2f} ms"
            )

    # ------------------------------------------------------------------ summary
    # The synthetic all_nan / all_zeros profiles compress to almost
    # nothing for any codec, so averaging them with the realistic
    # profile produces meaningless "ratio +1000%" numbers. The useful
    # verdicts are below: the "realistic" profile (representative of
    # live data) and the "real data" section (the user's actual chunks).
    # We skip the all-profiles summary on purpose to avoid misleading
    # conclusions.

    print()
    print("=" * 60)
    print(" VERDICT (realistic profile only -- most representative)")
    print("=" * 60)
    realistic = all_results.get("realistic", [])
    if realistic:
        baseline = next((r for r in realistic if r["codec"] == "gzip-6"), None)
        if baseline:
            print(
                f"  baseline gzip-6: ratio {baseline['ratio']:.2f}x, "
                f"compress {baseline['compress_ms']:.2f}ms, "
                f"decompress {baseline['decompress_ms']:.2f}ms, "
                f"size {baseline['compressed_kb']:.1f}KB"
            )
            for r in realistic:
                if r["codec"] == "gzip-6" or "error" in r:
                    continue
                ratio_pct = (r["ratio"] - baseline["ratio"]) / baseline["ratio"] * 100
                speedup_c = (
                    (baseline["compress_ms"] - r["compress_ms"])
                    / baseline["compress_ms"]
                    * 100
                )
                speedup_d = (
                    (baseline["decompress_ms"] - r["decompress_ms"])
                    / baseline["decompress_ms"]
                    * 100
                )
                size_pct = (
                    (baseline["compressed_kb"] - r["compressed_kb"])
                    / baseline["compressed_kb"]
                    * 100
                )
                print(
                    f"  {r['codec']:<10} "
                    f"ratio {ratio_pct:+6.1f}%  "
                    f"compress {speedup_c:+6.1f}%  "
                    f"decompress {speedup_d:+6.1f}%  "
                    f"size {size_pct:+6.1f}%"
                )

    # Real-data verdict (if --real was used)
    if args.real and n > 0:
        print()
        print("=" * 60)
        print(f" VERDICT (real data, {n} chunk(s) averaged)")
        print("=" * 60)
        real_summary = {}
        for cname in [c[0] for c in CODECS]:
            if not per_codec[cname]["compress"]:
                continue
            real_summary[cname] = {
                "ratio": sum(per_codec[cname]["ratio"])
                / len(per_codec[cname]["ratio"]),
                "compress": sum(per_codec[cname]["compress"])
                / len(per_codec[cname]["compress"]),
                "decompress": sum(per_codec[cname]["decompress"])
                / len(per_codec[cname]["decompress"]),
                "kb": sum(per_codec[cname]["kb"]) / len(per_codec[cname]["kb"]),
            }
        baseline = real_summary.get("gzip-6")
        if baseline:
            print(
                f"  baseline gzip-6: ratio {baseline['ratio']:.2f}x, "
                f"compress {baseline['compress']:.2f}ms, "
                f"decompress {baseline['decompress']:.2f}ms"
            )
            for cname, s in real_summary.items():
                if cname == "gzip-6":
                    continue
                ratio_pct = (s["ratio"] - baseline["ratio"]) / baseline["ratio"] * 100
                speedup_c = (
                    (baseline["compress"] - s["compress"]) / baseline["compress"] * 100
                )
                speedup_d = (
                    (baseline["decompress"] - s["decompress"])
                    / baseline["decompress"]
                    * 100
                )
                size_pct = (baseline["kb"] - s["kb"]) / baseline["kb"] * 100
                print(
                    f"  {cname:<10} "
                    f"ratio {ratio_pct:+6.1f}%  "
                    f"compress {speedup_c:+6.1f}%  "
                    f"decompress {speedup_d:+6.1f}%  "
                    f"size {size_pct:+6.1f}%"
                )

    # Verdict: see the dedicated "VERDICT" blocks above for realistic
    # and (if --real) real-data. The all-profiles summary is misleading
    # because the all_nan/all_zeros profiles compress to almost nothing
    # for every codec and dominate the average.


if __name__ == "__main__":
    main()
