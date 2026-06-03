"""CLI entry point:

python -m ong_tsdb verify [--db NAME] [--corrupt-only] [--progress]
python -m ong_tsdb repair [--db NAME] [--no-backup] [--dry-run]
"""

import argparse
import sys

from ong_tsdb import BASE_DIR, logger
from ong_tsdb.fileutils import FileUtils


def main():
    parser = argparse.ArgumentParser(prog="ong_tsdb")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_v = sub.add_parser("verify", help="Scan all chunks and report corrupt ones")
    p_v.add_argument("--db", default=None, help="Filter to a single database")
    p_v.add_argument(
        "--corrupt-only",
        action="store_true",
        help="Only print corrupt chunks (skip per-chunk output and per-sensor summary)",
    )
    p_v.add_argument(
        "--progress",
        action="store_true",
        help="Show a progress bar (tqdm if available, else a stdlib fallback). Implies --corrupt-only.",
    )
    p_v.add_argument(
        "--base-dir", default=BASE_DIR, help="Override BASE_DIR (default: from config)"
    )

    p_r = sub.add_parser(
        "repair",
        help="Repair truncated chunks: read the recoverable rows, verify their checksums, "
        "and extend the file with NaN-filled rows up to CHUNK_ROWS.",
    )
    p_r.add_argument("--db", default=None, help="Filter to a single database")
    p_r.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not keep the original file at <file>.corrupt.bak (still safe via atomic write)",
    )
    p_r.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be repaired without writing anything",
    )
    p_r.add_argument(
        "--base-dir", default=BASE_DIR, help="Override BASE_DIR (default: from config)"
    )

    args = parser.parse_args()

    if args.cmd == "verify":
        fu = FileUtils(base_path=args.base_dir)
        corrupt = fu.verify_all_chunks(
            filter_db_name=args.db,
            quiet=args.corrupt_only or args.progress,
            progress=args.progress,
        )
        sys.exit(1 if corrupt else 0)

    elif args.cmd == "repair":
        fu = FileUtils(base_path=args.base_dir)
        logger.info("Scanning for corrupt chunks in %s ...", args.base_dir)
        corrupt = fu.verify_all_chunks(
            filter_db_name=args.db,
            quiet=True,
        )
        if not corrupt:
            print("All chunks OK -- nothing to repair.")
            sys.exit(0)

        print(f"Found {len(corrupt)} corrupt chunk(s).")
        for fpath, msg, diff, date in corrupt:
            print(f"  {fpath}")
            print(f"    {msg}")
        print()

        results = fu.repair_corrupt_chunks(
            corrupt_list=corrupt,
            backup=not args.no_backup,
            dry_run=args.dry_run,
        )
        n_repaired = sum(1 for r in results if r[1] == "repaired")
        n_would = sum(1 for r in results if r[1] == "would_repair")
        n_skipped = len(results) - n_repaired - n_would

        for fpath, status, detail in results:
            label = {
                "repaired": "REPAIR ",
                "would_repair": "DRY-RUN",
                "skipped_checksum": "SKIP   ",
                "skipped_unreadable": "SKIP   ",
            }[status]
            print(f"  {label} {fpath}")
            print(f"          {detail}")

        print()
        if args.dry_run:
            print(f"Dry run: would repair {n_would} chunk(s), skip {n_skipped}.")
            sys.exit(0)

        print(f"Repaired {n_repaired} chunk(s), skipped {n_skipped}.")
        if n_repaired > 0 and not args.no_backup:
            print("Originals preserved at <file>.corrupt.bak")

        # Re-verify ONLY the files we touched (not the whole DB), to confirm
        # they are now valid. This avoids a slow full-database re-scan.
        print()
        logger.info("Re-verifying repaired files ...")
        repaired_paths = [r[0] for r in results if r[1] == "repaired"]
        bad = []
        for p in repaired_paths:
            try:
                fu.fast_read_np(p)
            except ValueError as e:
                bad.append((p, str(e)))
        if bad:
            print(f"=== {len(bad)} repaired file(s) still corrupt ===")
            for p, msg in bad:
                print(f"  {p}")
                print(f"    {msg}")
            sys.exit(1)
        else:
            print(f"=== All {n_repaired} repaired file(s) now pass fast_read_np ===")
            sys.exit(0)


if __name__ == "__main__":
    main()
