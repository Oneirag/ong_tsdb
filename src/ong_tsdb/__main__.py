"""CLI entry point:

python -m ong_tsdb verify  [--db NAME] [--corrupt-only] [--progress]
python -m ong_tsdb repair  [--db NAME] [--no-backup] [--dry-run]
python -m ong_tsdb migrate [--db NAME] [--target zstd|gzip|raw]
                            [--no-backup] [--force]
"""

import argparse
import sys

from ong_tsdb import (
    BASE_DIR,
    COMPRESSION_GZIP,
    COMPRESSION_ZSTD,
    logger,
)
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

    p_m = sub.add_parser(
        "migrate",
        help="Migrate chunks to a different compression format. Default: gzip -> zstd. "
        "Old chunks keep working (dual-format read). The migration is per-sensor "
        "and atomic; the source is backed up to <file>.bak unless --no-backup is "
        "set.",
    )
    p_m.add_argument("--db", default=None, help="Filter to a single database")
    p_m.add_argument(
        "--target",
        default=COMPRESSION_ZSTD,
        choices=[COMPRESSION_ZSTD, COMPRESSION_GZIP, ""],
        help="Target compression: 'zst' (default), 'gz' or '' (uncompressed)",
    )
    p_m.add_argument(
        "--no-backup",
        action="store_true",
        help="Do not keep the original at <file>.bak (faster, no undo)",
    )
    p_m.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Show what would be migrated without writing anything (default)",
    )
    p_m.add_argument(
        "--force",
        action="store_true",
        help="Actually rewrite the chunks. Disables the --dry-run default and "
        "asks for an interactive confirmation before touching any file.",
    )
    p_m.add_argument(
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

    elif args.cmd == "migrate":
        # --dry-run is the default; --force disables it and asks for
        # confirmation so the user has a chance to back out.
        dry_run = not args.force
        if not dry_run:
            confirm = input(
                f"This will re-write all chunks in {args.target or 'uncompressed'} "
                f"format. Originals are kept at <file>.bak unless --no-backup. "
                f"Type 'yes' to continue: "
            )
            if confirm.strip().lower() != "yes":
                print("Aborted.")
                sys.exit(1)

        fu = FileUtils(base_path=args.base_dir)
        # Scan every database/sensor and collect all chunks as full
        # paths (the only way migrate_compression can resolve them
        # correctly).
        if args.db:
            dbs = [args.db]
        else:
            dbs = fu.getdbs()
        all_chunks = []
        for db_name in dbs:
            for sensor in fu.getsensors(db_name):
                all_chunks.extend(
                    fu.path(db_name, sensor, cf) for cf in fu.getchunks(db_name, sensor)
                )

        if not all_chunks:
            print(f"No chunks found in {args.base_dir}.")
            sys.exit(0)

        if args.dry_run:
            print(
                f"Scanning {len(all_chunks)} chunk(s) for migration to "
                f"{args.target or 'uncompressed'}..."
            )
        else:
            print(
                f"Migrating {len(all_chunks)} chunk(s) to {args.target or 'uncompressed'}..."
            )

        results = fu.migrate_compression(
            chunk_list=all_chunks,
            target_ext=args.target,
            backup=not args.no_backup,
            dry_run=dry_run,
        )
        n_migrated = sum(1 for r in results if r[1] == "migrated")
        n_would = sum(1 for r in results if r[1] == "would_migrate")
        n_skipped = sum(1 for r in results if r[1] == "skipped_already_target")
        n_bad = len(results) - n_migrated - n_would - n_skipped

        for fpath, status, detail in results:
            label = {
                "migrated": "MIGRATE",
                "would_migrate": "DRY-RUN",
                "skipped_already_target": "SKIP   ",
                "skipped_checksum": "SKIP   ",
                "skipped_unreadable": "SKIP   ",
            }.get(status, status.upper())
            print(f"  {label} {fpath}")
            print(f"          {detail}")

        print()
        if dry_run:
            print(
                f"Dry run: would migrate {n_would} chunk(s), skip "
                f"{n_skipped + n_bad} (already in target: {n_skipped}, "
                f"bad: {n_bad})."
            )
            print("Run with --force to actually rewrite the chunks.")
        else:
            print(
                f"Migrated {n_migrated} chunk(s); skipped {n_skipped + n_bad} "
                f"(already in target: {n_skipped}, bad: {n_bad})."
            )
            if n_migrated > 0 and not args.no_backup:
                print("Originals preserved at <file>.bak")
            sys.exit(0 if n_bad == 0 else 1)
        sys.exit(0)


if __name__ == "__main__":
    main()
