"""CLI entry point: python -m ong_tsdb verify [--db NAME] [--corrupt-only] [--progress]"""

import argparse
import sys

from ong_tsdb import BASE_DIR
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
    args = parser.parse_args()
    if args.cmd == "verify":
        fu = FileUtils(base_path=args.base_dir)
        corrupt = fu.verify_all_chunks(
            filter_db_name=args.db,
            quiet=args.corrupt_only or args.progress,
            progress=args.progress,
        )
        sys.exit(1 if corrupt else 0)


if __name__ == "__main__":
    main()
