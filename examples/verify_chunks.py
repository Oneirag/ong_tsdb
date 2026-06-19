"""Inspect a database with the ong_tsdb maintenance CLI.

Run from the repo root:

    python examples/verify_chunks.py
    python examples/verify_chunks.py --corrupt-only
    python examples/verify_chunks.py --progress

Equivalent to invoking the package's own CLI:

    python -m ong_tsdb verify [--corrupt-only] [--progress]

This script is here so the example is self-documenting (no need to
remember the 'python -m ong_tsdb' incantation).
"""

import sys

from ong_tsdb import BASE_DIR
from ong_tsdb.__main__ import main as cli_main


def main():
    # Build a sys.argv that the package CLI understands and call it.
    sys.argv = ["ong_tsdb", "verify"] + sys.argv[1:]
    cli_main()


if __name__ == "__main__":
    main()
