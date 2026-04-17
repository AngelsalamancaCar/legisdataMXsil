#!/usr/bin/env python3
"""
pipeline.py — ETL orchestrator.

Loads raw CSV(s) → cleans → transforms → saves processed CSV(s).

Usage:
  python pipeline.py --legislatura LXVI
  python pipeline.py --legislatura LXIV,LXV,LXVI
  python pipeline.py --legislatura all
"""

import argparse
import logging
import os
import sys

from etl.load import load_legislature, load_all, LEGISLATURAS
from etl.clean import clean
from etl.transform import transform
from etl.save import save_legislature, PROCESSED_DIR

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
LOG_FILE = os.path.join(PROCESSED_DIR, "etl.log")


def setup_logging(verbose: bool = False) -> None:
    """Configure root logger: INFO to console, DEBUG to file."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # capture everything; handlers filter

    # Console: INFO by default, DEBUG if --verbose
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(logging.Formatter(LOG_FORMAT))

    # File: always DEBUG (full detail)
    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    root.addHandler(console)
    root.addHandler(file_handler)


logger = logging.getLogger(__name__)


def run_one(leg_name: str) -> None:
    logger.info("═" * 50)
    logger.info("START %s", leg_name)

    raw = load_legislature(leg_name)
    cleaned = clean(raw)
    processed = transform(cleaned)
    save_legislature(processed, leg_name)

    logger.info("DONE  %s", leg_name)


def resolve_legislaturas(arg: str) -> list[str]:
    if arg.strip().lower() == "all":
        return list(LEGISLATURAS)
    names = [n.strip().upper() for n in arg.split(",")]
    invalid = [n for n in names if n not in LEGISLATURAS]
    if invalid:
        raise SystemExit(
            f"Unknown legislature(s): {', '.join(invalid)}\n"
            f"Valid: {', '.join(LEGISLATURAS)}"
        )
    return names


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL pipeline for legisdatamxsil")
    parser.add_argument(
        "--legislatura",
        required=True,
        metavar="NAME",
        help="Legislature(s) to process. E.g.: LXVI | LXIV,LXV | all",
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show DEBUG-level output on console (always written to log file)",
    )
    args = parser.parse_args()

    setup_logging(verbose=args.verbose)
    logger.info("Log file: %s", os.path.abspath(LOG_FILE))

    targets = resolve_legislaturas(args.legislatura)
    errors = []

    for leg in targets:
        try:
            run_one(leg)
        except FileNotFoundError as e:
            logger.warning("SKIP %s — %s", leg, e)
            errors.append(leg)
        except Exception as e:
            logger.error("FAIL %s — %s", leg, e)
            errors.append(leg)
            raise

    logger.info("═" * 50)
    logger.info("Done. %d processed, %d skipped/failed.", len(targets) - len(errors), len(errors))
    logger.info("Full log → %s", os.path.abspath(LOG_FILE))
    if errors:
        logger.warning("Skipped: %s", errors)
        sys.exit(1)


if __name__ == "__main__":
    main()
