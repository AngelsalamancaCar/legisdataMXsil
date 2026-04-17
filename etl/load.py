"""
load.py — Hunting cave result reader.

Reads raw CSVs produced by scraper.py from data/<LEG>.csv.
"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

RAW_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

LEGISLATURAS = ["LVII", "LVIII", "LIX", "LX", "LXI", "LXII", "LXIII", "LXIV", "LXV", "LXVI"]


def load_legislature(leg_name: str) -> pd.DataFrame:
    """Load raw CSV for one legislature. Raises FileNotFoundError if missing."""
    leg_name = leg_name.upper()
    if leg_name not in LEGISLATURAS:
        raise ValueError(f"Unknown legislature: {leg_name}. Valid: {LEGISLATURAS}")
    path = os.path.join(RAW_DIR, f"{leg_name}.csv")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Raw data not found: {path}. Run scraper first.")

    file_kb = os.path.getsize(path) / 1024
    logger.info("Reading %s (%.1f KB)", path, file_kb)

    df = pd.read_csv(path, dtype=str)  # all str — clean.py handles types
    df["_source_file"] = leg_name

    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    logger.info("Loaded %d rows × %d cols | total nulls: %d", len(df), len(df.columns), total_nulls)
    if total_nulls:
        top_nulls = null_counts[null_counts > 0].sort_values(ascending=False).head(5)
        for col, n in top_nulls.items():
            logger.debug("  null %-35s %d / %d rows", col, n, len(df))

    return df


def load_all() -> dict[str, pd.DataFrame]:
    """Load all available legislature CSVs. Skips missing files with a warning."""
    result = {}
    for leg in LEGISLATURAS:
        try:
            result[leg] = load_legislature(leg)
        except FileNotFoundError as e:
            logger.warning("SKIP %s — %s", leg, e)
    return result
