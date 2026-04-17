"""
save.py — Storage cave.

Writes processed DataFrames to data/processed/<LEG>.csv.
"""

import logging
import os

import pandas as pd

logger = logging.getLogger(__name__)

PROCESSED_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "processed")


def save_legislature(df: pd.DataFrame, leg_name: str) -> str:
    """
    Write processed DataFrame to data/processed/<leg_name>.csv.

    Returns the output path.
    diputado_id index is written as first column.
    """
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    out_path = os.path.join(PROCESSED_DIR, f"{leg_name}.csv")
    df.to_csv(out_path, index=True, encoding="utf-8")
    file_kb = os.path.getsize(out_path) / 1024
    logger.info("[%s] Saved %d rows × %d cols → %s (%.1f KB)",
                leg_name, len(df), len(df.columns), out_path, file_kb)
    return out_path
