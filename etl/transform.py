"""
transform.py — Cooking cave step 2: extract features from JSON columns.

Flattens nested JSON columns into scalar ML-ready features:
  comisiones         → n_comisiones, n_presidencias, n_secretarias, lider_comision
  licencias          → n_licencias
  trayectoria_*      → n_trayectoria_* (entry counts)
  organos_de_gobierno → n_organos_gobierno
  observaciones      → n_observaciones

Original JSON columns are dropped after extraction.
"""

import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)


def _safe_parse(cell) -> list:
    """Parse JSON cell → list. Returns [] on null/error."""
    if pd.isna(cell) or not str(cell).strip():
        return []
    try:
        result = json.loads(cell)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _count_json(series: pd.Series) -> pd.Series:
    """Count entries in a JSON-list column."""
    return series.apply(lambda x: len(_safe_parse(x))).astype(int)


def _extract_comisiones(df: pd.DataFrame, leg_name: str = "?") -> pd.DataFrame:
    """
    From comisiones JSON:
      n_comisiones    — total count
      n_presidencias  — rows where Puesto == 'Presidente'
      n_secretarias   — rows where Puesto == 'Secretario'
      lider_comision  — 1 if has any Presidente or Secretario role
    """
    parsed = df["comisiones"].apply(_safe_parse)

    df["n_comisiones"] = parsed.apply(len).astype(int)
    df["n_presidencias"] = parsed.apply(
        lambda rows: sum(1 for r in rows if r.get("Puesto", "").strip() == "Presidente")
    ).astype(int)
    df["n_secretarias"] = parsed.apply(
        lambda rows: sum(1 for r in rows if r.get("Puesto", "").strip() == "Secretario")
    ).astype(int)
    df["lider_comision"] = ((df["n_presidencias"] + df["n_secretarias"]) > 0).astype(int)

    logger.info("[%s] comisiones: total=%d, presidencias=%d, secretarias=%d, lideres=%d",
                leg_name,
                df["n_comisiones"].sum(),
                df["n_presidencias"].sum(),
                df["n_secretarias"].sum(),
                df["lider_comision"].sum())

    df.drop(columns=["comisiones"], inplace=True)
    return df


def _extract_trayectorias(df: pd.DataFrame, leg_name: str = "?") -> pd.DataFrame:
    """Extract count features from each trajectory JSON column."""
    trayectoria_cols = {
        "licencias_reincorporaciones": "n_licencias",
        "trayectoria_administrativa":  "n_trayectoria_admin",
        "trayectoria_legislativa":     "n_trayectoria_legislativa",
        "trayectoria_politica":        "n_trayectoria_politica",
        "trayectoria_academica":       "n_trayectoria_academica",
        "trayectoria_empresarial":     "n_trayectoria_empresarial",
        "otros_rubros":                "n_otros_rubros",
        "organos_de_gobierno":         "n_organos_gobierno",
        "observaciones":               "n_observaciones",
    }
    for raw_col, feat_col in trayectoria_cols.items():
        if raw_col in df.columns:
            df[feat_col] = _count_json(df[raw_col]).astype(int)
            n_with_data = (df[feat_col] > 0).sum()
            logger.debug("[%s] %-35s → %-30s | %d rows with data, total entries=%d",
                         leg_name, raw_col, feat_col, n_with_data, df[feat_col].sum())
            df.drop(columns=[raw_col], inplace=True)
        else:
            df[feat_col] = 0
            logger.debug("[%s] %-35s missing — filled with 0", leg_name, raw_col)

    logger.info("[%s] trayectorias extracted: admin=%d, leg=%d, pol=%d, acad=%d, emp=%d",
                leg_name,
                df["n_trayectoria_admin"].sum(),
                df["n_trayectoria_legislativa"].sum(),
                df["n_trayectoria_politica"].sum(),
                df["n_trayectoria_academica"].sum(),
                df["n_trayectoria_empresarial"].sum())

    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract all JSON-column features.

    Input:  cleaned DataFrame from clean.py (JSON cols still as str)
    Output: fully flat ML-ready DataFrame
            - all JSON columns replaced by scalar counts
            - diputado_id remains as index
    """
    df = df.copy()
    leg_name = df["legislatura_nombre"].iloc[0] if "legislatura_nombre" in df.columns else "?"
    df = _extract_comisiones(df, leg_name)
    df = _extract_trayectorias(df, leg_name)
    logger.info("[%s] transform done → %d rows × %d cols", leg_name, len(df), len(df.columns))
    return df
