"""
clean.py — Cooking cave step 1: normalize raw data.

Handles:
- Drop useless columns (always-null, redundant)
- Parse nacimiento → anio_nacimiento + edad_al_tomar_cargo
- Encode boolean/categorical columns to numbers
- Fill nulls with ML-safe defaults
- Presence flags for contact fields
"""

import logging

import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)

# Legislature start years (used to compute edad_al_tomar_cargo)
LEG_START_YEAR: dict[str, int] = {
    "LVII":  1997,
    "LVIII": 2000,
    "LIX":   2003,
    "LX":    2006,
    "LXI":   2009,
    "LXII":  2012,
    "LXIII": 2015,
    "LXIV":  2018,
    "LXV":   2021,
    "LXVI":  2024,
}

# Ordinal education encoding (0 = unknown/missing)
GRADO_ORDINAL: dict[str, int] = {
    "no disponible":              0,
    "secundaria":                 1,
    "preparatoria":               2,
    "tecnico":                    3,
    "profesor normalista":        4,
    "pasante":                    5,
    "pasante/licenciatura trunca": 5,
    "licenciatura":               6,
    "especialidad":               7,
    "maestria":                   8,
    "doctorado":                  9,
}

# Columns always null/empty across all legislatures — drop for ML
DROP_COLUMNS = [
    "redes_sociales",       # 500/500 null every legislature
    "error",                # 500/500 null every legislature
    "profile_url",          # identifier, not a feature
    "numero_de_la_legislatura",   # redundant with legislatura_nombre
    "periodo_de_la_legislatura",  # redundant with legislatura_num
    "_source_file",         # internal load marker
]

# JSON trajectory/commission columns — handled by transform.py
JSON_COLUMNS = [
    "comisiones",
    "licencias_reincorporaciones",
    "trayectoria_administrativa",
    "trayectoria_legislativa",
    "trayectoria_politica",
    "trayectoria_academica",
    "trayectoria_empresarial",
    "otros_rubros",
    "organos_de_gobierno",
    "observaciones",
]


def _parse_nacimiento(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """DD/MM/YYYY → (birth_year int, date parsed). Returns (anio, fecha)."""
    fecha = pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")
    return fecha.dt.year, fecha


def _encode_grado(series: pd.Series) -> pd.Series:
    """Map ultimo_grado_de_estudios to ordinal int. Unknown → 0."""
    normalized = series.str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    return normalized.map(GRADO_ORDINAL).fillna(0).astype(int)


def _encode_principio(series: pd.Series) -> pd.Series:
    """
    Majority relative = 1, Proportional = 0, missing = -1.
    Binary feature for election mechanism.
    """
    mapping = {
        "mayoría relativa": 1,
        "mayoria relativa": 1,
        "representación proporcional": 0,
        "representacion proporcional": 0,
    }
    normalized = series.str.lower().str.strip()
    return normalized.map(mapping).fillna(-1).astype(int)


def _encode_partido(series: pd.Series) -> pd.Series:
    """Normalize party abbreviation: strip, uppercase, replace empty with DESCONOCIDO."""
    cleaned = series.str.strip().str.upper()
    return cleaned.fillna("DESCONOCIDO").replace("", "DESCONOCIDO")


def _presence_flag(series: pd.Series) -> pd.Series:
    """1 if value present (non-null, non-empty), else 0."""
    return series.notna().astype(int) & (series.str.strip() != "").astype(int)


def _text_length(series: pd.Series) -> pd.Series:
    """Word count of a text field. Missing → 0."""
    return series.fillna("").str.split().str.len().astype(int)


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply all cleaning steps to a raw legislature DataFrame.

    Input:  raw DataFrame from load.py (all str dtype)
    Output: cleaned DataFrame ready for transform.py
            - scalar columns typed correctly
            - JSON columns still as str (transform.py handles them)
            - diputado_id as index
    """
    df = df.copy()
    leg_name = df["legislatura_nombre"].iloc[0] if "legislatura_nombre" in df.columns else "?"

    # -- Drop useless columns (ignore if already missing) --
    dropped = [c for c in DROP_COLUMNS if c in df.columns]
    df.drop(columns=dropped, inplace=True)
    logger.info("[%s] Dropped %d redundant columns: %s", leg_name, len(dropped), dropped)

    # -- legislatura_num → int --
    df["legislatura_num"] = pd.to_numeric(df["legislatura_num"], errors="coerce").astype("Int64")

    # -- nacimiento → anio_nacimiento + edad_al_tomar_cargo --
    anio, _ = _parse_nacimiento(df["nacimiento"])
    df["anio_nacimiento"] = anio.astype("Int64")
    n_unparsed = df["anio_nacimiento"].isna().sum()
    if n_unparsed:
        logger.warning("[%s] nacimiento unparseable for %d / %d rows → anio_nacimiento=null",
                       leg_name, n_unparsed, len(df))
    else:
        logger.info("[%s] nacimiento parsed OK for all %d rows", leg_name, len(df))

    start_year = LEG_START_YEAR.get(leg_name)
    if start_year and df["anio_nacimiento"].notna().any():
        df["edad_al_tomar_cargo"] = (start_year - df["anio_nacimiento"]).astype("Int64")
        bad = (df["edad_al_tomar_cargo"] < 18) | (df["edad_al_tomar_cargo"] > 90)
        n_bad = bad.sum()
        if n_bad:
            logger.warning("[%s] %d ages outside [18,90] set to null", leg_name, n_bad)
        logger.info("[%s] edad_al_tomar_cargo: min=%s, max=%s, mean=%.1f",
                    leg_name,
                    df["edad_al_tomar_cargo"].min(),
                    df["edad_al_tomar_cargo"].max(),
                    df["edad_al_tomar_cargo"].mean())
    else:
        df["edad_al_tomar_cargo"] = pd.NA
        logger.warning("[%s] No start_year mapping — edad_al_tomar_cargo all null", leg_name)
    df.drop(columns=["nacimiento"], inplace=True)

    # -- Categorical encodings --
    df["grado_estudios_ord"] = _encode_grado(df.get("ultimo_grado_de_estudios", pd.Series(dtype=str)))
    unknown_grado = (df["grado_estudios_ord"] == 0).sum()
    logger.info("[%s] grado_estudios_ord: %d unknown (0), %d encoded",
                leg_name, unknown_grado, len(df) - unknown_grado)
    df.drop(columns=["ultimo_grado_de_estudios"], inplace=True)

    df["mayoria_relativa"] = _encode_principio(df.get("principio_de_eleccion", pd.Series(dtype=str)))
    vc = df["mayoria_relativa"].value_counts().to_dict()
    logger.info("[%s] mayoria_relativa: MR=%d, RP=%d, unknown=%d",
                leg_name, vc.get(1, 0), vc.get(0, 0), vc.get(-1, 0))
    df.drop(columns=["principio_de_eleccion"], inplace=True)

    df["partido"] = _encode_partido(df.get("partido", pd.Series(dtype=str)))
    parties = sorted(df["partido"].unique().tolist())
    logger.info("[%s] partido: %d unique — %s", leg_name, len(parties), parties)
    if "partido_nombre" in df.columns:
        df["partido_nombre"] = df["partido_nombre"].str.strip().fillna("DESCONOCIDO")

    # -- en_licencia → int --
    df["en_licencia"] = (
        df["en_licencia"]
        .map({"True": 1, "False": 0, "true": 1, "false": 0, "1": 1, "0": 0})
        .fillna(0)
        .astype(int)
    )
    n_licencia = df["en_licencia"].sum()
    logger.info("[%s] en_licencia: %d on leave, %d active", leg_name, n_licencia, len(df) - n_licencia)

    # -- suplente_referencia → int (0 = no suplente) --
    df["suplente_referencia"] = (
        pd.to_numeric(df.get("suplente_referencia", pd.Series(dtype=str)), errors="coerce")
        .fillna(0)
        .astype(int)
    )
    df["tiene_suplente"] = (df["suplente_referencia"] > 0).astype(int)
    logger.info("[%s] tiene_suplente: %d have substitute", leg_name, df["tiene_suplente"].sum())

    # -- referencia → int --
    df["referencia"] = pd.to_numeric(df["referencia"], errors="coerce").astype("Int64")

    # -- Contact presence flags --
    df["tiene_correo"] = _presence_flag(df.get("correo_electronico", pd.Series(dtype=str)))
    df["tiene_telefono"] = _presence_flag(df.get("telefono", pd.Series(dtype=str)))
    df["tiene_ubicacion"] = _presence_flag(df.get("ubicacion", pd.Series(dtype=str)))
    logger.info("[%s] contact flags: correo=%d, telefono=%d, ubicacion=%d",
                leg_name,
                df["tiene_correo"].sum(),
                df["tiene_telefono"].sum(),
                df["tiene_ubicacion"].sum())

    # -- Text length features (bag-of-words proxy) --
    df["n_palabras_preparacion"] = _text_length(df.get("preparacion_academica", pd.Series(dtype=str)))
    df["n_palabras_exp_legislativa"] = _text_length(df.get("experiencia_legislativa", pd.Series(dtype=str)))
    logger.debug("[%s] n_palabras_preparacion: mean=%.1f | n_palabras_exp_legislativa: mean=%.1f",
                 leg_name,
                 df["n_palabras_preparacion"].mean(),
                 df["n_palabras_exp_legislativa"].mean())

    # -- Geo: normalize entidad/ciudad, fill missing --
    for col in ["entidad", "ciudad"]:
        if col in df.columns:
            n_missing = df[col].isna().sum() + (df[col].str.strip() == "").sum()
            df[col] = df[col].str.strip().str.title().fillna("Desconocido").replace("", "Desconocido")
            if n_missing:
                logger.debug("[%s] %s: filled %d missing → 'Desconocido'", leg_name, col, n_missing)

    # -- region_de_eleccion: strip, fill missing --
    if "region_de_eleccion" in df.columns:
        df["region_de_eleccion"] = df["region_de_eleccion"].str.strip().fillna("DESCONOCIDO")

    # -- Drop raw contact columns (keep flags only) --
    df.drop(columns=[c for c in ["correo_electronico", "telefono", "ubicacion",
                                  "preparacion_academica", "experiencia_legislativa",
                                  "suplente"] if c in df.columns], inplace=True)

    # -- Set diputado_id as index --
    df.set_index("diputado_id", inplace=True)

    remaining_nulls = df.isnull().sum().sum()
    logger.info("[%s] clean done → %d rows × %d cols | remaining nulls: %d",
                leg_name, len(df), len(df.columns), remaining_nulls)
    return df
