"""
normalize.py — Eliminación del desbordamiento de secciones en columnas de trayectoria.

FUNCIÓN EN EL PIPELINE:
    Paso 1.5 del ETL (entre load.py y clean.py).
    Recibe el DataFrame crudo de load.py y devuelve el mismo DataFrame con las
    columnas de trayectoria corregidas.

PROBLEMA QUE RESUELVE:
    El scraper parsea el perfil de cada diputado de arriba a abajo y, al
    encontrar una sección nueva, inserta un encabezado centinela y continúa
    añadiendo entradas en la columna activa. El resultado es que cada columna
    acumula las secciones que la siguen:

      trayectoria_administrativa  →  admin + [LEG] + leg + [POL] + pol + ...
      trayectoria_legislativa     →  leg   + [POL] + pol + [ACAD] + acad + ...
      trayectoria_politica        →  pol   + [ACAD] + acad + [EMP] + emp + ...
      trayectoria_academica       →  acad  + [EMP]  + emp + [OTROS] + otros + ...
      trayectoria_empresarial     →  emp   + [OTROS] + otros + [INV] + inv
      otros_rubros                →  otros + [INV]  + inv

    Los encabezados centinela tienen la forma {"Del año": "TRAYECTORIA POLÍTICA",
    "Al año": "", "Experiencia": ""} — texto en mayúsculas en el campo "Del año".

    Las entradas propias de cada columna son las que preceden al primer centinela.
    Todo lo que sigue es duplicado de columnas posteriores (verificado: 0 casos
    de datos exclusivos en el desbordamiento que no existan en la columna dedicada).

LXVI — DIFERENCIAS DE FORMATO:
    - "TRAYECTORIA ACADÉMICA" → "ESCOLARIDAD Y PREPARACIÓN ACADÉMICA"
    - Nueva sección "INVESTIGACIÓN Y DOCENCIA" (sin columna dedicada en el scraper)
      que aparece como cola de 'otros_rubros'.

SALIDA:
    - Cada columna de trayectoria queda con solo sus entradas propias (lista JSON).
    - Nueva columna 'investigacion_docencia' con entradas de esa sección (LXVI).
    - 'organos_de_gobierno' y 'observaciones' no se tocan (sin desbordamiento).
"""

import json
import logging

import pandas as pd

logger = logging.getLogger(__name__)

# Todos los textos de encabezado centinela encontrados en cualquier legislatura.
# Añadir aquí si el scraper introduce nuevos nombres de sección en el futuro.
_SENTINEL_HEADERS: frozenset[str] = frozenset({
    "TRAYECTORIA LEGISLATIVA",
    "TRAYECTORIA POLÍTICA",
    "TRAYECTORIA ACADÉMICA",
    "ESCOLARIDAD Y PREPARACIÓN ACADÉMICA",        # LXVI: renombre de ACADÉMICA
    "TRAYECTORIA EMPRESARIAL/INICIATIVA PRIVADA",
    "OTROS RUBROS",
    "INVESTIGACIÓN Y DOCENCIA",                   # LXVI: sección nueva
})

# Columnas afectadas por el desbordamiento, en orden de cascada.
# 'organos_de_gobierno' y 'observaciones' quedan fuera: tienen esquema diferente
# y no presentan el problema.
_TRAY_COLS: tuple[str, ...] = (
    "trayectoria_administrativa",
    "trayectoria_legislativa",
    "trayectoria_politica",
    "trayectoria_academica",
    "trayectoria_empresarial",
    "otros_rubros",
)


def _parse_cell(cell) -> list:
    if not isinstance(cell, str) or not cell.strip() or cell.strip() == "[]":
        return []
    try:
        result = json.loads(cell)
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _split(entries: list) -> tuple[list, dict[str, list]]:
    """
    Separa las entradas propias de la columna de las secciones en cascada.

    Retorna:
        own      — entradas antes del primer centinela (las que pertenecen a esta columna)
        sections — {texto_centinela: [entradas_de_esa_sección]}
    """
    own: list = []
    sections: dict[str, list] = {}
    current: str | None = None

    for entry in entries:
        anio = entry.get("Del año", "").strip()
        if anio in _SENTINEL_HEADERS:
            current = anio
            if current not in sections:
                sections[current] = []
        elif current is None:
            own.append(entry)
        else:
            sections[current].append(entry)

    return own, sections


def _normalize_row(row: pd.Series, cols_present: tuple[str, ...]) -> dict[str, str]:
    result: dict[str, str] = {}
    inv_entries: list = []

    for col in cols_present:
        own, sections = _split(_parse_cell(row.get(col, "")))
        result[col] = json.dumps(own, ensure_ascii=False)
        if col == "otros_rubros":
            inv_entries = sections.get("INVESTIGACIÓN Y DOCENCIA", [])

    result["investigacion_docencia"] = json.dumps(inv_entries, ensure_ascii=False)
    return result


def normalize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina el desbordamiento de secciones en todas las columnas de trayectoria.

    Para cada columna en _TRAY_COLS, conserva solo las entradas que le
    corresponden (anteriores al primer centinela). Las entradas duplicadas
    en columnas posteriores se descartan.

    Introduce la columna 'investigacion_docencia' extraída del final de
    'otros_rubros' (presente en LXVI; vacía en legislaturas anteriores).

    Parámetros
    ----------
    df : pd.DataFrame — salida de load_legislature() (columnas JSON como str).

    Retorna
    -------
    pd.DataFrame con columnas de trayectoria limpias e 'investigacion_docencia'
    añadida. Listo para pasar a clean().
    """
    df = df.copy()
    leg_name = str(df["legislatura_num"].iloc[0]) if "legislatura_num" in df.columns else "?"
    cols_present = tuple(c for c in _TRAY_COLS if c in df.columns)

    normalized = df.apply(
        lambda row: _normalize_row(row, cols_present),
        axis=1,
        result_type="expand",
    )

    for col in cols_present:
        df[col] = normalized[col]
    df["investigacion_docencia"] = normalized["investigacion_docencia"]

    # Conteos de diagnóstico para el log
    stats = {
        col: (normalized[col] != "[]").sum()
        for col in cols_present
    }
    n_inv = (normalized["investigacion_docencia"] != "[]").sum()
    logger.info(
        "[%s] normalize: admin=%d leg=%d pol=%d acad=%d emp=%d otros=%d inv_doc=%d",
        leg_name,
        stats.get("trayectoria_administrativa", 0),
        stats.get("trayectoria_legislativa", 0),
        stats.get("trayectoria_politica", 0),
        stats.get("trayectoria_academica", 0),
        stats.get("trayectoria_empresarial", 0),
        stats.get("otros_rubros", 0),
        n_inv,
    )
    return df
