"""
save.py — Escritura de datos procesados al disco.

FUNCIÓN EN EL PIPELINE:
    Es el último paso del ETL. Recibe el DataFrame ya limpio y transformado
    y lo guarda como CSV en el directorio de la corrida actual.

CONEXIONES:
    Entrada : pd.DataFrame procesado por transform.py → transform()
              str run_dir  — directorio de la corrida (creado en pipeline.py)
    Salida  : data/etl/<run_ts>/<LEGISLATURA>_<processed_ts>.csv

NOMENCLATURA DE ARCHIVOS:
    - run_ts      : timestamp del inicio de la corrida (mismo para todas las
                    legislaturas de un mismo pipeline.py).  Lo genera pipeline.py.
    - processed_ts: timestamp del momento exacto en que se guardó este archivo.
                    Permite distinguir archivos dentro de la misma corrida.
"""

import logging
import os
from datetime import datetime

import pandas as pd

# Logger del módulo — mensajes con prefijo "etl.save".
logger = logging.getLogger(__name__)

# Directorio base de salida del ETL, relativo a la raíz del proyecto.
# pipeline.py lo importa para construir el run_dir de cada corrida.
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data", "etl")


def save_legislature(df: pd.DataFrame, leg_name: str, run_dir: str) -> str:
    """
    Guarda el DataFrame procesado en un CSV dentro del directorio de la corrida.

    El archivo incluye el índice (diputado_id) como primera columna para
    facilitar su uso posterior sin necesidad de reindexar.

    Parámetros
    ----------
    df       : pd.DataFrame — datos ya limpios y transformados por el ETL.
    leg_name : str — nombre de la legislatura (p. ej. "LXVI"), usado en el
               nombre del archivo.
    run_dir  : str — ruta al directorio de la corrida actual (data/etl/<run_ts>/).
               pipeline.py crea este directorio y lo pasa a esta función.

    Retorna
    -------
    str : ruta absoluta del archivo CSV creado.
    """
    # Timestamp del momento de guardado, independiente del inicio de la corrida.
    # Permite saber exactamente cuándo terminó de procesarse cada legislatura.
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Crear el directorio de la corrida si no existe aún.
    os.makedirs(run_dir, exist_ok=True)

    out_path = os.path.join(run_dir, f"{leg_name}_{ts}.csv")

    # index=True escribe diputado_id como primera columna del CSV.
    df.to_csv(out_path, index=True, encoding="utf-8")

    file_kb = os.path.getsize(out_path) / 1024
    logger.info(
        "[%s] Guardado: %d filas × %d columnas → %s (%.1f KB)",
        leg_name, len(df), len(df.columns), out_path, file_kb,
    )
    return out_path
