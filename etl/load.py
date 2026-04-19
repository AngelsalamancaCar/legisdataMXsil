"""
load.py — Lectura de datos crudos del scraper.

FUNCIÓN EN EL PIPELINE:
    Es el primer paso del ETL. Lee los CSV que produjo scraper.py y los
    entrega como DataFrames a clean.py para su limpieza.

CONEXIONES:
    Entrada : data/scraper/<run_ts>/<LEGISLATURA>.csv  (producido por scraper.py)
    Salida  : pd.DataFrame con todas las columnas crudas (dtype=str)
              → recibido por clean.py → clean()

DETECCIÓN AUTOMÁTICA:
    Si no se especifica un directorio de entrada, busca el run más reciente
    dentro de data/scraper/ ordenando los subdirectorios por nombre
    (formato YYYYMMDD_HHMMSS, por lo que el más reciente queda al final al
    ordenar de mayor a menor).
"""

import logging
import os

import pandas as pd

# Logger del módulo — los mensajes aparecen con el prefijo "etl.load"
# en la consola y en el archivo etl.log dentro del directorio de la corrida.
logger = logging.getLogger(__name__)

# Ruta base donde el scraper deposita sus corridas con timestamp.
# __file__ apunta a etl/load.py → ".." sube a la raíz del proyecto.
_SCRAPER_BASE = os.path.join(os.path.dirname(__file__), "..", "data", "scraper")

# Nombres romanos de todas las legislaturas soportadas, en orden cronológico.
# Se usa para validar el argumento --legislatura y para cargar "all".
LEGISLATURAS = [
    "LVII",
    "LVIII",
    "LIX",
    "LX",
    "LXI",
    "LXII",
    "LXIII",
    "LXIV",
    "LXV",
    "LXVI",
]


def latest_scraper_run() -> str:
    """
    Devuelve la ruta al directorio de la corrida más reciente del scraper.

    Los directorios tienen el formato YYYYMMDD_HHMMSS, así que ordenarlos
    de mayor a menor y tomar el primero equivale a tomar el más reciente.

    Lanza FileNotFoundError si no existe ninguna corrida previa.
    """
    # Verificar que el directorio base exista antes de intentar listar su contenido.
    if not os.path.isdir(_SCRAPER_BASE):
        raise FileNotFoundError(
            f"No se encontró salida del scraper en {_SCRAPER_BASE}. "
            "Ejecuta scraper.py primero."
        )

    # Filtrar solo subdirectorios (descartar posibles archivos sueltos).
    runs = sorted(
        (
            d
            for d in os.listdir(_SCRAPER_BASE)
            if os.path.isdir(os.path.join(_SCRAPER_BASE, d))
        ),
        reverse=True,  # más reciente primero
    )

    if not runs:
        # Sin subdirectorios: los CSV están directamente en data/scraper/ (formato legacy).
        csvs = [f for f in os.listdir(_SCRAPER_BASE) if f.endswith(".csv")]
        if csvs:
            return _SCRAPER_BASE
        raise FileNotFoundError(
            f"No hay corridas en {_SCRAPER_BASE}. Ejecuta scraper.py primero."
        )

    return os.path.join(_SCRAPER_BASE, runs[0])


def load_legislature(leg_name: str, raw_dir: str | None = None) -> pd.DataFrame:
    """
    Carga el CSV crudo de una legislatura y lo devuelve como DataFrame.

    Parámetros
    ----------
    leg_name : str
        Nombre romano de la legislatura (p. ej. "LXVI").
    raw_dir : str | None
        Directorio donde buscar el CSV. Si es None, se auto-detecta la
        corrida más reciente del scraper.

    Retorna
    -------
    pd.DataFrame
        Todas las columnas como str (dtype=str). clean.py se encarga de
        convertir tipos y normalizar valores.
        Incluye la columna auxiliar "_source_file" con el nombre de la
        legislatura, usada por clean.py para identificar el origen.

    Excepciones
    -----------
    ValueError          : si leg_name no es una legislatura válida.
    FileNotFoundError   : si el CSV no existe en el directorio indicado.
    """
    leg_name = leg_name.upper()

    # Validar que la legislatura esté en el catálogo antes de buscar el archivo.
    if leg_name not in LEGISLATURAS:
        raise ValueError(
            f"Legislatura desconocida: {leg_name}. Válidas: {LEGISLATURAS}"
        )

    # Si no se proporcionó directorio, usar la corrida más reciente del scraper.
    if raw_dir is None:
        raw_dir = latest_scraper_run()
        logger.info("Corrida del scraper detectada automáticamente: %s", raw_dir)

    path = os.path.join(raw_dir, f"{leg_name}.csv")

    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Datos crudos no encontrados: {path}. Ejecuta scraper.py primero."
        )

    file_kb = os.path.getsize(path) / 1024
    logger.info("Leyendo %s (%.1f KB)", path, file_kb)

    # dtype=str: se leen todas las columnas como texto para no perder
    # información (fechas, IDs con ceros iniciales, etc.).
    # clean.py convierte los tipos correctos en su etapa.
    df = pd.read_csv(path, dtype=str)

    # Columna auxiliar que indica de qué archivo proviene cada fila.
    # clean.py la elimina después de usarla para el logging.
    df["_source_file"] = leg_name

    # Diagnóstico de nulos para detectar columnas problemáticas en el scraper.
    null_counts = df.isnull().sum()
    total_nulls = null_counts.sum()
    logger.info(
        "Cargado: %d filas × %d columnas | nulos totales: %d",
        len(df),
        len(df.columns),
        total_nulls,
    )
    if total_nulls:
        # Mostrar solo las 5 columnas con más nulos para no saturar el log.
        top_nulls = null_counts[null_counts > 0].sort_values(ascending=False).head(5)
        for col, n in top_nulls.items():
            logger.debug("  nulo %-35s %d / %d filas", col, n, len(df))

    return df


def load_all(raw_dir: str | None = None) -> dict[str, pd.DataFrame]:
    """
    Carga todas las legislaturas disponibles en el directorio indicado.

    Las legislaturas cuyos CSV no existen se omiten con una advertencia
    (no se lanza excepción), permitiendo procesar corridas parciales del
    scraper sin interrumpir el pipeline.

    Retorna
    -------
    dict[str, pd.DataFrame]
        Diccionario {nombre_legislatura: DataFrame}.
        Solo contiene las legislaturas que se pudieron cargar.
    """
    # Auto-detectar directorio una sola vez para no repetir la búsqueda
    # por cada legislatura en el bucle.
    if raw_dir is None:
        raw_dir = latest_scraper_run()
        logger.info("Corrida del scraper detectada automáticamente: %s", raw_dir)

    result = {}
    for leg in LEGISLATURAS:
        try:
            result[leg] = load_legislature(leg, raw_dir)
        except FileNotFoundError as e:
            # Omitir legislaturas faltantes sin detener el proceso.
            logger.warning("OMITIR %s — %s", leg, e)
    return result
