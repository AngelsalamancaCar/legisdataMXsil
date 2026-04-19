#!/usr/bin/env python3
"""
pipeline.py — Orquestador del ETL.

FUNCIÓN EN EL PROYECTO:
    Coordina los cuatro pasos del ETL en orden:
      load → clean → transform → save

    Es el único punto de entrada para ejecutar el procesamiento de datos.
    El scraper (scraper.py) se ejecuta por separado antes de este script.

CONEXIONES CON OTROS MÓDULOS:
    Lee de  : etl/load.py    → load_legislature()  (datos crudos del scraper)
    Usa     : etl/clean.py   → clean()              (limpieza y normalización)
    Usa     : etl/transform.py → transform()        (extracción de JSON)
    Escribe : etl/save.py    → save_legislature()   (CSV procesado al disco)

ESTRUCTURA DE DIRECTORIOS:
    Cada ejecución del pipeline crea un subdirectorio con timestamp dentro
    de data/etl/, de modo que múltiples corridas no se sobreescriben.

    data/etl/<run_ts>/
        <LEGISLATURA>_<processed_ts>.csv   ← una por legislatura procesada
        etl.log                            ← log completo de la corrida

USO:
    python pipeline.py --legislatura LXVI
    python pipeline.py --legislatura LXIV,LXV,LXVI
    python pipeline.py --legislatura all
    python pipeline.py --legislatura all --input-dir data/scraper/20260418_140000/
"""

import argparse
import logging
import os
import sys
from datetime import datetime

# Importaciones del paquete ETL — cada módulo hace una sola cosa.
from etl.load import load_legislature, load_all, LEGISLATURAS, latest_scraper_run
from etl.clean import clean
from etl.transform import transform
from etl.save import save_legislature, DATA_DIR

# Formato unificado de log para consola y archivo.
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def setup_logging(run_dir: str, verbose: bool = False) -> None:
    """
    Configura el sistema de logging para la corrida.

    Escribe simultáneamente a:
      - Consola (stdout): nivel INFO por defecto, DEBUG si --verbose.
      - Archivo etl.log dentro de run_dir: siempre nivel DEBUG (registro completo).

    Se llama una sola vez al inicio de main() con el run_dir ya creado,
    antes de procesar cualquier legislatura.

    Parámetros
    ----------
    run_dir : str — directorio de la corrida actual (data/etl/<run_ts>/).
    verbose : bool — si True, muestra mensajes DEBUG en consola.
    """
    # El directorio debe existir antes de abrir el FileHandler.
    os.makedirs(run_dir, exist_ok=True)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)  # capturar todo; los handlers filtran por nivel

    # Handler de consola: para seguimiento en tiempo real durante la ejecución.
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(logging.Formatter(LOG_FORMAT))

    # Handler de archivo: registro permanente con máximo detalle.
    log_file = os.path.join(run_dir, "etl.log")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

    root.addHandler(console)
    root.addHandler(file_handler)


# Logger de este módulo — los mensajes aparecen con prefijo "__main__".
logger = logging.getLogger(__name__)


def run_one(leg_name: str, run_dir: str, raw_dir: str) -> None:
    """
    Ejecuta el pipeline completo para una sola legislatura.

    Encadena los cuatro pasos del ETL en orden:
      1. load_legislature() → lee el CSV crudo del scraper
      2. clean()            → limpia y normaliza tipos
      3. transform()        → desanida columnas JSON
      4. save_legislature() → escribe el CSV procesado

    Parámetros
    ----------
    leg_name : str — nombre romano de la legislatura (p. ej. "LXVI").
    run_dir  : str — directorio de esta corrida del pipeline (data/etl/<run_ts>/).
    raw_dir  : str — directorio de la corrida del scraper con los datos crudos.
    """
    logger.info("═" * 50)
    logger.info("INICIO %s", leg_name)

    # Paso 1: cargar datos crudos desde la corrida del scraper indicada.
    raw = load_legislature(leg_name, raw_dir)

    # Paso 2: limpiar y normalizar (tipos, categorías, flags, etc.).
    cleaned = clean(raw)

    # Paso 3: extraer características de columnas JSON (comisiones, trayectorias).
    processed = transform(cleaned)

    # Paso 4: guardar el resultado en data/etl/<run_ts>/<leg>_<ts>.csv.
    save_legislature(processed, leg_name, run_dir)

    logger.info("FIN   %s", leg_name)


def resolve_legislaturas(arg: str) -> list[str]:
    """
    Convierte el argumento --legislatura en una lista de nombres de legislatura.

    Acepta:
      "all"              → todas las legislaturas en orden cronológico
      "LXVI"             → una sola legislatura
      "LXIV,LXV,LXVI"   → varias separadas por coma

    Lanza SystemExit con mensaje descriptivo si algún nombre no es válido.
    """
    if arg.strip().lower() == "all":
        return list(LEGISLATURAS)

    names = [n.strip().upper() for n in arg.split(",")]
    invalid = [n for n in names if n not in LEGISLATURAS]
    if invalid:
        raise SystemExit(
            f"Legislatura(s) desconocida(s): {', '.join(invalid)}\n"
            f"Válidas: {', '.join(LEGISLATURAS)}"
        )
    return names


def main() -> None:
    """
    Punto de entrada principal del pipeline.

    Flujo:
      1. Parsear argumentos de línea de comandos.
      2. Determinar el directorio de entrada del scraper (auto-detectar o explícito).
      3. Crear el directorio de la corrida con timestamp (data/etl/<run_ts>/).
      4. Configurar logging (consola + archivo).
      5. Iterar sobre las legislaturas objetivo y llamar a run_one() para cada una.
      6. Reportar resumen y salir con código de error si alguna falló.
    """
    parser = argparse.ArgumentParser(description="Pipeline ETL para legisdatamxsil")
    parser.add_argument(
        "--legislatura",
        required=True,
        metavar="NOMBRE",
        help="Legislatura(s) a procesar. Ej: LXVI | LXIV,LXV | all",
    )
    parser.add_argument(
        "--input-dir",
        metavar="RUTA",
        default=None,
        help=(
            "Directorio de corrida del scraper del que leer los datos crudos. "
            "Si se omite, se usa automáticamente la corrida más reciente de data/scraper/."
        ),
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Mostrar mensajes DEBUG en consola (siempre se escriben en el archivo de log).",
    )
    args = parser.parse_args()

    # Determinar el directorio de datos crudos del scraper.
    # Si el usuario especificó --input-dir, usarlo; de lo contrario, auto-detectar.
    raw_dir = args.input_dir if args.input_dir else latest_scraper_run()

    # Crear el directorio de salida de esta corrida con un timestamp único.
    # Todas las legislaturas procesadas en esta invocación comparten el mismo run_ts,
    # lo que permite agrupar sus archivos de salida en un solo directorio.
    run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(DATA_DIR, run_ts)

    # Configurar logging antes de cualquier mensaje para que quede registrado.
    setup_logging(run_dir, verbose=args.verbose)
    logger.info("Directorio de entrada (scraper): %s", os.path.abspath(raw_dir))
    logger.info("Directorio de salida  (ETL)    : %s", os.path.abspath(run_dir))

    targets = resolve_legislaturas(args.legislatura)
    errors = []

    for leg in targets:
        try:
            run_one(leg, run_dir, raw_dir)
        except FileNotFoundError as e:
            # La legislatura no existe en los datos del scraper — omitir sin detener el proceso.
            logger.warning("OMITIR %s — %s", leg, e)
            errors.append(leg)
        except Exception as e:
            # Error inesperado — registrar y relanzar para detener el pipeline.
            logger.error("FALLO %s — %s", leg, e)
            errors.append(leg)
            raise

    # Resumen final de la corrida.
    logger.info("═" * 50)
    logger.info(
        "Terminado. %d procesadas, %d omitidas/fallidas.",
        len(targets) - len(errors), len(errors),
    )
    logger.info("Log completo → %s", os.path.join(os.path.abspath(run_dir), "etl.log"))

    if errors:
        logger.warning("Omitidas: %s", errors)
        sys.exit(1)  # código de salida no-cero para señalar error en scripts externos


def etl_all() -> None:
    """Entry point para `uv run etl-all` — corre el ETL en todas las legislaturas."""
    sys.argv = ["pipeline.py", "--legislatura", "all"]
    main()


if __name__ == "__main__":
    main()
