"""
clean.py — Limpieza y normalización de datos crudos.

FUNCIÓN EN EL PIPELINE:
    Segundo paso del ETL (después de load.py, antes de transform.py).
    Recibe un DataFrame con todas las columnas como str y devuelve un
    DataFrame con tipos correctos, categorías codificadas numéricamente
    y valores faltantes rellenados con defaults seguros para ML.

CONEXIONES:
    Entrada : pd.DataFrame crudo de load.py → load_legislature()
              (todas las columnas son str, incluyendo las de JSON)
    Salida  : pd.DataFrame limpio → recibido por transform.py → transform()
              - columnas escalares con tipos correctos
              - columnas JSON todavía como str (transform.py las procesa)
              - índice = diputado_id

QUÉ SE HACE AQUÍ vs. EN transform.py:
    clean.py  → columnas escalares: fechas, categorías, flags, texto.
    transform.py → columnas JSON: comisiones, trayectorias (requieren
                   parseo de listas anidadas y extracción de conteos).
"""

import logging
import re

import numpy as np
import pandas as pd

# Logger del módulo — mensajes con prefijo "etl.clean".
logger = logging.getLogger(__name__)

# Año de inicio de cada legislatura, usado para calcular la edad del
# legislador al momento de tomar posesión del cargo.
LEG_START_YEAR: dict[str, int] = {
    "LVII": 1997,
    "LVIII": 2000,
    "LIX": 2003,
    "LX": 2006,
    "LXI": 2009,
    "LXII": 2012,
    "LXIII": 2015,
    "LXIV": 2018,
    "LXV": 2021,
    "LXVI": 2024,
}

# Codificación ordinal del último grado de estudios.
# 0 = desconocido/faltante; valores más altos = mayor escolaridad.
# Esta escala permite usar la columna como variable numérica en modelos ML.
GRADO_ORDINAL: dict[str, int] = {
    "no disponible": 0,
    "secundaria": 1,
    "preparatoria": 2,
    "tecnico": 3,
    "profesor normalista": 4,
    "pasante": 5,
    "pasante/licenciatura trunca": 5,
    "licenciatura": 6,
    "especialidad": 7,
    "maestria": 8,
    "doctorado": 9,
}

# Columnas que se eliminan porque son siempre nulas, redundantes o no son
# características útiles para el análisis (solo identificadores o metadatos).
DROP_COLUMNS = [
    "redes_sociales",  # nula en el 100% de los registros de todas las legislaturas
    "error",  # nula en el 100% de los registros (se llenó solo si el scraper falló)
    "profile_url",  # URL del perfil en el SIL — identificador, no característica
    "numero_de_la_legislatura",  # redundante con legislatura_num
    "periodo_de_la_legislatura",  # redundante con legislatura_num
    "_source_file",  # columna auxiliar creada por load.py, sin valor analítico
    "licencias_reincorporaciones",  # eliminada del análisis
    "entidad",
    "ciudad",
    "ubicacion",
    "correo_electronico",
    "telefono",
    "redes_sociales",
]

# Columnas que contienen listas JSON serializadas.
# clean.py las pasa intactas a transform.py, que sabe cómo desanidarlas.
JSON_COLUMNS = [
    "comisiones",
    "trayectoria_administrativa",
    "trayectoria_legislativa",
    "trayectoria_politica",
    "trayectoria_academica",
    "trayectoria_empresarial",
    "otros_rubros",
    "investigacion_docencia",   # extraída de otros_rubros por normalize.py (LXVI+)
    "organos_de_gobierno",
    "observaciones",
]

# ---------------------------------------------------------------------------
# Funciones auxiliares de transformación
# Cada función opera sobre una Series y devuelve una Series transformada.
# Se definen como funciones separadas para facilitar las pruebas unitarias.
# ---------------------------------------------------------------------------


def _parse_nacimiento(series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Convierte la columna 'nacimiento' (formato DD/MM/YYYY) en año de nacimiento.

    Retorna una tupla (yyyy: Series[Int], fecha: Series[datetime]) para
    permitir que clean() calcule edad_al_tomar_cargo a partir del año.
    Los valores que no se puedan parsear quedan como NaT/NA.
    """
    fecha = pd.to_datetime(series, format="%d/%m/%Y", errors="coerce")
    return fecha.dt.year, fecha


def _encode_grado(series: pd.Series) -> pd.Series:
    """
    Mapea 'ultimo_grado_de_estudios' (texto) a un entero ordinal (0–9).

    Normaliza el texto antes del mapeo (minúsculas, espacios colapsados)
    para manejar variaciones de escritura del sitio SIL.
    Valores no reconocidos quedan como 0 (desconocido).
    """
    normalized = series.str.lower().str.strip().str.replace(r"\s+", " ", regex=True)
    return normalized.map(GRADO_ORDINAL).fillna(0).astype(int)


def _encode_principio(series: pd.Series) -> pd.Series:
    """
    Codifica el principio de elección como variable binaria extendida:
      1  = mayoría relativa
      0  = representación proporcional
     -1  = desconocido/faltante

    El -1 distingue los faltantes de la categoría "proporcional" (0),
    lo que permite al modelo tratar los datos faltantes explícitamente.
    """
    mapping = {
        "mayoría relativa": 1,
        "mayoria relativa": 1,  # variante sin acento
        "representación proporcional": 0,
        "representacion proporcional": 0,  # variante sin acento
    }
    normalized = series.str.lower().str.strip()
    return normalized.map(mapping).fillna(-1).astype(int)


PARTIDO_ALIAS: dict[str, str] = {
    # Variantes que requieren renombrado (no son identidad tras .upper())
    "IND.": "INDEPENDIENTE",
    # Partidos históricos y actuales — identidad tras .upper(), listados
    # explícitamente para documentar el universo conocido y facilitar futuras
    # correcciones sin tener que buscar en los CSVs crudos.
    "PANAL": "PANAL",  # Nueva Alianza (LX–LXIII)
    "MORENA": "MORENA",  # Movimiento Regeneración Nacional (LXII–)
    "SIN PARTIDO": "SIN PARTIDO",  # Sin afiliación declarada (LXIII–LXV)
    "CONVERGENCIA": "CONVERGENCIA",  # Convergencia (LIX–LX); no se colapsa con MC
    "MC": "MC",  # Movimiento Ciudadano (LXI–)
    "CD-PPN": "CD-PPN",  # Convergencia Democrática/PPN (LVIII)
    "PAS": "PAS",  # Partido Alianza Social (LVIII)
    "PASC": "PASC",  # Alternativa Socialdemócrata y Campesina (LX)
    "PSN": "PSN",  # Partido de la Sociedad Nacionalista (LVIII)
    "PES": "PES",  # Partido Encuentro Social (LXIII–LXIV)
    "PAN": "PAN",
    "PRD": "PRD",
    "PRI": "PRI",
    "PT": "PT",
    "PVEM": "PVEM",
}


def _encode_partido(series: pd.Series) -> pd.Series:
    """
    Normaliza la abreviación del partido político usando PARTIDO_ALIAS.

    Pasos:
      1. Strip + upper (maneja variantes de casing: 'Morena', 'Panal', etc.)
      2. map() contra PARTIDO_ALIAS (renombra 'IND.' → 'INDEPENDIENTE', etc.)
      3. Valores no reconocidos conservan la forma en mayúsculas (forward-compat).
      4. Nulos y vacíos → 'DESCONOCIDO'.
    """
    cleaned = series.str.strip().str.upper()
    mapped = cleaned.map(PARTIDO_ALIAS)
    return mapped.fillna(cleaned).fillna("DESCONOCIDO").replace("", "DESCONOCIDO")


# Mapping de preparacion_academica: valor crudo → categoría canónica.
#
# Principios de agrupación aplicados:
#   - Derecho absorbe Humanidades (Historia, Filosofía, Literatura) y Ciencias
#     Jurídicas: disciplinas hermanas en la tradición académica mexicana.
#   - Administración Pública se agrupa con Ciencias Políticas y Sociales por su
#     naturaleza político-institucional, no empresarial.
#   - Administración genérica y Administración de Empresas → Administración y
#     Contaduría (categoría SIL de mayor frecuencia y más representativa).
#   - Biología y Química → Ciencias (no Salud: no son carreras clínicas).
#   - Economía pura → Económico-Financiera; combinaciones con gobierno/política
#     → Ciencias Políticas y Sociales.
#   - Educación y Pedagogía forman categoría propia por su relevancia política.
#   - Valores sin mapeo explícito → "Otra" (catch-all).
PREPARACION_ACADEMICA_MAPPING: dict[str, str] = {
    # ── Derecho (incluye Humanidades y Ciencias Jurídicas) ────────────────
    "Derecho": "Derecho",
    "Abogado": "Derecho",
    "Derecho Constitucional": "Derecho",
    "Derecho Constitucional y Amparo": "Derecho",
    "Derecho Constitucional Mexicano": "Derecho",
    "Derecho Constitucional y Gobernabilidad": "Derecho",
    "Derecho Procesal Constitucional": "Derecho",
    "Derecho con Orientación en Derecho de Amparo": "Derecho",
    "Derecho Fiscal": "Derecho",
    "Derecho Electoral": "Derecho",
    "Derecho Parlamentario": "Derecho",
    "Derecho Administrativo y Fiscal": "Derecho",
    "Derecho Administrativo Sancionador Electoral": "Derecho",
    "Derecho Procesal Penal": "Derecho",
    "Derecho Procesal": "Derecho",
    "Derecho Laboral": "Derecho",
    "Derecho Penal": "Derecho",
    "Derecho Internacional": "Derecho",
    "Derecho Internacional y Finanzas Internacionales": "Derecho",
    "Derecho Económico Internacional": "Derecho",
    "Derecho Comparado": "Derecho",
    "Derecho Público": "Derecho",
    "Derecho y Asuntos Internacionales": "Derecho",
    "Derecho y Ciencias Sociales": "Derecho",
    "Derecho y Finanzas": "Derecho",
    "Ciencias Jurídicas": "Derecho",
    "Ciencias Jurídico Penales": "Derecho",
    "Ciencias Penales": "Derecho",
    "Ciencias Jurídicas, Administrativas y de la Educación": "Derecho",
    "Ciencias de lo Fiscal": "Derecho",
    "Juicios Orales": "Derecho",
    "Métodos Alternos de Solución de Controversias": "Derecho",
    "Mediación y Solución Colaborativa de Conflictos": "Derecho",
    "Política Criminal": "Derecho",
    "Procuración y Administración de Justicia": "Derecho",
    # Humanidades agrupadas en Derecho (disciplinas hermanas en facultades mexicanas)
    "Humanidades": "Derecho",
    "Historia": "Derecho",
    "Historia del Arte": "Derecho",
    "Literatura Española": "Derecho",
    "Filosofía": "Derecho",
    "Escritura": "Derecho",
    "Estudios de las Mujeres, Género y Ciudadanía": "Derecho",
    # ── Administración y Contaduría ───────────────────────────────────────
    # Incluye Administración genérica, Administración de Empresas y todas las
    # carreras de contabilidad, finanzas, mercadotecnia y dirección empresarial.
    "Administración y Contaduría": "Administración y Contaduría",
    "Administración": "Administración y Contaduría",
    "Administración de Empresas": "Administración y Contaduría",
    "Administración de Negocios": "Administración y Contaduría",
    "Administración de Negocios Internacionales": "Administración y Contaduría",
    "Administración Estratégica": "Administración y Contaduría",
    "Administración Internacional": "Administración y Contaduría",
    "Administración de Recursos Humanos": "Administración y Contaduría",
    "Administración Área Personal": "Administración y Contaduría",
    "Administración Naval": "Administración y Contaduría",
    "Administración Militar para la Seguridad y Defensa Nacional": "Administración y Contaduría",
    "Administración de Proyectos de Inversión": "Administración y Contaduría",
    "Administración y Mercadotecnia": "Administración y Contaduría",
    "Alta Dirección": "Administración y Contaduría",
    "Alta Dirección de Empresas": "Administración y Contaduría",
    "Contaduría": "Administración y Contaduría",
    "Contaduría Pública": "Administración y Contaduría",
    "Contaduría Administrativa": "Administración y Contaduría",
    "Contaduría Pública y Finanzas": "Administración y Contaduría",
    "Contador Público": "Administración y Contaduría",
    "Contador Público y Auditor": "Administración y Contaduría",
    "Contabilidad": "Administración y Contaduría",
    "Contabilidad y Administración Empresarial": "Administración y Contaduría",
    "Finanzas": "Administración y Contaduría",
    "Finanzas Públicas": "Administración y Contaduría",
    "Finanzas y Dirección": "Administración y Contaduría",
    "Auxiliar Contable": "Administración y Contaduría",
    "Informática Contable": "Administración y Contaduría",
    "Auditoría Gubernamental": "Administración y Contaduría",
    "Mercadotecnia": "Administración y Contaduría",
    "Mercadotecnia Internacional": "Administración y Contaduría",
    "Mercadotecnia y Ventas": "Administración y Contaduría",
    "Comercio": "Administración y Contaduría",
    "Comercio Internacional": "Administración y Contaduría",
    "Negocios Internacionales": "Administración y Contaduría",
    "Gestión y Administración": "Administración y Contaduría",
    "Capital Humano": "Administración y Contaduría",
    "Gestión de Recursos Humanos": "Administración y Contaduría",
    "Dirección de Empresas": "Administración y Contaduría",
    "Dirección de Empresas para Ejecutivos con Experiencia": "Administración y Contaduría",
    "Dirección Estratégica de Empresas Familiares": "Administración y Contaduría",
    "Dirección Estratégica y Gestión de la Innovación": "Administración y Contaduría",
    "Calidad Total": "Administración y Contaduría",
    "Calidad Total y Competitividad": "Administración y Contaduría",
    "Valuación": "Administración y Contaduría",
    "Ciencias de la Administración": "Administración y Contaduría",
    "Ciencias Administrativas": "Administración y Contaduría",
    # ── Ciencias Políticas y Sociales (incluye Administración Pública) ────
    # Administración Pública se agrupa aquí por su naturaleza político-
    # institucional: gestión del Estado, no de empresas.
    "Ciencias Políticas y Sociales": "Ciencias Políticas y Sociales",
    "Ciencias Políticas": "Ciencias Políticas y Sociales",
    "Ciencias Políticas y Administración Pública": "Ciencias Políticas y Sociales",
    "Ciencias Políticas y Gestión Pública": "Ciencias Políticas y Sociales",
    "Ciencias Políticas y Administración Urbana": "Ciencias Políticas y Sociales",
    "Administración Pública": "Ciencias Políticas y Sociales",
    "Administración Pública Estatal y Municipal": "Ciencias Políticas y Sociales",
    "Administración Pública y Políticas Públicas": "Ciencias Políticas y Sociales",
    "Administración y Políticas Públicas": "Ciencias Políticas y Sociales",
    "Administración y Políticas con Enfoque en Gestión Política": "Ciencias Políticas y Sociales",
    "Administración y Gestión Electoral": "Ciencias Políticas y Sociales",
    "Gestión Pública Aplicada": "Ciencias Políticas y Sociales",
    "Gestión Pública Municipal": "Ciencias Políticas y Sociales",
    "Gerencia Pública": "Ciencias Políticas y Sociales",
    "Gobierno y Gestión Pública": "Ciencias Políticas y Sociales",
    "Gobierno y Políticas Públicas": "Ciencias Políticas y Sociales",
    "Gobierno y Administración": "Ciencias Políticas y Sociales",
    "Gobierno y Administración Pública": "Ciencias Políticas y Sociales",
    "Políticas Públicas": "Ciencias Políticas y Sociales",
    "Políticas Públicas Comparadas": "Ciencias Políticas y Sociales",
    "Sociología": "Ciencias Políticas y Sociales",
    "Relaciones Internacionales": "Ciencias Políticas y Sociales",
    "Relaciones Públicas": "Ciencias Políticas y Sociales",
    "Asuntos Internacionales": "Ciencias Políticas y Sociales",
    "Ciencias Sociales": "Ciencias Políticas y Sociales",
    "Trabajo Social": "Ciencias Políticas y Sociales",
    "Derechos Humanos": "Ciencias Políticas y Sociales",
    "Derechos Humanos y Garantías": "Ciencias Políticas y Sociales",
    "Gobernanza y Derechos Humanos": "Ciencias Políticas y Sociales",
    "Gobernanza y Globalización": "Ciencias Políticas y Sociales",
    "Gobernanza y Gobiernos Locales": "Ciencias Políticas y Sociales",
    "Estudios Políticos y Gobierno": "Ciencias Políticas y Sociales",
    "Estudios Comparativos de Política": "Ciencias Políticas y Sociales",
    "Estudios Latinoamericanos": "Ciencias Políticas y Sociales",
    "Estudios Regionales": "Ciencias Políticas y Sociales",
    "Estudios de Población": "Ciencias Políticas y Sociales",
    "Estudios Parlamentarios": "Ciencias Políticas y Sociales",
    "Acción Política": "Ciencias Políticas y Sociales",
    "Práctica Política": "Ciencias Políticas y Sociales",
    "Gerencia Política": "Ciencias Políticas y Sociales",
    "Comunicación Política": "Ciencias Políticas y Sociales",
    "Comunicación Política y Gobernanza Estratégica": "Ciencias Políticas y Sociales",
    "Comunicación Social y Política": "Ciencias Políticas y Sociales",
    "Política y Gestión Pública": "Ciencias Políticas y Sociales",
    "Política y Gestión Social": "Ciencias Políticas y Sociales",
    "Desarrollo Regional": "Ciencias Políticas y Sociales",
    "Desarrollo Comunitario": "Ciencias Políticas y Sociales",
    "Desarrollo Económico Regional": "Ciencias Políticas y Sociales",
    "Responsabilidad Social": "Ciencias Políticas y Sociales",
    "Seguridad Nacional": "Ciencias Políticas y Sociales",
    "Intervención Social en las Sociedades del Conocimiento": "Ciencias Políticas y Sociales",
    # Economía combinada con gobierno/política va aquí, no a Económico-Financiera
    "Economía y Gobierno": "Ciencias Políticas y Sociales",
    "Economía y Buen Gobierno": "Ciencias Políticas y Sociales",
    "Economía y Política Pública": "Ciencias Políticas y Sociales",
    # ── Ciencias de la Educación (Educación + Pedagogía) ──────────────────
    # Categoría propia por su relevancia política: sindicatos docentes y
    # organizaciones magisteriales tienen peso electoral significativo.
    "Ciencias de la Educación": "Ciencias de la Educación",
    "Educación": "Ciencias de la Educación",
    "Educación Primaria": "Ciencias de la Educación",
    "Educación Básica": "Ciencias de la Educación",
    "Educación Media": "Ciencias de la Educación",
    "Educación Preescolar": "Ciencias de la Educación",
    "Educación Internacional": "Ciencias de la Educación",
    "Educación Primaria para el Medio Indígena": "Ciencias de la Educación",
    "Educación Relacional y Bioaprendizaje": "Ciencias de la Educación",
    "Educación Media en el Área de Ciencias Naturales": "Ciencias de la Educación",
    "Profesora de Educación Primaria": "Ciencias de la Educación",
    "Desarrollo de la Educación Básica": "Ciencias de la Educación",
    "Pedagogía": "Ciencias de la Educación",
    "Docencia en Educación Superior": "Ciencias de la Educación",
    "Innovación Educativa": "Ciencias de la Educación",
    "Gestión Educativa": "Ciencias de la Educación",
    "Gestión en la Educación Superior": "Ciencias de la Educación",
    "Gestión del Aprendizaje": "Ciencias de la Educación",
    "Administración Educativa": "Ciencias de la Educación",
    "Desarrollo Cognitivo de las Inteligencias Múltiples": "Ciencias de la Educación",
    "Programación Neurolingüística": "Ciencias de la Educación",
    "Educador Físico": "Ciencias de la Educación",
    "Ciencias de la Familia y Educación": "Ciencias de la Educación",
    "Ciencias Sociales de la Educación": "Ciencias de la Educación",
    # ── Ingeniería (incluye cómputo e informática) ────────────────────────
    "Ingeniería": "Ingeniería",
    "Ingeniería Civil": "Ingeniería",
    "Ingeniería Industrial": "Ingeniería",
    "Ingeniería Industrial y de Sistemas": "Ingeniería",
    "Ingeniería Mecánica": "Ingeniería",
    "Ingeniería Mecánica y Eléctrica": "Ingeniería",
    "Ingeniería Aeroespacial": "Ingeniería",
    "Ingeniería Biomédica": "Ingeniería",
    "Ingeniería Petrolera": "Ingeniería",
    "Ingeniería Administrativa": "Ingeniería",
    "Ingeniería en Sistemas Computacionales": "Ingeniería",
    "Ingeniería de Sistemas": "Ingeniería",
    "Ingeniería Forestal": "Ingeniería",
    "Computación": "Ingeniería",
    "Computación Administrativa": "Ingeniería",
    "Ciencias de la Computación": "Ingeniería",
    "Informática": "Ingeniería",
    "Sistemas Computarizados e Informática": "Ingeniería",
    "Sistemas de Computación Administrativa": "Ingeniería",
    "Programación": "Ingeniería",
    "Administración de Tecnologías de la Información": "Ingeniería",
    # ── Económico-Financiera (economía pura; combinaciones con política → CPS) ──
    "Económico-Financiera": "Económico-Financiera",
    "Economía": "Económico-Financiera",
    "Economía del Empleo": "Económico-Financiera",
    "Economía, Pobreza y Desarrollo Social": "Económico-Financiera",
    # ── Ciencias de la Salud ──────────────────────────────────────────────
    "Ciencias de la Salud": "Ciencias de la Salud",
    "Médico Cirujano": "Ciencias de la Salud",
    "Médico Cirujano y Partero": "Ciencias de la Salud",
    "Medicina": "Ciencias de la Salud",
    "Medicina Tradicional China y Moxibustión": "Ciencias de la Salud",
    "Enfermería": "Ciencias de la Salud",
    "Enfermería y Obstetricia": "Ciencias de la Salud",
    "Nutrición": "Ciencias de la Salud",
    "Nutrición Clínica": "Ciencias de la Salud",
    "Odontología": "Ciencias de la Salud",
    "Gerontología": "Ciencias de la Salud",
    "Químico Farmacobiólogo": "Ciencias de la Salud",
    "Administración en Sistemas de Salud": "Ciencias de la Salud",
    "Administración de Hospitales": "Ciencias de la Salud",
    "Administración de Instituciones de Salud": "Ciencias de la Salud",
    # ── Comunicación ──────────────────────────────────────────────────────
    "Comunicación": "Comunicación",
    "Ciencias de la Comunicación": "Comunicación",
    "Comunicación Organizacional": "Comunicación",
    "Comunicación Social": "Comunicación",
    "Comunicación y Cultura": "Comunicación",
    "Ciencias de la Información y Comunicación": "Comunicación",
    "Diseño y Comunicación Visual": "Comunicación",
    "Locución": "Comunicación",
    # ── Arquitectura y Diseño ─────────────────────────────────────────────
    "Arquitectura y Diseño": "Arquitectura y Diseño",
    "Arquitectura": "Arquitectura y Diseño",
    "Diseño Industrial": "Arquitectura y Diseño",
    "Diseño de Modas": "Arquitectura y Diseño",
    # ── Agropecuaria y Zootecnia (incluye veterinaria y forestal) ─────────
    "Agropecuaria y Zootecnia": "Agropecuaria y Zootecnia",
    "Ingeniero Agrónomo": "Agropecuaria y Zootecnia",
    "Ingeniero Agrónomo Zootecnista": "Agropecuaria y Zootecnia",
    "Ingeniero Agrónomo Fitotecnista": "Agropecuaria y Zootecnia",
    "Ingeniería Agrónoma Fitotecnista": "Agropecuaria y Zootecnia",
    "Ingeniería Agrónoma en Producción": "Agropecuaria y Zootecnia",
    "Ingeniería en Desarrollo Agrícola": "Agropecuaria y Zootecnia",
    "Medicina Veterinaria y Zootecnia": "Agropecuaria y Zootecnia",
    "Veterinaria y Zootecnista": "Agropecuaria y Zootecnia",
    "Ciencias Forestales": "Agropecuaria y Zootecnia",
    "Forestal": "Agropecuaria y Zootecnia",
    "Manejo y Conservación de Bosques Tropicales y Biodiversidad": "Agropecuaria y Zootecnia",
    "Desarrollo Sustentable y Ecoturismo": "Agropecuaria y Zootecnia",
    "Desarrollo, Medio Ambiente y Territorio": "Agropecuaria y Zootecnia",
    "Gestión Ambiental": "Agropecuaria y Zootecnia",
    # ── Ciencias (ciencias naturales; Biología y Química van aquí, no a Salud) ──
    "Ciencias": "Ciencias",
    "Ciencias Naturales": "Ciencias",
    "Biología": "Ciencias",
    "Química": "Ciencias",
    # ── Psicología ────────────────────────────────────────────────────────
    "Psicología": "Psicología",
    "Psicología Organizacional": "Psicología",
    "Psicología Moderna": "Psicología",
    "Psicoterapias Humanistas": "Psicología",
    # ── Otra (turismo, deporte y misceláneos sin categoría natural) ───────
    "Otra": "Otra",
    "Hotelería y Gastronomía": "Otra",
    "Turismo": "Otra",
    "Organización Deportiva": "Otra",
    "Gestión de Entidades Deportivas": "Otra",
}


def _normalize_preparacion(series: pd.Series) -> pd.Series:
    """
    Normaliza 'preparacion_academica' al conjunto canónico de 14 categorías.

    Usa mapeo exacto contra PREPARACION_ACADEMICA_MAPPING. Valores no
    encontrados caen a 'Otra' y se reportan en el log para detectar nuevas
    especialidades que el scraper capture en futuras legislaturas.
    """
    cleaned = series.str.strip()
    mapped = cleaned.map(PREPARACION_ACADEMICA_MAPPING)
    unmapped_mask = mapped.isna() & cleaned.notna() & (cleaned != "")
    if unmapped_mask.any():
        unique_unmapped = sorted(cleaned[unmapped_mask].unique().tolist())
        logger.warning(
            "preparacion_academica: %d valor(es) sin mapeo → 'Otra': %s",
            len(unique_unmapped),
            unique_unmapped,
        )
    return mapped.fillna("Otra")


def _extract_experiencia_legislativa(series: pd.Series) -> pd.DataFrame:
    """
    Extrae flags binarios de 'experiencia_legislativa'.

    El campo contiene concatenaciones de hasta 3 roles previos (separados por
    espacio), siempre del conjunto {'Diputado/a Local', 'Diputado/a Federal',
    'Senador/a'}. Vacío = sin experiencia previa (no es dato faltante).

    Retorna un DataFrame con 4 columnas:
      fue_diputado_local          — 1 si tuvo cargo de diputado/a local previo
      fue_diputado_federal        — 1 si tuvo cargo de diputado/a federal previo
      fue_senador                 — 1 si tuvo cargo de senador/a previo
      n_cargos_legislativos_prev  — suma de los tres flags (0–3)
    """
    s = series.fillna("").str.strip()
    local = s.str.contains(r"Diputad[oa] Local", regex=True).astype(int)
    federal = s.str.contains(r"Diputad[oa] Federal", regex=True).astype(int)
    senador = s.str.contains(r"Senador[a]?", regex=True).astype(int)
    return pd.DataFrame(
        {
            "fue_diputado_local": local,
            "fue_diputado_federal": federal,
            "fue_senador": senador,
            "n_cargos_legislativos_prev": local + federal + senador,
        }
    )


def _presence_flag(series: pd.Series) -> pd.Series:
    """
    Crea un flag binario (0/1) que indica si el campo tiene valor.

    1 = el campo tiene un valor no nulo y no vacío.
    0 = el campo es nulo o solo contiene espacios.

    Se usa para los campos de contacto (correo, teléfono, ubicación):
    el contenido textual no es relevante para ML, pero sí el hecho de
    que exista o no.
    """
    return series.notna().astype(int) & (series.str.strip() != "").astype(int)


def _text_length(series: pd.Series) -> pd.Series:
    """
    Cuenta las palabras en un campo de texto libre.

    Convierte los campos 'preparacion_academica' y 'experiencia_legislativa'
    (texto de longitud variable) en una característica numérica: número de
    palabras. Los nulos se tratan como texto vacío (0 palabras).
    """
    return series.fillna("").str.split().str.len().astype(int)


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica todos los pasos de limpieza a un DataFrame crudo de una legislatura.

    Este es el punto de entrada que llama pipeline.py. Recibe el DataFrame
    tal como lo entregó load.py (todas las columnas como str) y devuelve un
    DataFrame listo para que transform.py extraiga características de las
    columnas JSON.

    Pasos en orden:
      1. Eliminar columnas inútiles (DROP_COLUMNS).
      2. Convertir legislatura_num a entero.
      3. Parsear nacimiento → y_nacimiento + edad_al_tomar_cargo.
      4. Codificar categorías: grado de estudios, principio de elección, partido.
      5. Codificar suplente_referencia como entero.
      6. Crear flags de presencia para campos de contacto.
      7. Calcular longitud de texto para campos descriptivos.
      8. Eliminar columnas de texto ya procesadas.
      9. Establecer diputado_id como índice.

    Parámetros
    ----------
    df : pd.DataFrame — DataFrame crudo de load_legislature() (all str).

    Retorna
    -------
    pd.DataFrame con índice diputado_id y columnas limpias. Las columnas
    JSON siguen como str para que transform.py las procese.
    """
    df = df.copy()  # no modificar el DataFrame original que recibió pipeline.py

    # Identificar la legislatura para incluirla en los mensajes de log.
    leg_name = (
        str(df["legislatura_num"].iloc[0]) if "legislatura_num" in df.columns else "?"
    )

    # --- 1. Eliminar columnas inútiles ---
    # "ignore" evita error si alguna columna ya no existe en el CSV.
    dropped = [c for c in DROP_COLUMNS if c in df.columns]
    df.drop(columns=dropped, inplace=True)
    logger.info(
        "[%s] Eliminadas %d columnas redundantes: %s", leg_name, len(dropped), dropped
    )

    # --- 2. Legislatura como entero ---
    # El CSV crudo tiene legislatura_num como str ("66"); lo convertimos a Int64
    # (entero con soporte de NA de pandas) para uso numérico.
    df["legislatura_num"] = pd.to_numeric(
        df["legislatura_num"], errors="coerce"
    ).astype("Int64")

    # --- 3. Fecha de nacimiento → año + edad al tomar cargo ---
    yyyy, _ = _parse_nacimiento(df["nacimiento"])
    df["y_nacimiento"] = yyyy.astype("Int64")

    # Reportar filas con fecha no parseables para detectar problemas en el scraper.
    n_unparsed = df["y_nacimiento"].isna().sum()
    if n_unparsed:
        logger.warning(
            "[%s] nacimiento no parseable en %d / %d filas → y_nacimiento=null",
            leg_name,
            n_unparsed,
            len(df),
        )
    else:
        logger.info(
            "[%s] nacimiento parseado correctamente en todas las %d filas",
            leg_name,
            len(df),
        )

    # Calcular edad al inicio de la legislatura usando la tabla LEG_START_YEAR.
    start_year = LEG_START_YEAR.get(leg_name)
    if start_year and df["y_nacimiento"].notna().any():
        # Calcular edad al inicio de la legislatura.
        df["edad_al_tomar_cargo"] = (start_year - df["y_nacimiento"]).astype("Int64")
        # Marcar como nulo las edades fuera de rango plausible (error de datos).
        bad = (df["edad_al_tomar_cargo"] < 18) | (df["edad_al_tomar_cargo"] > 90)
        n_bad = bad.sum()
        if n_bad:
            df.loc[bad, "edad_al_tomar_cargo"] = pd.NA
            logger.warning(
                "[%s] %d edades fuera de [18,90] → se mantienen como están",
                leg_name,
                n_bad,
            )
        logger.info(
            "[%s] edad_al_tomar_cargo: min=%s, max=%s, media=%.1f",
            leg_name,
            df["edad_al_tomar_cargo"].min(),
            df["edad_al_tomar_cargo"].max(),
            df["edad_al_tomar_cargo"].mean(),
        )
    else:
        # Si no hay mapeo de año de inicio, la edad no se puede calcular.
        df["edad_al_tomar_cargo"] = pd.NA
        logger.warning(
            "[%s] Sin año de inicio de legislatura — edad_al_tomar_cargo todo nulo",
            leg_name,
        )

    # La columna 'nacimiento' ya fue procesada; se elimina para no duplicar información.
    df.drop(columns=["nacimiento"], inplace=True)

    # --- 4a. Grado de estudios → entero ordinal ---
    df["grado_estudios_ord"] = _encode_grado(
        df.get("ultimo_grado_de_estudios", pd.Series(dtype=str))
    )
    unknown_grado = (df["grado_estudios_ord"] == 0).sum()
    logger.info(
        "[%s] grado_estudios_ord: %d desconocidos (0), %d codificados",
        leg_name,
        unknown_grado,
        len(df) - unknown_grado,
    )
    df.drop(columns=["ultimo_grado_de_estudios"], inplace=True)

    # --- 4b. Principio de elección → 1 / 0 / -1 ---
    df["mayoria_relativa"] = _encode_principio(
        df.get("principio_de_eleccion", pd.Series(dtype=str))
    )
    vc = df["mayoria_relativa"].value_counts().to_dict()
    logger.info(
        "[%s] mayoria_relativa: MR=%d, RP=%d, desconocido=%d",
        leg_name,
        vc.get(1, 0),
        vc.get(0, 0),
        vc.get(-1, 0),
    )
    df.drop(columns=["principio_de_eleccion"], inplace=True)

    # --- 4c. Partido → abreviación normalizada en mayúsculas ---
    df["partido"] = _encode_partido(df.get("partido", pd.Series(dtype=str)))
    parties = sorted(df["partido"].unique().tolist())
    logger.info("[%s] partido: %d únicos — %s", leg_name, len(parties), parties)
    # --- 5b. suplente_referencia → int (0 si no tiene suplente) ---
    df["suplente_referencia"] = (
        pd.to_numeric(
            df.get("suplente_referencia", pd.Series(dtype=str)), errors="coerce"
        )
        .fillna(0)
        .astype(int)
    )
    # Flag derivado: 1 si el legislador tiene suplente identificado.
    df["tiene_suplente"] = (df["suplente_referencia"] > 0).astype(int)
    logger.info(
        "[%s] tiene_suplente: %d con suplente", leg_name, df["tiene_suplente"].sum()
    )

    # --- 5c. referencia → int (ID del perfil en el SIL) ---
    df["referencia"] = pd.to_numeric(df["referencia"], errors="coerce").astype("Int64")

    # --- 7. Normalización y métricas de texto para campos descriptivos ---
    # 7a. preparacion_academica → categoría canónica (área de formación) +
    #     conteo de palabras como indicador de riqueza/especificidad del perfil.
    prep_series = df.get("preparacion_academica", pd.Series(dtype=str))
    df["area_formacion"] = _normalize_preparacion(prep_series)

    area_counts = df["area_formacion"].value_counts().to_dict()
    n_otra = area_counts.get("Otra", 0)
    logger.info(
        "[%s] area_formacion: %d categorías únicas, %d sin mapeo → 'Otra' | distribución: %s",
        leg_name,
        df["area_formacion"].nunique(),
        n_otra,
        dict(sorted(area_counts.items(), key=lambda x: -x[1])),
    )

    # 7b. experiencia_legislativa → flags binarios por tipo de cargo previo.
    # El campo es una concatenación de roles del conjunto cerrado
    # {Diputado/a Local, Diputado/a Federal, Senador/a}; vacío = sin
    # experiencia previa (no es dato faltante, no se imputa).
    exp_series = df.get("experiencia_legislativa", pd.Series(dtype=str))
    exp_flags = _extract_experiencia_legislativa(exp_series)
    exp_flags.index = df.index
    df = pd.concat([df, exp_flags], axis=1)
    logger.info(
        "[%s] experiencia_legislativa: local=%d, federal=%d, senador=%d | "
        "sin experiencia previa=%d",
        leg_name,
        df["fue_diputado_local"].sum(),
        df["fue_diputado_federal"].sum(),
        df["fue_senador"].sum(),
        (df["n_cargos_legislativos_prev"] == 0).sum(),
    )

    # --- 9. Eliminar columnas de texto ya procesadas ---
    # Después de extraer flags y conteos de palabras, el texto original
    # no aporta valor adicional y solo ocupa espacio.
    cols_to_drop = [
        "correo_electronico",
        "telefono",
        "ubicacion",
        "preparacion_academica",
        "experiencia_legislativa",
        "suplente",  # nombre del suplente — solo se conserva suplente_referencia
    ]
    df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True)

    # --- 10. diputado_id como índice ---
    # El ID estable entre legislaturas se convierte en índice del DataFrame.
    # save.py lo escribe como primera columna del CSV de salida.
    df.set_index("diputado_id", inplace=True)

    remaining_nulls = df.isnull().sum().sum()
    logger.info(
        "[%s] limpieza completada → %d filas × %d columnas | nulos restantes: %d",
        leg_name,
        len(df),
        len(df.columns),
        remaining_nulls,
    )
    return df
