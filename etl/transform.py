"""
transform.py — Extracción de características desde columnas JSON.

FUNCIÓN EN EL PIPELINE:
    Tercer paso del ETL (después de clean.py, antes de save.py).
    Recibe el DataFrame limpio de clean.py y desanida las columnas que
    contienen listas JSON (comisiones, trayectorias, etc.), convirtiéndolas
    en conteos numéricos listos para modelos de ML.

CONEXIONES:
    Entrada : pd.DataFrame de clean.py → clean()
              (columnas JSON todavía como str, índice = diputado_id)
    Salida  : pd.DataFrame completamente plano → recibido por save.py → save_legislature()
              - todas las columnas JSON reemplazadas por conteos enteros
              - diputado_id sigue siendo el índice

POR QUÉ AQUÍ Y NO EN clean.py:
    Las columnas JSON requieren parsear listas anidadas de dicts, lo que es
    cualitativamente diferente a normalizar tipos escalares. Separar esta
    lógica facilita modificar las reglas de extracción sin tocar la limpieza.
"""

import json
import logging
import re
import unicodedata

import pandas as pd

# Logger del módulo — mensajes con prefijo "etl.transform".
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Funciones auxiliares de parseo JSON
# ---------------------------------------------------------------------------

def _safe_parse(cell) -> list:
    """
    Parsea una celda que contiene una lista JSON serializada como texto.

    El scraper almacena las secciones anidadas (comisiones, trayectorias)
    como cadenas JSON dentro de una celda CSV. Esta función las convierte
    de vuelta a listas de Python.

    Retorna [] si la celda está vacía, es nula o no es JSON válido, para
    que los conteos resulten en 0 en lugar de causar una excepción.
    """
    if pd.isna(cell) or not str(cell).strip():
        return []
    try:
        result = json.loads(cell)
        # Garantizar que el resultado sea una lista (nunca dict u otro tipo).
        return result if isinstance(result, list) else []
    except (json.JSONDecodeError, TypeError):
        return []


def _count_json(series: pd.Series) -> pd.Series:
    """
    Cuenta el número de elementos en cada celda JSON de una Series.

    Aplica _safe_parse() a cada celda y devuelve la longitud de la lista
    resultante como entero. Es la transformación base para todas las
    columnas de trayectoria (que solo necesitan conteo, no detalle).
    """
    return series.apply(lambda x: len(_safe_parse(x))).astype(int)


# ---------------------------------------------------------------------------
# Normalización de comisiones
# ---------------------------------------------------------------------------

# Strips the body suffix — (C. Diputados), (H. Congreso de la Unión),
# (C. Senadores) — from the raw commission name.
# Com. Perm. entries are handled before this step (counted separately).
_RE_STRIP_BODY = re.compile(
    r"\s*\((?:C\.\s*Diputados|H\.\s*Congreso[^)]*|C\.\s*Senadores)\)\s*$"
)

# Maps old commission name (body-stripped, whitespace-normalized) → canonical.
# Canonical = most recent name used in SIL for that thematic area.
COMISION_CANONICAL: dict[str, str] = {
    # ── Igualdad de Género ────────────────────────────────────────────────
    "Equidad y Género": "Igualdad de Género",

    # ── Ciencia, Tecnología e Innovación ──────────────────────────────────
    "Ciencia y Tecnología": "Ciencia, Tecnología e Innovación",

    # ── Medio Ambiente y Recursos Naturales ───────────────────────────────
    "Ecología y Medio Ambiente": "Medio Ambiente y Recursos Naturales",
    "Medio Ambiente, Sustentabilidad, Cambio Climático y Recursos Naturales": "Medio Ambiente y Recursos Naturales",

    # ── Cambio Climático y Sostenibilidad ─────────────────────────────────
    "Cambio Climático": "Cambio Climático y Sostenibilidad",

    # ── Recursos Hidráulicos, Agua Potable y Saneamiento ──────────────────
    "Asuntos Hidráulicos": "Recursos Hidráulicos, Agua Potable y Saneamiento",
    "Recursos Hidráulicos": "Recursos Hidráulicos, Agua Potable y Saneamiento",
    "Agua Potable y Saneamiento": "Recursos Hidráulicos, Agua Potable y Saneamiento",

    # ── Pueblos Indígenas y Afromexicanos ─────────────────────────────────
    "Asuntos Indígenas": "Pueblos Indígenas y Afromexicanos",
    "Pueblos Indígenas": "Pueblos Indígenas y Afromexicanos",

    # ── Derechos de la Niñez y Adolescencia ──────────────────────────────
    "Derechos de la Niñez": "Derechos de la Niñez y Adolescencia",

    # ── Economía, Comercio y Competitividad ───────────────────────────────
    "Comercio y Fomento Industrial": "Economía, Comercio y Competitividad",
    "Economía": "Economía, Comercio y Competitividad",
    "Comercio": "Economía, Comercio y Competitividad",
    "Competitividad": "Economía, Comercio y Competitividad",

    # ── Economía Social y Fomento del Cooperativismo ──────────────────────
    "Fomento Cooperativo": "Economía Social y Fomento del Cooperativismo",
    "Fomento Cooperativo y Economía Social": "Economía Social y Fomento del Cooperativismo",

    # ── Comunicaciones y Transportes ──────────────────────────────────────
    # Comunicaciones and Transportes were separate committees until merged.
    "Comunicaciones": "Comunicaciones y Transportes",
    "Transportes": "Comunicaciones y Transportes",

    # ── Cultura y Cinematografía ──────────────────────────────────────────
    "Cultura": "Cultura y Cinematografía",

    # ── Radio, Televisión y Cinematografía ────────────────────────────────
    "Radio y Televisión": "Radio, Televisión y Cinematografía",

    # ── Asuntos Migratorios ───────────────────────────────────────────────
    "Población, Fronteras y Asuntos Migratorios": "Asuntos Migratorios",

    # ── Gobernación y Población ───────────────────────────────────────────
    "Gobernación": "Gobernación y Población",
    "Gobernación y Seguridad Pública": "Gobernación y Población",
    "Gobernación y Puntos Constitucionales": "Gobernación y Población",

    # ── Presupuesto y Cuenta Pública ──────────────────────────────────────
    "Programación, Presupuesto y Cuenta Pública": "Presupuesto y Cuenta Pública",

    # ── Vigilancia de la Auditoría Superior de la Federación ──────────────
    "Vigilancia de la Contaduría Mayor de Hacienda": "Vigilancia de la Auditoría Superior de la Federación",

    # ── Régimen, Reglamentos y Prácticas Parlamentarias ───────────────────
    "Reglamentos y Prácticas Parlamentarias": "Régimen, Reglamentos y Prácticas Parlamentarias",

    # ── Protección Civil y Prevención de Desastres ────────────────────────
    "Protección Civil": "Protección Civil y Prevención de Desastres",

    # ── Desarrollo Metropolitano, Urbano, Ordenamiento Territorial y Movilidad
    "Asentamientos Humanos y Obras Públicas": "Desarrollo Metropolitano, Urbano, Ordenamiento Territorial y Movilidad",
    "Desarrollo Urbano y Ordenamiento Territorial": "Desarrollo Metropolitano, Urbano, Ordenamiento Territorial y Movilidad",
    "Desarrollo Metropolitano": "Desarrollo Metropolitano, Urbano, Ordenamiento Territorial y Movilidad",
    "Movilidad": "Desarrollo Metropolitano, Urbano, Ordenamiento Territorial y Movilidad",

    # ── Fortalecimiento del Federalismo ───────────────────────────────────
    "Fortalecimiento al Federalismo": "Fortalecimiento del Federalismo",

    # ── Federalismo y Desarrollo Municipal ───────────────────────────────
    "Desarrollo Municipal": "Federalismo y Desarrollo Municipal",

    # ── Comité de Información, Gestoría y Quejas ──────────────────────────
    "Información Gestoría y Quejas": "Comité de Información, Gestoría y Quejas",

    # ── Justicia ──────────────────────────────────────────────────────────
    "Justicia y Derechos Humanos": "Justicia",

    # ── Educación Pública y Servicios Educativos ──────────────────────────
    "Educación": "Educación Pública y Servicios Educativos",

    # ── Desarrollo y Conservación Rural, Agrícola y Autosuficiencia Alimentaria
    "Desarrollo Rural": "Desarrollo y Conservación Rural, Agrícola y Autosuficiencia Alimentaria",

    # ── Ciudad de México ──────────────────────────────────────────────────
    "Distrito Federal": "Ciudad de México",

    # ── Población ────────────────────────────────────────────────────────
    "Población y Desarrollo": "Población",
}

# Canonical names that receive a binary feature column (comision_<slug>).
# Threshold: > 50 total appearances across all legislatures after canonicalization.
# "Agricultura y Ganadería" kept separate — it was a merged committee that later
# split into "Agricultura" and "Ganadería"; each historical record maps correctly.
MAJOR_COMISIONES: list[str] = [
    "Agricultura y Ganadería",
    "Agricultura y Sistemas de Riego",
    "Asuntos Frontera Norte",
    "Asuntos Frontera Sur",
    "Asuntos Frontera Sur-Sureste",
    "Asuntos Migratorios",
    "Atención a Grupos Vulnerables",
    "Bienestar",
    "Cambio Climático y Sostenibilidad",
    "Ciencia, Tecnología e Innovación",
    "Ciudad de México",
    "Comité de Administración",
    "Comité de Información, Gestoría y Quejas",
    "Comité del Centro de Estudios de Derecho e Investigaciones Parlamentarias",
    "Comité del Centro de Estudios de las Finanzas Públicas",
    "Comité del Centro de Estudios Sociales y de Opinión Pública",
    "Comunicaciones y Transportes",
    "Cultura y Cinematografía",
    "Defensa Nacional",
    "Deporte",
    "Derechos de la Niñez y Adolescencia",
    "Derechos Humanos",
    "Desarrollo Social",
    "Desarrollo y Conservación Rural, Agrícola y Autosuficiencia Alimentaria",
    "Desarrollo Metropolitano, Urbano, Ordenamiento Territorial y Movilidad",
    "Economía, Comercio y Competitividad",
    "Economía Social y Fomento del Cooperativismo",
    "Educación Pública y Servicios Educativos",
    "Energía",
    "Federalismo y Desarrollo Municipal",
    "Fortalecimiento del Federalismo",
    "Función Pública",
    "Ganadería",
    "Gobernación y Población",
    "Hacienda y Crédito Público",
    "Igualdad de Género",
    "Infraestructura",
    "Jurisdiccional",
    "Justicia",
    "Juventud",
    "Juventud y Deporte",
    "Marina",
    "Medio Ambiente y Recursos Naturales",
    "Participación Ciudadana",
    "Pesca",
    "Población",
    "Presupuesto y Cuenta Pública",
    "Protección Civil y Prevención de Desastres",
    "Pueblos Indígenas y Afromexicanos",
    "Puntos Constitucionales",
    "Radio, Televisión y Cinematografía",
    "Recursos Hidráulicos, Agua Potable y Saneamiento",
    "Reforma Agraria",
    "Reforma Política-Electoral",
    "Régimen, Reglamentos y Prácticas Parlamentarias",
    "Relaciones Exteriores",
    "Salud",
    "Seguridad Ciudadana",
    "Seguridad Pública",
    "Seguridad Social",
    "Trabajo y Previsión Social",
    "Transparencia y Anticorrupción",
    "Turismo",
    "Vigilancia de la Auditoría Superior de la Federación",
    "Vivienda",
    "Zonas Metropolitanas",
]

# Precomputed slug → canonical mapping for fast lookup in column names.
# Generated once at module load; never mutated at runtime.
_COMISION_SLUG: dict[str, str] = {}  # slug → canonical (populated below)
_CANONICAL_SLUG: dict[str, str] = {}  # canonical → slug (populated below)


def _slug(name: str) -> str:
    """Canonical commission name → lowercase ASCII identifier with underscores."""
    nfkd = unicodedata.normalize("NFD", name)
    ascii_str = nfkd.encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z0-9]+", "_", ascii_str.lower()).strip("_")


for _canon in MAJOR_COMISIONES:
    _s = _slug(_canon)
    _CANONICAL_SLUG[_canon] = _s
    _COMISION_SLUG[_s] = _canon


def _parse_comision_name(raw: str) -> tuple[str, str, str]:
    """
    Parses a raw 'Comisión' field into (canonical_name, cuerpo, tipo).

    cuerpo — legislative body: 'C. Diputados', 'Com. Perm.',
              'H. Congreso de la Unión', 'C. Senadores'
    tipo   — 'regular', 'Especial', 'Comité', 'Bicamaral'
    canonical_name — after whitespace normalization, body strip, and mapping
                     via COMISION_CANONICAL (falls back to the cleaned name).
    """
    norm = re.sub(r"\s+", " ", raw).strip()

    # 1. Detect body from the raw string (before stripping).
    if "Com. Perm." in norm:
        cuerpo = "Com. Perm."
    elif "C. Diputados" in norm:
        cuerpo = "C. Diputados"
    elif "H. Congreso" in norm:
        cuerpo = "H. Congreso de la Unión"
    elif "C. Senadores" in norm:
        cuerpo = "C. Senadores"
    else:
        cuerpo = "desconocido"

    # 2. Strip body suffix to get the thematic name.
    name = _RE_STRIP_BODY.sub("", norm).strip()

    # 3. Detect commission type from prefix.
    if name.startswith("Especial"):
        tipo = "Especial"
    elif name.startswith("Comité"):
        tipo = "Comité"
    elif name.startswith("Bicamaral"):
        tipo = "Bicamaral"
    else:
        tipo = "regular"

    # 4. Apply canonical mapping; fall back to the cleaned name.
    canonical = COMISION_CANONICAL.get(name, name)

    return canonical, cuerpo, tipo


# ---------------------------------------------------------------------------
# Extracción específica por sección
# ---------------------------------------------------------------------------

def _extract_comisiones(df: pd.DataFrame, leg_name: str = "?") -> pd.DataFrame:
    """
    Extrae características de la columna 'comisiones'.

    Cada entrada JSON tiene: Comisión, Puesto, Fecha Inicial, Fecha Final, Estatus.
    Estatus no se usa como filtro — se procesan todas las membresías.

    Contadores generados:
      n_comisiones            — membresías en comisiones regulares + Especiales
                                (excluye sesiones de Comisión Permanente)
      n_comisiones_especiales — subconjunto de n_comisiones con tipo Especial
      n_presidencias          — roles de Presidente, Vicepresidente o Copresidente
      n_secretarias           — roles de Secretario
      presidente_comision     — 1 si tuvo al menos un rol presidencial
      lider_comision          — 1 si tuvo cualquier rol de liderazgo (pdte o sec)

    Flags binarios (uno por comisión en MAJOR_COMISIONES):
      comision_<slug>         — 1 si el legislador perteneció a esa comisión
                                (solo comisiones regulares/Comité/Bicamaral;
                                 Especiales y Com. Perm. no generan flags)

    La columna 'comisiones' original se elimina al finalizar.
    """
    parsed = df["comisiones"].apply(_safe_parse)

    PDTE_PUESTOS = {"Presidente", "Vicepresidente", "Copresidente"}

    def _process_row(items: list) -> dict:
        n_com = 0
        n_esp = 0
        n_pdte = 0
        n_sec = 0
        comision_set: set[str] = set()

        for item in items:
            raw = item.get("Comisión", "").strip()
            puesto = item.get("Puesto", "").strip()
            canon, cuerpo, tipo = _parse_comision_name(raw)

            if cuerpo == "Com. Perm.":
                pass  # Com.Perm. puesto still counts for leadership tallies
            else:
                n_com += 1
                if tipo == "Especial":
                    n_esp += 1
                elif canon in _CANONICAL_SLUG:
                    # only regular/Comité/Bicamaral get binary flags
                    comision_set.add(canon)

            if puesto in PDTE_PUESTOS:
                n_pdte += 1
            elif puesto == "Secretario":
                n_sec += 1

        return {
            "n_comisiones": n_com,
            "n_comisiones_especiales": n_esp,
            "n_presidencias": n_pdte,
            "n_secretarias": n_sec,
            "presidente_comision": int(n_pdte > 0),
            "lider_comision": int((n_pdte + n_sec) > 0),
            "_cset": comision_set,
        }

    results = parsed.apply(_process_row)
    result_df = pd.DataFrame(list(results), index=df.index)

    scalar_cols = [
        "n_comisiones", "n_comisiones_especiales",
        "n_presidencias", "n_secretarias", "presidente_comision", "lider_comision",
    ]
    for col in scalar_cols:
        df[col] = result_df[col].astype(int)

    # Binary flag columns — one per major canonical commission.
    csets = result_df["_cset"]
    n_flag_cols = 0
    for canon in MAJOR_COMISIONES:
        col = f"comision_{_CANONICAL_SLUG[canon]}"
        df[col] = csets.apply(lambda s, c=canon: int(c in s))
        n_flag_cols += 1

    logger.info(
        "[%s] comisiones: n_com=%d, especiales=%d | "
        "presidentes=%d, secretarios=%d, líderes=%d | flags=%d",
        leg_name,
        df["n_comisiones"].sum(),
        df["n_comisiones_especiales"].sum(),
        df["n_presidencias"].sum(),
        df["n_secretarias"].sum(),
        df["lider_comision"].sum(),
        n_flag_cols,
    )

    df.drop(columns=["comisiones"], inplace=True)
    return df


# ---------------------------------------------------------------------------
# Extracción de trayectoria_administrativa
# ---------------------------------------------------------------------------

# Guarda defensiva: normalize.py elimina los centinelas antes de llegar aquí,
# pero este filtro protege contra datos crudos no normalizados.
_RE_TRAY_HEADER = re.compile(r"TRAYECTORIA|ESCOLARIDAD|INVESTIGACIÓN|OTROS RUBROS", re.IGNORECASE)

# --- Flags de rol jerárquico ---
# Ordered most-specific first so director_general is caught before director.
_ROLE_FLAGS: list[tuple[str, re.Pattern]] = [
    ("fue_presidente_mun",   re.compile(r"^\s*presidenta?\s+municipal\b", re.I)),
    ("fue_presidente_org",   re.compile(r"^\s*presidenta?\b(?!\s+municipal)", re.I)),
    ("fue_director_general", re.compile(r"\bdirectora?\s+general\b", re.I)),
    ("fue_secretario_cargo", re.compile(r"^\s*secretari[ao]\b", re.I)),
    ("fue_subsecretario",    re.compile(r"^\s*subsecretari[ao]\b", re.I)),
    ("fue_director",         re.compile(r"^\s*directora?\b", re.I)),
    ("fue_coordinador",      re.compile(r"^\s*coordinador[a]?\b", re.I)),
    ("fue_delegado",         re.compile(r"^\s*delegad[ao]\b", re.I)),
    ("fue_asesor",           re.compile(r"^\s*asesor[a]?\b", re.I)),
    ("fue_regidor",          re.compile(r"^\s*regidor[a]?\b", re.I)),
    ("fue_sindico",          re.compile(r"^\s*s[íi]ndico\b", re.I)),
]

# --- Flags de tipo de institución ---
_INST_FLAGS: list[tuple[str, re.Pattern]] = [
    ("admin_en_partido", re.compile(
        r"\bpartido\b|\b(?:pri|pan|prd|morena|pvem|pt|pst|mc|panal|pes)\b",
        re.I,
    )),
    ("admin_en_sindicato", re.compile(
        r"\bsindicato\b|\bctm\b|\bcrom\b|\bcnte\b|\bsnte\b|\bfstse\b"
        r"|\buni[oó]n\s+de\s+trabajadores\b|\bcongreso\s+del\s+trabajo\b",
        re.I,
    )),
    ("admin_en_universidad", re.compile(
        r"\buniversidad\b|\bunam\b|\bipn\b|\bitesm\b"
        r"|\btecnol[oó]gico\s+de\s+monterrey\b",
        re.I,
    )),
    ("admin_en_gobierno_fed", re.compile(
        r"\bgobierno\s+federal\b|\bimss\b|\bissste\b|\bpemex\b|\bcfe\b"
        r"|\bsagarpa\b|\bsectur\b|\bpgr\b|\bsre\b|\bsct\b|\bstps\b"
        r"|\bsedesol\b|\bssa\b|\bsemar\b|\bsedena\b|\bshcp\b"
        r"|\bbanxico\b|\bnafinsa\b|\binfonavit\b|\bfovissste\b",
        re.I,
    )),
    ("admin_en_gobierno_est", re.compile(
        r"\bgobierno\s+del?\s+estado\b|\bgobierno\s+estatal\b"
        r"|\bgobernador[a]?\b|\ben\s+el\s+gobierno\s+de\b",
        re.I,
    )),
    ("admin_en_gobierno_mun", re.compile(
        r"\bpresidente?\s+municipal\b|\bayuntamiento\b"
        r"|\bpresidencia\s+municipal\b|\bgobierno\s+municipal\b",
        re.I,
    )),
]

# --- Nivel de cargo (seniority ordinal 0–5), checked in descending order ---
_SENIORITY_LEVELS: list[tuple[int, re.Pattern]] = [
    (5, re.compile(r"^\s*presidenta?\s+municipal\b", re.I)),
    (5, re.compile(r"\bgobernador[a]?\b", re.I)),
    (5, re.compile(r"^\s*vicepresidente?a?\b", re.I)),
    (5, re.compile(r"^\s*s[íi]ndico\b", re.I)),
    (5, re.compile(r"^\s*presidenta?\b(?!\s+municipal)", re.I)),
    (4, re.compile(r"\bdirectora?\s+general\b", re.I)),
    (4, re.compile(r"^\s*secretari[ao]\b", re.I)),
    (4, re.compile(r"^\s*subsecretari[ao]\b", re.I)),
    (3, re.compile(r"^\s*directora?\b", re.I)),
    (3, re.compile(r"^\s*jef[ae]\b", re.I)),
    (3, re.compile(r"^\s*gerente\b", re.I)),
    (3, re.compile(r"^\s*subdirector[a]?\b", re.I)),
    (3, re.compile(r"^\s*titular\b", re.I)),
    (2, re.compile(r"^\s*coordinador[a]?\b", re.I)),
    (2, re.compile(r"^\s*delegad[ao]\b", re.I)),
    (2, re.compile(r"^\s*representante\b", re.I)),
    (2, re.compile(r"^\s*regidor[a]?\b", re.I)),
    (2, re.compile(r"^\s*fundador[a]?\b", re.I)),
    (2, re.compile(r"^\s*dirigente\b", re.I)),
    (1, re.compile(r"^\s*(?:miembro|integrante)\b", re.I)),
    (1, re.compile(r"^\s*asesor[a]?\b", re.I)),
    (1, re.compile(r"^\s*vocal\b", re.I)),
    (1, re.compile(r"^\s*consejero?a?\b", re.I)),
]

# --- Patrones de liderazgo juvenil ---
# Keywords that indicate a youth-related entry
_JUV_KEYWORD = re.compile(
    r"\b(?:juven[a-záéíóúüñ]*|jov[a-záéíóúüñ]*)\b", re.I
)
# High-level leadership roles (presidenta?, director general, sec. general, etc.)
_JUV_LIDER_ROLE = re.compile(
    r"^\s*(?:"
    r"presidenta?|"
    r"directora?\s+general|directora?\b|"
    r"secretario\s+general|"
    r"coordinador[a]?\s+(?:general|nacional)|"
    r"dirigente|fundador[a]?|subsecretari[ao]"
    r")\b",
    re.I,
)
# Mid-level cargo roles
_JUV_CARGO_ROLE = re.compile(
    r"^\s*(?:secretari[ao]|coordinador[a]?|delegad[ao]|representante)\b", re.I
)
# Party youth wings
_JUV_PARTIDO_ORG = re.compile(
    r"\b(?:pri|pan|prd|morena|pvem|pst|cnop|convergencia)\b"
    r"|\bfrente\s+juvenil\b|\bacci[oó]n\s+juvenil\b|\bvanguardia\s+juvenil\b"
    r"|\bjuventudes?\s+(?:pri[a-z]*|pan[a-z]*|revolucionari[ao]s?|populares?)\b"
    r"|\bjuventud\s+(?:revolucionaria|popular|democr[aá]tica)\b"
    r"|\bmovimiento\b.{0,40}\bjuvenil\b",
    re.I,
)
# Government youth institutes
_JUV_GOBIERNO_ORG = re.compile(
    r"\binstituto\s+(?:mexicano|nacional|estatal|potosino|poblano|mexiquense"
    r"|de\s+la\s+juventud|(?:\w+\s+)?de\s+la\s+juventud)\b"
    r"|\bcausa\s+joven\b|\bcrea\b|\bimjuve\b"
    r"|\bprograma\b.{0,30}\bjuventud\b"
    r"|\bconsejo\b.{0,30}\bjuventud\b",
    re.I,
)


def _extract_trayectoria_admin(df: pd.DataFrame, leg_name: str = "?") -> pd.DataFrame:
    """
    Extrae características estructuradas de 'trayectoria_administrativa'.

    El campo es texto libre (~60k valores únicos), por lo que la extracción
    usa patrones regex en lugar de mapeo de nombres.

    Filtro previo: se descartan entradas con Experiencia vacía o con encabezados
    de sección filtrados incorrectamente por el scraper ('Del año' = 'TRAYECTORIA...').

    Columnas generadas:
      n_trayectoria_admin     — entradas válidas (mismo criterio que las demás trayectorias)

      Flags de rol (1 si el legislador tuvo ese cargo en alguna entrada):
        fue_presidente_mun    fue_presidente_org    fue_director_general
        fue_secretario_cargo  fue_subsecretario     fue_director
        fue_coordinador       fue_delegado          fue_asesor
        fue_regidor           fue_sindico

      Flags de institución (1 si alguna entrada menciona esa institución):
        admin_en_partido      admin_en_sindicato    admin_en_universidad
        admin_en_gobierno_fed admin_en_gobierno_est admin_en_gobierno_mun

      nivel_cargo_max   — máximo nivel jerárquico alcanzado (0–5):
                          0=sin clasificar, 1=miembro/asesor, 2=coordinador/regidor,
                          3=director/jefe, 4=director_general/secretario, 5=presidente/gobernador

      Variables de liderazgo juvenil (entradas con keywords 'juvenil'/'joven'):
        tiene_exp_juvenil     — 1 si alguna entrada tiene keyword de juventud
        lider_juvenil_partido — 1 si tuvo cargo de liderazgo en ala juvenil de partido
        lider_juvenil_gobierno— 1 si tuvo cargo en instituto gubernamental de juventud
        miembro_org_juvenil   — 1 si participó en org juvenil sin rol de liderazgo
        nivel_liderazgo_juvenil — ordinal 0–3 (0=ninguno, 1=participación,
                                  2=cargo, 3=liderazgo)
    """
    parsed = df["trayectoria_administrativa"].apply(_safe_parse)

    def _process_row(items: list) -> dict:
        clean_exps = [
            item["Experiencia"].strip()
            for item in items
            if item.get("Experiencia", "").strip()
            and not _RE_TRAY_HEADER.search(item.get("Del año", ""))
        ]

        role_flags  = {col: 0 for col, _ in _ROLE_FLAGS}
        inst_flags  = {col: 0 for col, _ in _INST_FLAGS}
        max_level   = 0
        tiene_juv   = 0
        lider_pdo   = 0
        lider_gob   = 0
        miembro_juv = 0
        max_juv_lv  = 0

        for exp in clean_exps:
            for col, pat in _ROLE_FLAGS:
                if not role_flags[col] and pat.search(exp):
                    role_flags[col] = 1

            for col, pat in _INST_FLAGS:
                if not inst_flags[col] and pat.search(exp):
                    inst_flags[col] = 1

            for level, pat in _SENIORITY_LEVELS:
                if pat.search(exp):
                    if level > max_level:
                        max_level = level
                    break

            if _JUV_KEYWORD.search(exp):
                tiene_juv = 1
                is_lider  = bool(_JUV_LIDER_ROLE.search(exp))
                is_cargo  = bool(_JUV_CARGO_ROLE.search(exp))
                is_pdo    = bool(_JUV_PARTIDO_ORG.search(exp))
                is_gob    = bool(_JUV_GOBIERNO_ORG.search(exp))

                if is_lider and is_pdo:
                    lider_pdo = 1
                if is_lider and is_gob:
                    lider_gob = 1
                if not (is_lider or is_cargo):
                    miembro_juv = 1

                juv_lv = 3 if is_lider else (2 if is_cargo else 1)
                if juv_lv > max_juv_lv:
                    max_juv_lv = juv_lv

        return {
            "n_trayectoria_admin": len(clean_exps),
            **role_flags,
            **inst_flags,
            "nivel_cargo_max":          max_level,
            "tiene_exp_juvenil":        tiene_juv,
            "lider_juvenil_partido":    lider_pdo,
            "lider_juvenil_gobierno":   lider_gob,
            "miembro_org_juvenil":      miembro_juv,
            "nivel_liderazgo_juvenil":  max_juv_lv,
        }

    if "trayectoria_administrativa" in df.columns:
        results    = parsed.apply(_process_row)
        result_df  = pd.DataFrame(list(results), index=df.index)
        for col in result_df.columns:
            df[col] = result_df[col].astype(int)
        df.drop(columns=["trayectoria_administrativa"], inplace=True)
    else:
        df["n_trayectoria_admin"] = 0
        for col, _ in _ROLE_FLAGS:
            df[col] = 0
        for col, _ in _INST_FLAGS:
            df[col] = 0
        for col in ("nivel_cargo_max", "tiene_exp_juvenil", "lider_juvenil_partido",
                    "lider_juvenil_gobierno", "miembro_org_juvenil", "nivel_liderazgo_juvenil"):
            df[col] = 0

    juv_cols = ["tiene_exp_juvenil", "lider_juvenil_partido",
                "lider_juvenil_gobierno", "miembro_org_juvenil"]
    logger.info(
        "[%s] trayectoria_admin: n_entradas=%d | nivel_max≥3: %d | "
        "exp_juvenil=%d, lider_pdo=%d, lider_gob=%d, miembro=%d",
        leg_name,
        df["n_trayectoria_admin"].sum(),
        (df["nivel_cargo_max"] >= 3).sum(),
        df["tiene_exp_juvenil"].sum(),
        df["lider_juvenil_partido"].sum(),
        df["lider_juvenil_gobierno"].sum(),
        df["miembro_org_juvenil"].sum(),
    )
    return df


# ---------------------------------------------------------------------------
# Extracción de trayectoria_academica
# ---------------------------------------------------------------------------

_ACAD_POSGRADO  = re.compile(r'\bmaestría\b|\bmaestria\b|\bmaster\b|\bmba\b|\bdoctorado\b|\bphd\b|\bdoctor\s+en\b|\bespecialidad\b|\bespecialización\b', re.I)
_ACAD_DOCTORADO = re.compile(r'\bdoctorado\b|\bphd\b|\bdoctor\s+en\b', re.I)

_ACAD_UNIV_PUBLICA = re.compile(
    r'\bunam\b|\bipn\b|\buam\b'
    r'|\buniversidad\s+(?:autónoma|veracruzana|michoacana|de\s+guadalajara'
    r'|de\s+colima|de\s+guanajuato|de\s+sonora|de\s+sinaloa|de\s+yucatán'
    r'|de\s+occidente|de\s+nayarit|de\s+quintana\s+roo)\b'
    r'|\bua[a-z]{1,3}\b',  # UAM, UANL, UABJ, UAQ, UABC, etc.
    re.I,
)
_ACAD_UNIV_PRIVADA = re.compile(
    r'\bitesm\b|\btecnológico\s+de\s+monterrey\b|\btecnol[oó]gico\s+de\s+monterrey\b'
    r'|\bitam\b|\biberoamericana\b|\banáhuac\b|\banahuac\b'
    r'|\bescuela\s+libre\s+de\s+derecho\b|\bpanamericana\b'
    r'|\bla\s+salle\b|\bdel\s+valle\s+de\s+m[eé]xico\b'
    r'|\bcrist[oó]bal\s+col[oó]n\b|\bclaustro\b|\bmonterre[yi]\b'
    r'|\buniversidad\s+(?:del\s+norte|regiomontana|a[eé]rea)\b',
    re.I,
)
_ACAD_UNIV_EXTRANJERA = re.compile(
    r'\b(?:london|harvard|oxford|yale|sorbonne|cambridge|columbia'
    r'|stanford|princeton|georgetown|sciences\s+po'
    r'|politechnic|polytechnic)\b'
    r'|\b(?:de\s+)?(?:alemania|españa|francia|estados\s+unidos|eeuu|eua'
    r'|reino\s+unido|inglaterra|inglaterra|holanda|italia|canadá)\b'
    r'|\b(?:in|of)\s+(?:london|england|paris|berlin|madrid|rome)\b',
    re.I,
)

_ACAD_TOP10: list[tuple[str, re.Pattern]] = [
    ("acad_unam",    re.compile(r'\bunam\b', re.I)),
    ("acad_itesm",   re.compile(r'\bitesm\b|\btecnol[oó]gico\s+de\s+monterrey\b', re.I)),
    ("acad_itam",    re.compile(r'\bitam\b', re.I)),
    ("acad_ibero",   re.compile(r'\biberoamericana\b', re.I)),
    ("acad_udg",     re.compile(r'\buniversidad\s+de\s+guadalajara\b', re.I)),
    ("acad_ipn",     re.compile(r'\bipn\b|\bpolitécnico\s+nacional\b', re.I)),
    ("acad_uam",     re.compile(r'\buam\b|\buniversidad\s+aut[oó]noma\s+metropolitana\b', re.I)),
    ("acad_anahuac", re.compile(r'\ban[aá]huac\b', re.I)),
    ("acad_uanl",    re.compile(r'\buanl\b|\buniversidad\s+aut[oó]noma\s+de\s+nuevo\s+le[oó]n\b', re.I)),
    ("acad_uv",      re.compile(r'\buniversidad\s+veracruzana\b', re.I)),
]

_ACAD_BINARY_COLS = [
    "tiene_posgrado", "tiene_doctorado", "estudios_en_extranjero",
    "univ_publica", "univ_privada", "univ_extranjera",
] + [col for col, _ in _ACAD_TOP10]


def _extract_trayectoria_academica(df: pd.DataFrame, leg_name: str = "?") -> pd.DataFrame:
    """
    Extrae variables binarias de 'trayectoria_academica'.

    Columnas generadas (todas 0/1):
      tiene_posgrado        — al menos una entrada de maestría, doctorado o especialidad
      tiene_doctorado       — al menos una entrada de doctorado o PhD
      estudios_en_extranjero — al menos una entrada con institución extranjera

      univ_publica          — asistió a universidad pública (UNAM, IPN, UAM, autónomas estatales)
      univ_privada          — asistió a universidad privada (ITESM, ITAM, Ibero, Anáhuac, etc.)
      univ_extranjera       — asistió a institución en el extranjero

      acad_unam … acad_uv   — flags por institución para el top-10 de frecuencia

    La columna 'trayectoria_academica' original se elimina al finalizar.
    """
    parsed = df["trayectoria_academica"].apply(_safe_parse) if "trayectoria_academica" in df.columns else None

    if parsed is None:
        for col in _ACAD_BINARY_COLS:
            df[col] = 0
        logger.debug("[%s] trayectoria_academica ausente — rellenado con 0", leg_name)
        return df

    def _process_row(items: list) -> dict:
        out = {col: 0 for col in _ACAD_BINARY_COLS}
        for item in items:
            exp = item.get("Experiencia", "").strip()
            if not exp:
                continue
            if _ACAD_POSGRADO.search(exp):
                out["tiene_posgrado"] = 1
            if _ACAD_DOCTORADO.search(exp):
                out["tiene_doctorado"] = 1
            if _ACAD_UNIV_EXTRANJERA.search(exp):
                out["estudios_en_extranjero"] = 1
                out["univ_extranjera"] = 1
            if _ACAD_UNIV_PUBLICA.search(exp):
                out["univ_publica"] = 1
            if _ACAD_UNIV_PRIVADA.search(exp):
                out["univ_privada"] = 1
            for col, pat in _ACAD_TOP10:
                if not out[col] and pat.search(exp):
                    out[col] = 1
        return out

    results   = parsed.apply(_process_row)
    result_df = pd.DataFrame(list(results), index=df.index)
    for col in _ACAD_BINARY_COLS:
        df[col] = result_df[col].astype(int)

    logger.info(
        "[%s] trayectoria_academica: posgrado=%d, doctorado=%d, extranjero=%d"
        " | pub=%d, priv=%d, ext=%d",
        leg_name,
        df["tiene_posgrado"].sum(), df["tiene_doctorado"].sum(),
        df["estudios_en_extranjero"].sum(),
        df["univ_publica"].sum(), df["univ_privada"].sum(), df["univ_extranjera"].sum(),
    )

    df.drop(columns=["trayectoria_academica"], inplace=True)
    return df


def _extract_trayectorias(df: pd.DataFrame, leg_name: str = "?") -> pd.DataFrame:
    """
    Extrae conteos de entradas de las columnas de trayectoria y secciones auxiliares.

    Cada columna listada en `trayectoria_cols` contiene una lista JSON de
    dicts (experiencias/eventos). Solo se necesita el conteo de entradas,
    no su contenido detallado, para el análisis de ML.

    Si una columna no existe en el DataFrame (varía entre legislaturas),
    se crea la columna de conteo con valor 0 para todas las filas.

    Las columnas JSON originales se eliminan al finalizar.
    """
    # Mapa: nombre de columna JSON en el scraper → nombre de conteo en el output.
    # trayectoria_administrativa is handled by _extract_trayectoria_admin (called before this).
    # trayectoria_academica handled by _extract_trayectoria_academica (called before this).
    # trayectoria_administrativa handled by _extract_trayectoria_admin (called before this).
    trayectoria_cols = {
        "trayectoria_legislativa":     "n_trayectoria_legislativa",
        "trayectoria_politica":        "n_trayectoria_politica",
        "trayectoria_empresarial":     "n_trayectoria_empresarial",
        "investigacion_docencia":      "n_investigacion_docencia",   # LXVI+; 0 en legislaturas anteriores
        "organos_de_gobierno":         "n_organos_gobierno",
    }

    for raw_col, feat_col in trayectoria_cols.items():
        if raw_col in df.columns:
            df[feat_col] = _count_json(df[raw_col]).astype(int)
            n_con_datos = (df[feat_col] > 0).sum()
            logger.debug(
                "[%s] %-35s → %-30s | %d filas con datos, total entradas=%d",
                leg_name, raw_col, feat_col, n_con_datos, df[feat_col].sum(),
            )
            df.drop(columns=[raw_col], inplace=True)
        else:
            # Columna ausente en esta legislatura — se rellena con 0 para
            # mantener el esquema uniforme entre todas las legislaturas.
            df[feat_col] = 0
            logger.debug("[%s] %-35s ausente — rellenado con 0", leg_name, raw_col)

    logger.info(
        "[%s] trayectorias extraídas: admin=%d, leg=%d, pol=%d, emp=%d",
        leg_name,
        df["n_trayectoria_admin"].sum(),
        df["n_trayectoria_legislativa"].sum(),
        df["n_trayectoria_politica"].sum(),
        df["n_trayectoria_empresarial"].sum(),
    )

    return df


# ---------------------------------------------------------------------------
# Función principal
# ---------------------------------------------------------------------------

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extrae todas las características de columnas JSON y devuelve un DataFrame plano.

    Este es el punto de entrada que llama pipeline.py. Recibe el DataFrame
    de clean.py (con las columnas JSON todavía como str) y devuelve el
    DataFrame completamente plano que save.py escribirá al disco.

    Pasos:
      1. Extraer características de 'comisiones' → _extract_comisiones()
      2. Extraer flags estructurados de 'trayectoria_administrativa' → _extract_trayectoria_admin()
         (las columnas ya vienen limpias de normalize.py; el filtro _RE_TRAY_HEADER es defensivo)
      3. Extraer variables binarias de 'trayectoria_academica' → _extract_trayectoria_academica()
      4. Extraer conteos de las demás columnas JSON → _extract_trayectorias()
         (incluye 'investigacion_docencia' extraída por normalize.py de otros_rubros)

    Parámetros
    ----------
    df : pd.DataFrame — salida de clean.py, con índice diputado_id.

    Retorna
    -------
    pd.DataFrame completamente plano, sin columnas JSON, listo para guardar.
    """
    df = df.copy()  # no modificar el DataFrame que recibió pipeline.py

    # Identificar la legislatura desde la primera fila para los mensajes de log.
    leg_name = str(df["legislatura_num"].iloc[0]) if "legislatura_num" in df.columns else "?"

    # Paso 1: comisiones (requiere lógica adicional para roles de liderazgo).
    df = _extract_comisiones(df, leg_name)

    # Paso 2: trayectoria_administrativa (flags de rol, institución, seniority, juventud).
    df = _extract_trayectoria_admin(df, leg_name)

    # Paso 3: trayectoria_academica (flags binarios de grado, institución, extranjero).
    df = _extract_trayectoria_academica(df, leg_name)

    # Paso 4: todas las demás columnas JSON (solo necesitan conteo de entradas).
    df = _extract_trayectorias(df, leg_name)

    logger.info(
        "[%s] transformación completada → %d filas × %d columnas",
        leg_name, len(df), len(df.columns),
    )
    return df
