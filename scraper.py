#!/usr/bin/env python3
"""
scraper.py — Raspador de perfiles legislativos del SIL.

FUNCIÓN EN EL PROYECTO:
    Es el primer paso del flujo completo. Descarga los perfiles de los
    legisladores directamente del sitio web del SIL y los guarda como
    CSV crudos. Su salida es la entrada del pipeline ETL (pipeline.py).

    Se ejecuta de forma independiente al ETL; no llama a ningún módulo
    del paquete etl/. El ETL lee los CSV que este script produce.

FUENTE DE DATOS:
    Sistema de Información Legislativa (SIL)
    URL base: https://sil.gobernacion.gob.mx
    Cámara  : Diputados (Cámara=1)
    Cobertura: Legislaturas LVII–LXVI (1997–presente), ~500 diputados c/u.

SALIDA:
    data/scraper/<run_ts>/
        <LEGISLATURA>.csv   ← una fila por legislador, 36 columnas crudas
        scraper.log         ← log completo de la corrida

    El directorio <run_ts> (formato YYYYMMDD_HHMMSS) identifica de forma
    única cada corrida del raspador, permitiendo conservar históricos y
    que el ETL elija qué corrida procesar con --input-dir.

REANUDACIÓN AUTOMÁTICA:
    Si el scraper se interrumpe, relanzarlo con los mismos argumentos
    retoma desde donde quedó: lee las referencias ya guardadas en el CSV
    y omite los perfiles que ya fueron raspados.

FLUJO INTERNO:
    main()
      └─ run_legislature(leg_name, leg_num, run_dir)
           ├─ get_parties(leg_num)              # lista de partidos del SIL
           ├─ get_legislator_refs(party_url)    # IDs de legisladores por partido
           └─ scrape_profile(referencia)        # datos completos de cada perfil
                ├─ parse_tftable(soup)          # datos personales (tabla principal)
                └─ parse_tftable2(tabla)        # secciones anidadas (comisiones, trayectorias)

USO:
    python scraper.py --legislatura LXVI
    python scraper.py --legislatura LXIV,LXV,LXVI
    python scraper.py --legislatura all
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import re
import time
import unicodedata
from urllib.parse import urljoin

import requests
import urllib3
from bs4 import BeautifulSoup

# El sitio tiene una cadena SSL incompleta (GoDaddy sin intermediate cert).
# Desactivamos las advertencias para no contaminar la salida del log.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------------------------
# Configuración general
# ---------------------------------------------------------------------------

# URL base del SIL; todos los paths relativos se resuelven contra esta raíz.
BASE_URL = "https://sil.gobernacion.gob.mx"

# Codificación que usa el sitio en sus páginas HTML.
ENCODING = "windows-1252"

# Pausa (segundos) entre peticiones HTTP para no saturar el servidor.
DELAY = 1.5

# Mapa nombre romano → número entero de legislatura.
# Usado para construir los parámetros de URL (?Legislatura=62, etc.)
LEGISLATURAS = {
    "LVII":  57,
    "LVIII": 58,
    "LIX":   59,
    "LX":    60,
    "LXI":   61,
    "LXII":  62,
    "LXIII": 63,
    "LXIV":  64,
    "LXV":   65,
    "LXVI":  66,
}

# Columnas del CSV de salida, en orden fijo.
# Las secciones anidadas (comisiones, trayectorias) se guardan como
# cadenas JSON dentro de la celda correspondiente.
CSV_COLUMNS = [
    # --- Identificadores ---
    "diputado_id",          # ID único estable entre legislaturas: sha256(nombre_norm|nacimiento)[:12]
    "referencia",           # ID numérico del perfil en el SIL (varía por legislatura)
    "legislatura_num",      # Número entero de la legislatura (66, etc.)
    "profile_url",          # URL directa al perfil raspado
    # --- Datos personales (tabla TFtable) ---
    "nombre",               # Nombre completo del legislador (extraído de tituloN)
    "numero_de_la_legislatura",
    "periodo_de_la_legislatura",
    "partido",
    "nacimiento",
    "entidad",
    "ciudad",
    "principio_de_eleccion",
    "ubicacion",
    "correo_electronico",
    "telefono",
    "suplente",
    "suplente_referencia",  # ID del suplente en el SIL (si existe)
    "ultimo_grado_de_estudios",
    "preparacion_academica",
    "experiencia_legislativa",
    "redes_sociales",
    # --- Secciones anidadas (tablas TFtable2), serializadas como JSON ---
    "comisiones",                          # [{Comisión, Puesto, Fecha Inicial, Fecha Final, Estatus}]
    "licencias_reincorporaciones",         # [{Del año, Al año, Experiencia}] — ausencias y reincorporaciones
    "trayectoria_administrativa",          # [{Del año, Al año, Experiencia}]
    "trayectoria_legislativa",             # [{Del año, Al año, Experiencia}]
    "trayectoria_politica",                # [{Del año, Al año, Experiencia}]
    "trayectoria_academica",               # [{Del año, Al año, Experiencia}]
    "trayectoria_empresarial",             # [{Del año, Al año, Experiencia}] — iniciativa privada
    "otros_rubros",                        # [{Del año, Al año, Experiencia}]
    "organos_de_gobierno",                 # [{...}] — participación en órganos de gobierno
    "observaciones",                       # [{...}] — notas varias del perfil
    # --- Estado del legislador ---
    # --- Diagnóstico ---
    "error",  # Vacío si todo salió bien; mensaje de error en caso contrario
]

# Directorio base; el subdirectorio de la corrida se crea en main().
_BASE_SCRAPER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "scraper")


# ---------------------------------------------------------------------------
# Sesión HTTP compartida
# ---------------------------------------------------------------------------

# Usamos una sesión para reutilizar conexiones TCP y compartir headers/cookies.
session = requests.Session()
session.verify = False  # SSL chain rota — ignorar verificación de certificado
session.headers.update({
    # User-Agent de navegador real para evitar bloqueos por bot-detection básico.
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    # Idioma preferido: español México, para recibir respuestas en español.
    "Accept-Language": "es-MX,es;q=0.9,en;q=0.8",
})


def fetch(url, **kwargs):
    """
    Descarga una URL y devuelve un objeto BeautifulSoup.

    Reintenta hasta 3 veces con pausa de 3s entre intentos.
    Devuelve None si todos los intentos fallan.

    El contenido se decodifica con ENCODING (windows-1252) porque el SIL
    usa esa codificación en sus páginas HTML.
    """
    for intento in range(3):
        try:
            resp = session.get(url, timeout=30, **kwargs)
            resp.raise_for_status()
            html = resp.content.decode(ENCODING, errors="replace")
            logger.debug("GET %s → %s (%s chars)", url, resp.status_code, len(html))
            return BeautifulSoup(html, "html.parser")
        except Exception as e:
            logger.warning("Intento %s/3 fallido para %s: %s", intento + 1, url, e)
            if intento < 2:
                time.sleep(3)
    logger.error("✗ Todos los reintentos fallaron: %s", url)
    return None


# ---------------------------------------------------------------------------
# Extracción de partidos políticos
# ---------------------------------------------------------------------------

def get_parties(leg_num):
    """
    Obtiene la lista de partidos políticos para una legislatura dada,
    en dos grupos:

      - Activos (Estatus=A):       legisladores que ejercieron el cargo completo
      - En licencia (ENFUNCION=N): legisladores que tomaron licencia y fueron
                                   sustituidos por su suplente

    Cada entrada incluye el flag `en_licencia` (True/False) para identificar
    a qué grupo pertenece el legislador en el CSV final.

    Se omiten las filas "Totales" (sin parámetro Partido=).

    Devuelve lista de dicts [{name, url, count, en_licencia}],
    o lista vacía si falla la carga.
    """
    url = f"{BASE_URL}/Numeralia/Legisladores/NumeraliaLegisladores.php"
    logger.info("Cargando partidos para legislatura %s...", leg_num)
    soup = fetch(url, params={"SID": "", "Legislatura": leg_num, "Camara": 1})
    if not soup:
        logger.error("✗ No se pudo cargar la página de partidos para legislatura %s", leg_num)
        return []

    partidos = []
    vistos = set()  # evitar duplicados si la misma URL aparece más de una vez

    for tr in soup.find_all("tr"):
        # Recolectamos enlaces de ambos grupos: activos (Estatus=A) y en licencia (ENFUNCION=N)
        enlaces_activos = [
            a for a in tr.find_all("a", href=True)
            if "resultadosNumeraliaLegisladores" in a["href"]
            and "Estatus=A" in a["href"]
            and "Partido=" in a["href"]
        ]
        enlaces_licencia = [
            a for a in tr.find_all("a", href=True)
            if "resultadosNumeraliaLegisladores" in a["href"]
            and "ENFUNCION=N" in a["href"]
            and "Partido=" in a["href"]
        ]

        for enlaces, en_licencia in [(enlaces_activos, False), (enlaces_licencia, True)]:
            if not enlaces:
                continue
            celdas = tr.find_all("td")
            nombre = celdas[0].get_text(strip=True) if celdas else "Desconocido"
            url_partido = urljoin(BASE_URL, enlaces[0]["href"])

            if url_partido in vistos:
                continue
            vistos.add(url_partido)

            partidos.append({
                "name": nombre,
                "url": url_partido,
                "count": enlaces[0].get_text(strip=True),
                "en_licencia": en_licencia,
            })

    activos = sum(1 for p in partidos if not p["en_licencia"])
    en_licencia = sum(1 for p in partidos if p["en_licencia"])
    logger.info(
        "✓ Legislatura %s: %s grupos activos, %s grupos en licencia",
        leg_num, activos, en_licencia,
    )
    for p in partidos:
        estado = "EN LICENCIA" if p["en_licencia"] else "activo"
        logger.debug("  [%s] %s: %s → %s", estado, p["name"], p["count"], p["url"])

    return partidos


# ---------------------------------------------------------------------------
# Extracción de referencias de legisladores
# ---------------------------------------------------------------------------

def get_legislator_refs(party_url):
    """
    Obtiene los IDs únicos (Referencia) de los legisladores de un partido.

    La página de resultados muestra una tabla con los legisladores.
    Cada fila tiene un atributo onclick con una llamada a window.open()
    que contiene la URL del perfil, p. ej.:
      window.open("/Librerias/pp_PerfilLegislador.php?SID=&Referencia=9216717","leg")

    Extraemos el valor de Referencia con una expresión regular y
    deduplicamos con un set (cada legislador aparece dos veces por fila:
    en el tr y en el enlace del nombre).

    Devuelve lista de strings con los IDs únicos.
    """
    logger.debug("Cargando lista de legisladores: %s", party_url)
    soup = fetch(party_url)
    if not soup:
        logger.error("✗ No se pudo cargar la página de resultados: %s", party_url)
        return []

    referencias = set()
    for elem in soup.find_all(onclick=True):
        m = re.search(r"Referencia=(\d+)", elem.get("onclick", ""))
        if m:
            referencias.add(m.group(1))

    logger.debug("  %s referencias únicas encontradas", len(referencias))
    return list(referencias)


# ---------------------------------------------------------------------------
# Normalización de claves de campos
# ---------------------------------------------------------------------------

def _normalizar_clave(texto_crudo):
    """
    Convierte el encabezado de un campo HTML en una clave Python válida.

    Ejemplo: "Último grado de estudios:" → "ultimo_grado_de_estudios"

    Pasos:
      1. Minúsculas
      2. Eliminar dos puntos finales y espacios extra
      3. Reemplazar espacios por guiones bajos
      4. Eliminar caracteres con acento (á→a, é→e, etc.)
      5. Eliminar caracteres de ruido (°, .)
    """
    return (
        texto_crudo.lower().rstrip(":").strip()
        .replace(" ", "_")
        .replace("á", "a").replace("é", "e").replace("í", "i")
        .replace("ó", "o").replace("ú", "u").replace("ü", "u")
        .replace("ñ", "n").replace("°", "").replace(".", "")
        .strip("_")
    )


# ---------------------------------------------------------------------------
# Parseo de la tabla de datos personales (TFtable)
# ---------------------------------------------------------------------------

def parse_tftable(soup):
    """
    Extrae los datos personales del legislador desde la tabla principal.

    La tabla tiene clase CSS "TFtable" y contiene filas con dos celdas:
      - Primera celda: nombre del campo (en negrita)
      - Segunda celda: valor del campo

    Caso especial para "suplente": la celda contiene un enlace cuyo href
    incluye la Referencia del suplente, que se extrae por separado como
    "suplente_referencia".

    Devuelve dict {clave_normalizada: valor_texto}.
    """
    datos = {}
    tftable = soup.find("table", class_="TFtable")
    if not tftable:
        logger.debug("No se encontró tabla TFtable en el perfil")
        return datos

    for tr in tftable.find_all("tr"):
        celdas = tr.find_all("td")
        if len(celdas) < 2:
            continue

        clave = _normalizar_clave(celdas[0].get_text(strip=True))
        enlace = celdas[1].find("a")

        # El suplente viene como hipervínculo; extraemos ID y nombre por separado
        if clave == "suplente" and enlace:
            ref_m = re.search(r"Referencia=(\d+)", enlace.get("href", ""))
            datos["suplente_referencia"] = ref_m.group(1) if ref_m else ""
            datos[clave] = enlace.get_text(strip=True)
        else:
            # separator=" " para unir texto de etiquetas <br> con espacio
            datos[clave] = celdas[1].get_text(separator=" ", strip=True)

    logger.debug("  Datos personales extraídos: %s", list(datos.keys()))
    return datos


# ---------------------------------------------------------------------------
# Parseo de tablas de trayectoria y comisiones (TFtable2)
# ---------------------------------------------------------------------------

def parse_tftable2(tabla):
    """
    Extrae los datos de una tabla secundaria (clase "TFtable2").

    Estas tablas tienen una fila de encabezados (<th>) seguida de
    filas de datos (<td>). Se devuelve una lista de dicts donde
    cada dict representa una fila, usando los encabezados como claves.

    Si no hay encabezados (caso raro), se usan claves "col_0", "col_1", etc.
    Las filas completamente vacías se omiten.

    Devuelve lista de dicts, o lista vacía si la tabla no tiene datos.
    """
    filas, encabezados = [], []

    for tr in tabla.find_all("tr"):
        ths = tr.find_all("th")
        tds = tr.find_all("td")

        if ths:
            # Fila de encabezados — guardamos para usarlos como claves
            encabezados = [th.get_text(strip=True) for th in ths]
        elif tds:
            if encabezados:
                fila = {
                    encabezados[i]: tds[i].get_text(separator=" ", strip=True)
                    if i < len(tds) else ""
                    for i in range(len(encabezados))
                }
            else:
                # Sin encabezados: fallback a claves posicionales
                fila = {
                    f"col_{i}": td.get_text(separator=" ", strip=True)
                    for i, td in enumerate(tds)
                }
            # Omitir filas donde todos los valores son vacíos
            if any(v for v in fila.values()):
                filas.append(fila)

    return filas


# ---------------------------------------------------------------------------
# Detección de sección para cada TFtable2
# ---------------------------------------------------------------------------

def _etiqueta_seccion(tabla):
    """
    Determina a qué sección pertenece una tabla TFtable2.

    Cada tabla TFtable2 está precedida por una tabla con clase "datosL2"
    que contiene el nombre de la sección (TRAYECTORIA ADMINISTRATIVA,
    TRAYECTORIA LEGISLATIVA, etc.).

    Buscamos hacia atrás en el DOM el elemento datosL2 más cercano y
    devolvemos su texto en mayúsculas para facilitar la comparación.

    Si no hay datosL2 precedente (primera tabla = comisiones), devuelve "".
    """
    anterior = tabla.find_previous("table", class_="datosL2")
    if anterior:
        texto = re.sub(r"\s+", " ", anterior.get_text(strip=True)).upper()
        logger.debug("  Etiqueta de sección detectada: %r", texto)
        return texto
    return ""


# ---------------------------------------------------------------------------
# Extracción de nombre e ID único del legislador
# ---------------------------------------------------------------------------

# Prefijos de cargo que aparecen antes del nombre en el elemento tituloN.
# Se eliminan para quedarnos solo con el nombre propio.
_PREFIJOS_CARGO = re.compile(
    r"^(Diputad[oa]|Senador[a]?|C\.|Lic\.|Dr\.|Dra\.|Ing\.|Mtro\.|Mtra\.)\s+",
    re.IGNORECASE,
)


def extraer_nombre(soup):
    """
    Extrae el nombre completo del legislador desde el elemento con clase 'tituloN'.

    El sitio muestra el nombre con un prefijo de cargo, por ejemplo:
      "Diputado Carlos Humberto Aceves del Olmo"
      "Diputada María Luisa García Jiménez"

    Se elimina el prefijo y se devuelve solo el nombre propio.
    Devuelve cadena vacía si el elemento no existe.
    """
    titulo = soup.find(class_="tituloN")
    if not titulo:
        return ""
    texto = titulo.get_text(separator=" ", strip=True)
    # Eliminar prefijo de cargo (Diputado/a, Sen., etc.)
    nombre = _PREFIJOS_CARGO.sub("", texto).strip()
    return nombre


def _normalizar_para_id(texto):
    """
    Normaliza un texto para usarlo como componente del ID único.

    Pasos:
      1. Minúsculas
      2. Eliminar acentos y diacríticos (NFD → solo caracteres ASCII base)
      3. Eliminar caracteres que no sean letras o números
      4. Colapsar espacios múltiples
    """
    # Descomponer caracteres acentuados y eliminar los diacríticos
    sin_acentos = unicodedata.normalize("NFD", texto)
    sin_acentos = "".join(c for c in sin_acentos if unicodedata.category(c) != "Mn")
    # Minúsculas, solo alfanumérico y espacios
    limpio = re.sub(r"[^a-z0-9 ]", "", sin_acentos.lower())
    return re.sub(r"\s+", " ", limpio).strip()


def generar_diputado_id(nombre, nacimiento):
    """
    Genera un ID único y estable para un legislador, usable entre legislaturas.

    El ID es un hash SHA-256 truncado a 12 caracteres hexadecimales,
    calculado sobre la concatenación de nombre normalizado y fecha de nacimiento.

    Ejemplo:
      nombre     = "Carlos Humberto Aceves del Olmo"
      nacimiento = "05/11/1940"
      clave      = "carlos humberto aceves del olmo|05/11/1940"
      id         = sha256(clave)[:12]  →  "12c48785d22a"

    Propiedades:
      - Determinístico: misma persona → mismo ID en cualquier legislatura
      - Estable: no cambia al re-raspar el sitio
      - Bajo riesgo de colisión: 12 hex = 48 bits de entropía
      - Robusto a variaciones de partido o circunscripción

    Si nombre o nacimiento están vacíos, devuelve cadena vacía para evitar
    IDs falsos por datos faltantes.
    """
    nombre_norm = _normalizar_para_id(nombre)
    nacimiento_norm = nacimiento.strip()

    if not nombre_norm or not nacimiento_norm:
        logger.debug("ID no generado: nombre=%r, nacimiento=%r", nombre, nacimiento)
        return ""

    clave = f"{nombre_norm}|{nacimiento_norm}"
    return hashlib.sha256(clave.encode("utf-8")).hexdigest()[:12]


# ---------------------------------------------------------------------------
# Raspado completo de un perfil
# ---------------------------------------------------------------------------

def scrape_profile(referencia):
    """
    Raspa todos los datos del perfil de un legislador dado su ID (referencia).

    Flujo:
      1. Descarga la página pp_PerfilLegislador.php?Referencia=<id>
      2. Extrae datos personales de la tabla TFtable
      3. Clasifica y extrae todas las tablas TFtable2:
           - Primera sin etiqueta → comisiones
           - "ADMINISTRATIVA"     → trayectoria_administrativa
           - "LEGISLATIVA"        → trayectoria_legislativa
           - "POL…"               → trayectoria_politica
           - "ACAD…"              → trayectoria_academica
           - "OTROS" / "RUBRO"    → otros_rubros
      4. Serializa las listas anidadas como JSON para guardar en CSV

    Devuelve dict con todos los campos, incluyendo "error" si algo falló.
    """
    url = f"{BASE_URL}/Librerias/pp_PerfilLegislador.php"
    logger.debug("Raspando perfil referencia=%s", referencia)

    soup = fetch(url, params={"SID": "", "Referencia": referencia})
    if not soup:
        logger.error("✗ No se pudo cargar el perfil %s", referencia)
        return {"referencia": referencia, "error": "fetch_failed"}

    datos = {
        "referencia": referencia,
        "profile_url": f"{url}?SID=&Referencia={referencia}",
        "error": "",
    }

    # Extraer datos personales planos
    datos.update(parse_tftable(soup))

    # Extraer nombre y generar ID único estable entre legislaturas
    nombre = extraer_nombre(soup)
    datos["nombre"] = nombre
    datos["diputado_id"] = generar_diputado_id(nombre, datos.get("nacimiento", ""))
    logger.debug("  nombre=%r, diputado_id=%r", nombre, datos["diputado_id"])

    # Contenedores para cada sección de trayectoria/comisiones
    secciones = {
        "comisiones":                  [],
        "licencias_reincorporaciones": [],
        "trayectoria_administrativa":  [],
        "trayectoria_legislativa":     [],
        "trayectoria_politica":        [],
        "trayectoria_academica":       [],
        "trayectoria_empresarial":     [],
        "otros_rubros":                [],
        "organos_de_gobierno":         [],
        "observaciones":               [],
    }

    tablas_tf2 = soup.find_all("table", class_="TFtable2")
    logger.debug("  TFtable2 encontradas: %s", len(tablas_tf2))

    for i, tabla in enumerate(tablas_tf2):
        etiqueta = _etiqueta_seccion(tabla)
        filas = parse_tftable2(tabla)

        # Clasificar la tabla por su etiqueta de sección.
        # La primera tabla sin etiqueta (índice 0) siempre es comisiones.
        # Algunas legislaturas añaden la etiqueta "COMISIONES" explícitamente.
        if "COMISION" in etiqueta or (i == 0 and not etiqueta):
            secciones["comisiones"] = filas
        elif "LICENCIA" in etiqueta or "REINCORPORAC" in etiqueta:
            secciones["licencias_reincorporaciones"] = filas
        elif "ADMINISTRATIVA" in etiqueta:
            secciones["trayectoria_administrativa"] = filas
        elif "LEGISLATIVA" in etiqueta:
            secciones["trayectoria_legislativa"] = filas
        elif "POL" in etiqueta:          # cubre "POLÍTICA" y variantes
            secciones["trayectoria_politica"] = filas
        elif "ACAD" in etiqueta:         # cubre "ACADÉMICA" y variantes
            secciones["trayectoria_academica"] = filas
        elif "EMPRESARIAL" in etiqueta or "INICIATIVA PRIVADA" in etiqueta:
            secciones["trayectoria_empresarial"] = filas
        elif "ORGANO" in etiqueta or "\u00d3RGANO" in etiqueta:  # ÓRGANOS DE GOBIERNO
            secciones["organos_de_gobierno"] = filas
        elif "OTROS" in etiqueta or "RUBRO" in etiqueta:
            secciones["otros_rubros"] = filas
        elif "OBSERVACION" in etiqueta:
            secciones["observaciones"] = filas
        else:
            logger.warning(
                "TFtable2 sin clasificar: etiqueta=%r, ref=%s, índice=%s",
                etiqueta, referencia, i,
            )

    # Serializar listas como JSON para almacenar en una sola celda CSV
    for clave, valor in secciones.items():
        datos[clave] = json.dumps(valor, ensure_ascii=False)
        logger.debug("  %s: %s registros", clave, len(valor))

    return datos


# ---------------------------------------------------------------------------
# Manejo del archivo CSV
# ---------------------------------------------------------------------------

def cargar_refs_existentes(csv_path):
    """
    Lee el CSV existente y devuelve el conjunto de referencias ya raspadas.

    Permite reanudar una ejecución interrumpida sin volver a raspar
    los perfiles que ya fueron procesados.

    Devuelve set vacío si el archivo no existe.
    """
    if not os.path.exists(csv_path):
        logger.info("CSV no existe aún: %s — se creará desde cero", csv_path)
        return set()

    refs = set()
    with open(csv_path, encoding="utf-8", newline="") as f:
        lector = csv.DictReader(f)
        for fila in lector:
            ref = fila.get("referencia", "").strip()
            if ref:
                refs.add(ref)

    logger.info("Reanudando: %s perfiles ya en %s", len(refs), csv_path)
    return refs


def abrir_csv_escritor(csv_path, append=False):
    """
    Abre el archivo CSV para escritura y devuelve (archivo, DictWriter).

    Si append=True, abre en modo append (no escribe encabezado de nuevo).
    Si append=False, crea/sobreescribe el archivo y escribe el encabezado.

    extrasaction="ignore" descarta silenciosamente campos extra en el dict
    que no estén en CSV_COLUMNS (p. ej. claves inesperadas del sitio).
    """
    modo = "a" if append else "w"
    archivo = open(csv_path, modo, encoding="utf-8", newline="")
    escritor = csv.DictWriter(
        archivo,
        fieldnames=CSV_COLUMNS,
        extrasaction="ignore",
        quoting=csv.QUOTE_ALL,
    )
    if not append:
        escritor.writeheader()
        logger.debug("CSV creado con encabezados: %s", csv_path)
    return archivo, escritor


# ---------------------------------------------------------------------------
# Ejecución por legislatura
# ---------------------------------------------------------------------------

def run_legislature(leg_name, leg_num, run_dir):
    """
    Raspa todos los legisladores de una legislatura y los guarda en CSV.

    Flujo completo:
      1. Determina la ruta del CSV de salida
      2. Carga referencias ya raspadas (reanudación automática)
      3. Obtiene lista de partidos de la legislatura
      4. Para cada partido: obtiene referencias de legisladores
      5. Para cada referencia nueva: raspa el perfil y escribe en CSV
      6. Deduplica referencias entre partidos (un legislador puede
         aparecer en más de un partido en la misma página)

    El CSV se hace flush después de cada fila para minimizar pérdida
    de datos en caso de interrupción.
    """
    csv_path = os.path.join(run_dir, f"{leg_name}.csv")
    refs_raspadas = cargar_refs_existentes(csv_path)
    en_modo_append = bool(refs_raspadas)

    archivo, escritor = abrir_csv_escritor(csv_path, append=en_modo_append)
    total = errores = 0

    try:
        logger.info("=" * 60)
        logger.info("Legislatura %s (número %s)", leg_name, leg_num)
        logger.info("Salida: %s", os.path.abspath(csv_path))
        logger.info("=" * 60)
        time.sleep(DELAY)

        partidos = get_parties(leg_num)
        if not partidos:
            logger.warning("⊘ No se encontraron partidos para %s — omitiendo", leg_name)
            return

        # Set para deduplicar entre partidos dentro de la misma ejecución
        refs_vistas_en_esta_ejecucion = set()

        for partido in partidos:
            logger.info(
                "[%s] Partido: %s (~%s legisladores)",
                leg_name, partido["name"], partido["count"],
            )
            time.sleep(DELAY)

            refs_partido = get_legislator_refs(partido["url"])

            # Filtrar refs ya raspadas y ya vistas en esta ejecución
            refs_nuevas = [
                r for r in refs_partido
                if r not in refs_raspadas
                and r not in refs_vistas_en_esta_ejecucion
            ]
            refs_vistas_en_esta_ejecucion.update(refs_partido)

            logger.info(
                "  %s refs totales, %s ya raspadas, %s nuevas",
                len(refs_partido),
                len(refs_partido) - len(refs_nuevas),
                len(refs_nuevas),
            )

            for ref in refs_nuevas:
                estado = "EN LICENCIA" if partido["en_licencia"] else "activo"
                logger.info("  Raspando ref %s [%s] [%s]", ref, partido["name"], estado)
                time.sleep(DELAY)

                perfil = scrape_profile(ref)
                perfil["legislatura_num"] = leg_num

                escritor.writerow(perfil)
                archivo.flush()  # guardar inmediatamente en disco

                refs_raspadas.add(ref)
                total += 1

                if perfil.get("error"):
                    errores += 1
                    logger.warning("✗ Error en ref %s: %s", ref, perfil["error"])

                # Reporte de progreso cada 50 perfiles
                if total % 50 == 0:
                    logger.info(
                        "[%s] Progreso: %s raspados, %s errores",
                        leg_name, total, errores,
                    )

    finally:
        # Cerrar el archivo aunque haya ocurrido una excepción
        archivo.close()

    logger.info(
        "✓ [%s] Completado: %s perfiles raspados, %s errores → %s",
        leg_name, total, errores, os.path.abspath(csv_path),
    )


# ---------------------------------------------------------------------------
# Interfaz de línea de comandos
# ---------------------------------------------------------------------------

def parse_args():
    """
    Define y parsea los argumentos de línea de comandos.

    --legislatura: obligatorio; acepta un nombre, lista separada por
                   comas, o "all" para todas las legislaturas.
    --delay:       opcional; pausa en segundos entre peticiones HTTP.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Raspa perfiles de legisladores de la Cámara de Diputados "
            "desde el SIL (sil.gobernacion.gob.mx)"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "--legislatura",
        required=True,
        metavar="NOMBRE",
        help=(
            "Legislatura(s) a raspar. Ejemplos:\n"
            "  --legislatura LXVI\n"
            "  --legislatura LXIV,LXV,LXVI\n"
            "  --legislatura all  (todas: LVII a LXVI)"
        ),
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=DELAY,
        metavar="SEGUNDOS",
        help=f"Pausa entre peticiones HTTP (default: {DELAY}s)",
    )
    return parser.parse_args()


def resolver_legislaturas(arg):
    """
    Convierte el argumento --legislatura en lista de tuplas (nombre, número).

    Acepta:
      - "all"                  → todas las legislaturas en orden cronológico
      - "LXVI"                 → una sola
      - "LXIV,LXV,LXVI"       → varias separadas por coma

    Lanza SystemExit con mensaje de error si algún nombre es inválido.
    """
    if arg.strip().lower() == "all":
        return list(LEGISLATURAS.items())

    nombres = [n.strip().upper() for n in arg.split(",")]
    invalidos = [n for n in nombres if n not in LEGISLATURAS]
    if invalidos:
        raise SystemExit(
            f"Legislatura(s) no reconocida(s): {', '.join(invalidos)}\n"
            f"Válidas: {', '.join(LEGISLATURAS)}"
        )
    return [(n, LEGISLATURAS[n]) for n in nombres]


def configurar_logging(run_dir):
    """
    Configura el sistema de logging para escribir simultáneamente a:
      - Consola (stdout): para seguimiento en tiempo real
      - Archivo scraper.log en run_dir: registro persistente

    Formato: timestamp | nivel | módulo | mensaje
    El directorio de salida se crea aquí si no existe.
    """
    os.makedirs(run_dir, exist_ok=True)
    ruta_log = os.path.join(run_dir, "scraper.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(ruta_log, encoding="utf-8"),
        ],
    )
    logger.info("Log iniciado: %s", os.path.abspath(ruta_log))


# Logger a nivel de módulo — se inicializa antes de configurar_logging()
# pero solo emite mensajes una vez que basicConfig() haya sido llamado en main().
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

def main():
    """
    Punto de entrada principal del script.

    1. Parsea argumentos de CLI
    2. Crea directorio de corrida con timestamp
    3. Configura logging
    4. Aplica el delay elegido
    5. Itera sobre las legislaturas objetivo y llama a run_legislature()
    """
    import datetime
    args = parse_args()

    run_ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir = os.path.join(_BASE_SCRAPER_DIR, run_ts)

    configurar_logging(run_dir)

    # Actualizar DELAY global con el valor del argumento --delay
    global DELAY
    DELAY = args.delay

    objetivos = resolver_legislaturas(args.legislatura)

    logger.info("=" * 60)
    logger.info("INICIANDO RASPADO")
    logger.info("Legislaturas objetivo: %s", [n for n, _ in objetivos])
    logger.info("Delay entre peticiones: %ss", DELAY)
    logger.info("Directorio de salida: %s", os.path.abspath(run_dir))
    logger.info("=" * 60)

    for leg_name, leg_num in objetivos:
        run_legislature(leg_name, leg_num, run_dir)

    logger.info("=" * 60)
    logger.info("✓ Proceso terminado.")
    logger.info("=" * 60)


def scrape_all() -> None:
    """Entry point para `uv run scrape-all` — raspa todas las legislaturas."""
    import sys
    sys.argv = ["scraper.py", "--legislatura", "all"]
    main()


if __name__ == "__main__":
    main()
