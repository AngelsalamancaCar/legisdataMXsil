# legisdatamxsil

Raspador web y pipeline ETL para perfiles de legisladores de la Cámara de Diputados de México.
Fuente: [sil.gobernacion.gob.mx](https://sil.gobernacion.gob.mx)
Cobertura: Legislaturas LVII–LXVI (1997–presente), ~500 diputados por legislatura.

---

## Cómo se conectan los módulos

El proyecto tiene dos etapas independientes que se ejecutan en secuencia:

```
scraper.py  →  data/scraper/<run_ts>/  →  pipeline.py  →  data/etl/<run_ts>/
```

1. **scraper.py** descarga los perfiles del SIL y los guarda como CSV crudos.
2. **pipeline.py** lee esos CSV, los procesa con el paquete `etl/` y guarda los resultados listos para análisis.

Cada ejecución crea su propio subdirectorio con timestamp, de modo que las corridas no se sobreescriben y se puede rastrear el historial.

```
legisdatamxsil/
├── scraper.py          # Paso 1 — descarga perfiles del SIL
├── pipeline.py         # Paso 2 — orquesta load → normalize → clean → transform → save
├── etl/
│   ├── load.py         # Lee CSV crudos; auto-detecta la corrida más reciente del scraper
│   ├── normalize.py    # Elimina desbordamiento de secciones en columnas de trayectoria
│   ├── clean.py        # Normaliza tipos, codifica categorías, extrae flags de experiencia legislativa
│   ├── transform.py    # Desanida columnas JSON (comisiones, trayectorias) en conteos y flags
│   └── save.py         # Escribe el CSV procesado con timestamp en data/etl/
├── data/
│   ├── scraper/
│   │   └── <YYYYMMDD_HHMMSS>/
│   │       ├── <LEGISLATURA>.csv   # CSV crudo: una fila por legislador, 32 columnas
│   │       └── scraper.log
│   └── etl/
│       └── <YYYYMMDD_HHMMSS>/
│           ├── <LEGISLATURA>_<ts>.csv  # CSV procesado: una fila por diputado_id
│           └── etl.log
├── pyproject.toml
├── requirements.txt
└── Makefile
```

---

## Inicio rápido

### Con uv (recomendado)

```bash
uv sync

# Raspar una legislatura y ejecutar el ETL
uv run scrape-all          # todas las legislaturas
uv run etl-all             # ETL en todas
uv run etl-one --legislatura LXVI  # ETL en una
```

### Con pip / Make

```bash
make install

# Raspar una legislatura y ejecutar el ETL
make all LEG=LXVI

# Todo (LVII–LXVI)
make all-full

# Limpiar todas las corridas anteriores
make clean
```

---

## Paso 1 — Raspador (`scraper.py`)

Descarga todos los perfiles de legisladores del SIL para las legislaturas indicadas.
Salida: `data/scraper/<run_ts>/<LEGISLATURA>.csv` — una fila por diputado, 32 columnas crudas.

```bash
make scrape LEG=LXVI
make scrape-all

# o directamente:
python scraper.py --legislatura LXVI
python scraper.py --legislatura LXIV,LXV,LXVI
python scraper.py --legislatura all

# ajustar pausa entre peticiones (default: 1.5s):
python scraper.py --legislatura LXVI --delay 2.0
```

**Cómo funciona internamente:**

| Función | Qué hace |
|---|---|
| `get_parties(leg_num)` | Obtiene la lista de partidos políticos (activos y en licencia) de la legislatura desde la página de Numeralia del SIL |
| `get_legislator_refs(party_url)` | Extrae los IDs (Referencia) de todos los legisladores de un partido |
| `scrape_profile(referencia)` | Descarga y parsea el perfil completo de un legislador |
| `parse_tftable(soup)` | Extrae datos personales desde la tabla principal del perfil |
| `parse_tftable2(tabla)` | Extrae filas de tablas secundarias (comisiones, trayectorias) como listas de dicts |
| `generar_diputado_id(nombre, nacimiento)` | Crea un ID estable entre legislaturas usando SHA-256 sobre nombre normalizado + fecha de nacimiento |

**Notas:**
- Reanuda automáticamente si se interrumpe (omite referencias ya raspadas)
- Captura tanto legisladores activos como los que tomaron licencia (`en_licencia`)
- Verificación SSL desactivada (el SIL usa una cadena GoDaddy incompleta)
- Pausa de 1.5 s entre peticiones para no saturar el servidor (`--delay` para cambiar)
- 3 reintentos automáticos con pausa de 3 s ante fallos de red

---

## Paso 2 — Pipeline ETL (`pipeline.py` + paquete `etl/`)

Transforma los CSV crudos del raspador en tablas planas listas para análisis o modelos ML.
Salida: `data/etl/<run_ts>/<LEGISLATURA>_<ts>.csv` — una fila por `diputado_id`.

```bash
make etl LEG=LXVI
make etl-all

# o directamente:
python pipeline.py --legislatura LXVI
python pipeline.py --legislatura all

# con output verbose en consola:
python pipeline.py --legislatura LXVI --verbose

# usar una corrida específica del scraper en lugar de la más reciente:
python pipeline.py --legislatura all --input-dir data/scraper/20260418_140000/
```

### Los cinco módulos del ETL y su conexión

```
load.py → normalize.py → clean.py → transform.py → save.py
```

#### `load.py` — Carga de datos crudos

Lee el CSV de la legislatura indicada desde el directorio de la corrida del scraper.
Si no se especifica `--input-dir`, detecta automáticamente la corrida más reciente
en `data/scraper/` (ordenando por nombre, que sigue el formato YYYYMMDD_HHMMSS).

Devuelve un `pd.DataFrame` con todas las columnas como `str` para no perder
información en la lectura (fechas, IDs con ceros, etc.). Las etapas siguientes
convierten los tipos.

#### `normalize.py` — Eliminación de desbordamiento de secciones

El scraper serializa las secciones de trayectoria de arriba hacia abajo; las columnas
de trayectoria más tempranas acumulan las secciones subsecuentes como registros extra
(p. ej. `trayectoria_administrativa` contiene también los registros de
`trayectoria_legislativa`, `trayectoria_politica`, etc.).

`normalize.py` detecta los encabezados centinela (`{"Del año": "TRAYECTORIA POLÍTICA", ...}`)
y trunca cada columna a sus entradas propias. Además extrae la sección
`INVESTIGACIÓN Y DOCENCIA` (presente en LXVI) que aparece al final de `otros_rubros`
y la coloca en la columna nueva `investigacion_docencia`.

#### `clean.py` — Limpieza y normalización

Recibe el DataFrame normalizado y aplica en orden:

| Columna original | → Resultado |
|---|---|
| `nacimiento` (DD/MM/YYYY) | → `y_nacimiento` (int) + `edad_al_tomar_cargo` (int) |
| `ultimo_grado_de_estudios` | → `grado_estudios_ord` (0–9 ordinal) |
| `principio_de_eleccion` | → `mayoria_relativa` (1=MR / 0=RP / -1=desconocido) |
| `partido` | → `partido` (abreviación en mayúsculas, normalizada con `PARTIDO_ALIAS`) |
| `suplente_referencia` | → entero (0 si no tiene suplente) + `tiene_suplente` (0/1) |
| `preparacion_academica` | → `area_formacion` (categoría canónica de 14 áreas de estudio) |
| `experiencia_legislativa` | → `fue_diputado_local`, `fue_diputado_federal`, `fue_senador`, `n_cargos_legislativos_prev` |
| columnas de identificación redundantes, URLs, campos de contacto | → eliminadas |

Al final establece `diputado_id` como índice del DataFrame.
Las columnas JSON (comisiones, trayectorias) se pasan intactas a `transform.py`.

#### `transform.py` — Extracción de características JSON

Recibe el DataFrame de `clean.py` con las columnas JSON todavía como texto serializado
y las convierte en características numéricas. Opera en cuatro sub-pasos:

**Comisiones** (`_extract_comisiones`):

| Columna JSON | → Características |
|---|---|
| `comisiones` | → `n_comisiones`, `n_comisiones_especiales`, `n_presidencias`, `n_secretarias`, `presidente_comision`, `lider_comision` + flag binario `comision_<slug>` por cada comisión en `MAJOR_COMISIONES` (~63 flags) |

**Trayectoria administrativa** (`_extract_trayectoria_admin`):

| Columna JSON | → Características |
|---|---|
| `trayectoria_administrativa` | → `n_trayectoria_admin` + 11 flags de rol (fue_presidente_mun, fue_director_general, etc.) + 6 flags de institución (admin_en_partido, admin_en_gobierno_fed, etc.) + `nivel_cargo_max` (0–5) + 5 variables de liderazgo juvenil |

**Trayectoria académica** (`_extract_trayectoria_academica`):

| Columna JSON | → Características |
|---|---|
| `trayectoria_academica` | → `tiene_posgrado`, `tiene_doctorado`, `estudios_en_extranjero`, `univ_publica`, `univ_privada`, `univ_extranjera` + 10 flags por institución (acad_unam, acad_itesm, acad_itam, …) |

**Demás trayectorias** (`_extract_trayectorias`):

| Columna JSON | → Característica |
|---|---|
| `trayectoria_legislativa` | → `n_trayectoria_legislativa` |
| `trayectoria_politica` | → `n_trayectoria_politica` |
| `trayectoria_empresarial` | → `n_trayectoria_empresarial` |
| `investigacion_docencia` | → `n_investigacion_docencia` (LXVI+; 0 en legislaturas anteriores) |
| `organos_de_gobierno` | → `n_organos_gobierno` |

Las columnas `otros_rubros` y `observaciones` se conservan como texto JSON en el CSV de salida (no se desanidan).

#### `save.py` — Escritura al disco

Recibe el DataFrame completamente plano y lo guarda en:
`data/etl/<run_ts>/<LEGISLATURA>_<processed_ts>.csv`

- `run_ts` lo genera `pipeline.py` una vez al inicio: todas las legislaturas de una misma corrida comparten carpeta.
- `processed_ts` lo genera `save.py` al momento de guardar cada archivo: registra cuándo terminó de procesarse cada legislatura.

---

## Esquema de salida del ETL

El DataFrame final tiene una fila por `diputado_id` y aproximadamente 130 columnas agrupadas así:

### Identificadores y metadatos

| Columna | Tipo | Descripción |
|---|---|---|
| `diputado_id` | str (índice) | ID estable entre legislaturas: SHA-256[:12] de nombre+nacimiento |
| `referencia` | int | ID del perfil en el SIL (único dentro de la legislatura) |
| `legislatura_num` | int | Número entero de la legislatura (57–66) |
| `nombre` | str | Nombre completo del legislador |

### Variables demográficas y de trayectoria escolar

| Columna | Tipo | Descripción |
|---|---|---|
| `partido` | str | Abreviación del partido (PRI, MORENA, etc.), normalizada |
| `y_nacimiento` | int | Año de nacimiento |
| `edad_al_tomar_cargo` | int | Edad al inicio de la legislatura |
| `grado_estudios_ord` | int | Escolaridad: 0 (desconocido) a 9 (doctorado) |
| `area_formacion` | str | Área canónica de formación académica (14 categorías: Derecho, Ingeniería, etc.) |
| `mayoria_relativa` | int | 1=mayoría relativa, 0=prop. proporcional, -1=desconocido |
| `tiene_suplente` | int | 1 si tiene suplente registrado |
| `suplente_referencia` | int | ID SIL del suplente (0 si no tiene) |

### Experiencia legislativa previa

| Columna | Tipo | Descripción |
|---|---|---|
| `fue_diputado_local` | int | 1 si tuvo cargo de diputado/a local antes de esta legislatura |
| `fue_diputado_federal` | int | 1 si fue diputado/a federal en una legislatura anterior |
| `fue_senador` | int | 1 si fue senador/a antes de esta legislatura |
| `n_cargos_legislativos_prev` | int | Suma de los tres flags anteriores (0–3) |

### Comisiones

| Columna | Tipo | Descripción |
|---|---|---|
| `n_comisiones` | int | Total de membresías en comisiones (regulares + especiales) |
| `n_comisiones_especiales` | int | Membresías en comisiones especiales |
| `n_presidencias` | int | Roles de Presidente, Vicepresidente o Copresidente |
| `n_secretarias` | int | Roles de Secretario en comisiones |
| `presidente_comision` | int | 1 si tuvo al menos un rol presidencial |
| `lider_comision` | int | 1 si tuvo cualquier rol de liderazgo (presidente o secretario) |
| `comision_<slug>` × ~63 | int | 1 si perteneció a esa comisión (una columna por cada comisión en `MAJOR_COMISIONES`) |

### Trayectoria administrativa

| Columna | Tipo | Descripción |
|---|---|---|
| `n_trayectoria_admin` | int | Número de entradas válidas en trayectoria administrativa |
| `nivel_cargo_max` | int | Nivel jerárquico máximo alcanzado (0=sin clasificar … 5=presidente/gobernador) |
| `fue_presidente_mun` | int | 1 si fue presidente/a municipal |
| `fue_presidente_org` | int | 1 si fue presidente/a de una organización (no municipal) |
| `fue_director_general` | int | 1 si fue director/a general |
| `fue_secretario_cargo` | int | 1 si fue secretario/a en algún cargo |
| `fue_subsecretario` | int | 1 si fue subsecretario/a |
| `fue_director` | int | 1 si fue director/a |
| `fue_coordinador` | int | 1 si fue coordinador/a |
| `fue_delegado` | int | 1 si fue delegado/a |
| `fue_asesor` | int | 1 si fue asesor/a |
| `fue_regidor` | int | 1 si fue regidor/a |
| `fue_sindico` | int | 1 si fue síndico/a |
| `admin_en_partido` | int | 1 si tuvo cargo en un partido político |
| `admin_en_sindicato` | int | 1 si tuvo cargo en un sindicato |
| `admin_en_universidad` | int | 1 si tuvo cargo en una universidad |
| `admin_en_gobierno_fed` | int | 1 si tuvo cargo en el gobierno federal o dependencias federales |
| `admin_en_gobierno_est` | int | 1 si tuvo cargo en el gobierno estatal |
| `admin_en_gobierno_mun` | int | 1 si tuvo cargo en el gobierno municipal |
| `tiene_exp_juvenil` | int | 1 si alguna entrada menciona keywords de juventud |
| `lider_juvenil_partido` | int | 1 si tuvo liderazgo en ala juvenil de un partido |
| `lider_juvenil_gobierno` | int | 1 si tuvo cargo en instituto gubernamental de juventud |
| `miembro_org_juvenil` | int | 1 si participó en org juvenil sin rol de liderazgo |
| `nivel_liderazgo_juvenil` | int | Ordinal 0–3 (0=ninguno, 1=participación, 2=cargo, 3=liderazgo) |

### Trayectoria académica

| Columna | Tipo | Descripción |
|---|---|---|
| `tiene_posgrado` | int | 1 si tiene maestría, doctorado o especialidad |
| `tiene_doctorado` | int | 1 si tiene doctorado o PhD |
| `estudios_en_extranjero` | int | 1 si estudió en institución extranjera |
| `univ_publica` | int | 1 si asistió a universidad pública (UNAM, IPN, UAM, autónomas) |
| `univ_privada` | int | 1 si asistió a universidad privada (ITESM, ITAM, Ibero, etc.) |
| `univ_extranjera` | int | 1 si asistió a institución en el extranjero |
| `acad_unam` … `acad_uv` × 10 | int | Flag por institución del top-10 de frecuencia |

### Demás trayectorias (conteos)

| Columna | Tipo | Descripción |
|---|---|---|
| `n_trayectoria_legislativa` | int | Entradas en trayectoria legislativa |
| `n_trayectoria_politica` | int | Entradas en trayectoria política |
| `n_trayectoria_empresarial` | int | Entradas en trayectoria empresarial/iniciativa privada |
| `n_investigacion_docencia` | int | Entradas en investigación y docencia (LXVI+; 0 en anteriores) |
| `n_organos_gobierno` | int | Participaciones en órganos de gobierno |

### Columnas JSON conservadas

| Columna | Tipo | Descripción |
|---|---|---|
| `otros_rubros` | str (JSON) | Entradas de la sección "Otros rubros" que no son investigación/docencia |
| `observaciones` | str (JSON) | Notas del perfil (sin desanidar) |

---

## Requisitos

- Python 3.10+
- Conexión a internet (solo para el raspador)

```
requests>=2.33.1
beautifulsoup4>=4.14.3
urllib3>=2.6.3
pandas>=2.0.0
numpy>=1.24.0
```
