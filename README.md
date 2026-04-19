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

Cada ejecución de cualquiera de los dos scripts crea su propio subdirectorio con timestamp, de modo que las corridas no se sobreescriben y se puede rastrear el historial.

```
legisdatamxsil/
├── scraper.py          # Paso 1 — descarga perfiles del SIL
├── pipeline.py         # Paso 2 — orquesta load → clean → transform → save
├── etl/
│   ├── load.py         # Lee CSV crudos; auto-detecta la corrida más reciente del scraper
│   ├── clean.py        # Normaliza tipos, codifica categorías, crea flags y conteos de texto
│   ├── transform.py    # Desanida columnas JSON (comisiones, trayectorias) en conteos
│   └── save.py         # Escribe el CSV procesado con timestamp en data/etl/
├── data/
│   ├── scraper/
│   │   └── <YYYYMMDD_HHMMSS>/
│   │       ├── <LEGISLATURA>.csv   # CSV crudo: una fila por legislador, 36 columnas
│   │       └── scraper.log
│   └── etl/
│       └── <YYYYMMDD_HHMMSS>/
│           ├── <LEGISLATURA>_<ts>.csv  # CSV procesado: una fila por diputado_id
│           └── etl.log
├── requirements.txt
└── Makefile
```

---

## Inicio rápido

```bash
make install

# Raspar una legislatura y ejecutar el ETL
make all LEG=LXVI

# Todo (LVII–LXVI)
make all-full
```

---

## Paso 1 — Raspador (`scraper.py`)

Descarga todos los perfiles de legisladores del SIL para las legislaturas indicadas.
Salida: `data/scraper/<run_ts>/<LEGISLATURA>.csv` — una fila por diputado, 36 columnas crudas.

```bash
make scrape LEG=LXVI
make scrape-all

# o directamente:
python scraper.py --legislatura LXVI
python scraper.py --legislatura LXIV,LXV,LXVI
python scraper.py --legislatura all
```

**Cómo funciona internamente:**

| Función | Qué hace |
|---|---|
| `get_parties(leg_num)` | Obtiene la lista de partidos políticos de la legislatura desde la página de Numeralia del SIL |
| `get_legislator_refs(party_url)` | Extrae los IDs (Referencia) de todos los legisladores de un partido |
| `scrape_profile(referencia)` | Descarga y parsea el perfil completo de un legislador |
| `parse_tftable(soup)` | Extrae datos personales desde la tabla principal del perfil |
| `parse_tftable2(tabla)` | Extrae filas de tablas secundarias (comisiones, trayectorias) como listas de dicts |
| `generar_diputado_id(nombre, nacimiento)` | Crea un ID estable entre legislaturas usando SHA-256 sobre nombre normalizado + fecha de nacimiento |

**Notas:**
- Reanuda automáticamente si se interrumpe (omite referencias ya raspadas)
- Verificación SSL desactivada (el SIL usa una cadena GoDaddy incompleta)
- Pausa de 1.5 s entre peticiones para no saturar el servidor (`--delay` para cambiar)

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

# Usar una corrida específica del scraper en lugar de la más reciente:
python pipeline.py --legislatura all --input-dir data/scraper/20260418_140000/
```

### Los cuatro módulos del ETL y su conexión

```
load.py → clean.py → transform.py → save.py
```

#### `load.py` — Carga de datos crudos

Lee el CSV de la legislatura indicada desde el directorio de la corrida del scraper.
Si no se especifica `--input-dir`, detecta automáticamente la corrida más reciente
en `data/scraper/` (ordenando por nombre, que sigue el formato YYYYMMDD_HHMMSS).

Devuelve un `pd.DataFrame` con todas las columnas como `str` para no perder
información en la lectura (fechas, IDs con ceros, etc.). `clean.py` convierte los tipos.

#### `clean.py` — Limpieza y normalización

Recibe el DataFrame crudo y aplica en orden:

| Columna original | → Resultado |
|---|---|
| `nacimiento` (DD/MM/YYYY) | → `anio_nacimiento` (int) + `edad_al_tomar_cargo` (int) |
| `ultimo_grado_de_estudios` | → `grado_estudios_ord` (0–9 ordinal) |
| `principio_de_eleccion` | → `mayoria_relativa` (1=MR / 0=RP / -1=desconocido) |
| `partido` | → `partido` (abreviación en mayúsculas, normalizada) |
| `en_licencia` (bool texto) | → `en_licencia` (0/1 entero) |
| `correo_electronico`, `telefono`, `ubicacion` | → `tiene_correo`, `tiene_telefono`, `tiene_ubicacion` (flags 0/1) |
| `preparacion_academica`, `experiencia_legislativa` | → `n_palabras_preparacion`, `n_palabras_exp_legislativa` (conteo de palabras) |
| `entidad`, `ciudad` | → normalizadas, vacíos → "Desconocido" |
| `redes_sociales`, `error` (siempre nulas) | → eliminadas |

Al final establece `diputado_id` como índice del DataFrame.
Las columnas JSON (comisiones, trayectorias) se pasan intactas a `transform.py`.

#### `transform.py` — Extracción de características JSON

Recibe el DataFrame de `clean.py` con las columnas JSON todavía como texto serializado
y las convierte en conteos numéricos:

| Columna JSON | → Características |
|---|---|
| `comisiones` | → `n_comisiones`, `n_presidencias`, `n_secretarias`, `lider_comision` |
| `licencias_reincorporaciones` | → `n_licencias` |
| `trayectoria_administrativa` | → `n_trayectoria_admin` |
| `trayectoria_legislativa` | → `n_trayectoria_legislativa` |
| `trayectoria_politica` | → `n_trayectoria_politica` |
| `trayectoria_academica` | → `n_trayectoria_academica` |
| `trayectoria_empresarial` | → `n_trayectoria_empresarial` |
| `organos_de_gobierno` | → `n_organos_gobierno` |
| `otros_rubros` | → `n_otros_rubros` |
| `observaciones` | → `n_observaciones` |

#### `save.py` — Escritura al disco

Recibe el DataFrame completamente plano y lo guarda en:
`data/etl/<run_ts>/<LEGISLATURA>_<processed_ts>.csv`

- `run_ts` lo genera `pipeline.py` una vez al inicio: todas las legislaturas de una misma corrida comparten carpeta.
- `processed_ts` lo genera `save.py` al momento de guardar cada archivo: registra cuándo terminó de procesarse cada legislatura.

---

## Esquema de salida del ETL (34 columnas)

| Columna | Tipo | Descripción |
|---|---|---|
| `diputado_id` | str (índice) | ID estable entre legislaturas: SHA-256[:12] de nombre+nacimiento |
| `referencia` | int | ID del perfil en el SIL (único dentro de la legislatura) |
| `legislatura_nombre` | str | Nombre romano (LXVI, etc.) |
| `legislatura_num` | int | Número entero (57–66) |
| `partido_nombre` | str | Nombre completo del partido |
| `partido` | str | Abreviación del partido (PRI, MORENA, etc.) |
| `nombre` | str | Nombre completo del legislador |
| `entidad` | str | Estado de la República |
| `ciudad` | str | Ciudad |
| `region_de_eleccion` | str | Circunscripción o distrito electoral |
| `anio_nacimiento` | int | Año de nacimiento |
| `edad_al_tomar_cargo` | int | Edad al inicio de la legislatura |
| `grado_estudios_ord` | int | Escolaridad: 0 (desconocido) a 9 (doctorado) |
| `mayoria_relativa` | int | 1=mayoría relativa, 0=prop. proporcional, -1=desconocido |
| `en_licencia` | int | 1 si tomó licencia durante la legislatura |
| `tiene_suplente` | int | 1 si tiene suplente registrado |
| `suplente_referencia` | int | ID SIL del suplente (0 si no tiene) |
| `tiene_correo` | int | 1 si tiene correo electrónico registrado |
| `tiene_telefono` | int | 1 si tiene teléfono registrado |
| `tiene_ubicacion` | int | 1 si tiene ubicación de oficina registrada |
| `n_palabras_preparacion` | int | Palabras en el campo de preparación académica |
| `n_palabras_exp_legislativa` | int | Palabras en el campo de experiencia legislativa |
| `n_comisiones` | int | Total de membresías en comisiones |
| `n_presidencias` | int | Comisiones que presidió |
| `n_secretarias` | int | Comisiones como secretario |
| `lider_comision` | int | 1 si tuvo algún rol de liderazgo en comisiones |
| `n_licencias` | int | Número de licencias/reincorporaciones |
| `n_trayectoria_admin` | int | Entradas en trayectoria administrativa |
| `n_trayectoria_legislativa` | int | Entradas en trayectoria legislativa |
| `n_trayectoria_politica` | int | Entradas en trayectoria política |
| `n_trayectoria_academica` | int | Entradas en trayectoria académica |
| `n_trayectoria_empresarial` | int | Entradas en trayectoria empresarial |
| `n_otros_rubros` | int | Entradas en otros rubros |
| `n_organos_gobierno` | int | Participaciones en órganos de gobierno |
| `n_observaciones` | int | Notas del perfil |

---

## Requisitos

- Python 3.10+
- Conexión a internet (solo para el raspador)

```
requests>=2.33.1
beautifulsoup4>=4.14.3
urllib3>=2.6.3
pandas>=2.0.0
```
