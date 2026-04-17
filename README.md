# legisdatamxsil

Scraper + ETL pipeline for Mexican Chamber of Deputies legislator profiles.  
Source: [sil.gobernacion.gob.mx](https://sil.gobernacion.gob.mx)  
Coverage: Legislatures LVII–LXVI (1997–present), ~500 diputados each.

---

## Cave structure

```
legisdatamxsil/
├── scraper.py          # Hunting cave — fetches raw profiles from SIL
├── pipeline.py         # ETL orchestrator — load → clean → transform → save
├── etl/
│   ├── load.py         # Load raw CSV(s) from data/
│   ├── clean.py        # Normalize types, encode categories, fill nulls
│   ├── transform.py    # Extract features from nested JSON columns
│   └── save.py         # Write processed CSV to data/processed/
├── data/
│   ├── <LEG>.csv       # Raw scraper output (one file per legislature)
│   └── processed/
│       └── <LEG>.csv   # Flat ML-ready table (one file per legislature)
├── requirements.txt
└── Makefile
```

Each cave does **one job**. Other cavemen can swap any cave independently.

---

## Quick start

```bash
make install

# Scrape one legislature + run ETL
make all LEG=LXVI

# Everything (LVII–LXVI)
make all-full
```

---

## Scraper (hunting cave)

Fetches all legislator profiles from the SIL for the given legislature(s).  
Output: `data/<LEG>.csv` — one row per diputado, 36 raw columns.

```bash
make scrape LEG=LXVI
make scrape-all

# or directly:
python scraper.py --legislatura LXVI
python scraper.py --legislatura LXIV,LXV,LXVI
python scraper.py --legislatura all
```

**Notes:**
- Auto-resumes if interrupted (skips already-scraped references)
- SSL verification disabled (SIL uses broken GoDaddy chain)
- 1.5s delay between requests by default (`--delay` to override)

---

## ETL pipeline (cooking + storage caves)

Transforms raw CSVs into flat ML-ready tables.  
Output: `data/processed/<LEG>.csv` — one row per `diputado_id`, 34 columns.

```bash
make etl LEG=LXVI
make etl-all

# or directly:
python pipeline.py --legislatura LXVI
python pipeline.py --legislatura all
```

### What the ETL does

**clean.py:**
| Raw column | → Processed |
|---|---|
| `nacimiento` (DD/MM/YYYY) | → `anio_nacimiento` (int) + `edad_al_tomar_cargo` (int) |
| `ultimo_grado_de_estudios` | → `grado_estudios_ord` (0–9 ordinal) |
| `principio_de_eleccion` | → `mayoria_relativa` (1/0/−1) |
| `partido` | → `partido` (uppercased, normalized) |
| `en_licencia` (bool) | → `en_licencia` (0/1) |
| `correo_electronico`, `telefono`, `ubicacion` | → `tiene_correo`, `tiene_telefono`, `tiene_ubicacion` (0/1 flags) |
| `preparacion_academica`, `experiencia_legislativa` | → `n_palabras_preparacion`, `n_palabras_exp_legislativa` (word count) |
| `entidad`, `ciudad` | → normalized, nulls → "Desconocido" |
| `redes_sociales`, `error` (always null) | → dropped |

**transform.py:**
| JSON column | → Features |
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

### Output schema (34 columns)

| Column | Type | Description |
|---|---|---|
| `diputado_id` | str (index) | SHA-256[:12] stable cross-legislature ID |
| `referencia` | int | SIL profile ID (unique within legislature) |
| `legislatura_nombre` | str | Roman numeral name (LXVI, etc.) |
| `legislatura_num` | int | Integer (57–66) |
| `partido_nombre` | str | Full party name |
| `partido` | str | Party abbreviation (PRI, MORENA, etc.) |
| `nombre` | str | Full name |
| `entidad` | str | State |
| `ciudad` | str | City |
| `region_de_eleccion` | str | Electoral region |
| `anio_nacimiento` | int | Birth year |
| `edad_al_tomar_cargo` | int | Age at legislature start |
| `grado_estudios_ord` | int | Education level 0–9 ordinal |
| `mayoria_relativa` | int | 1=majority, 0=proportional, −1=unknown |
| `en_licencia` | int | 1 if took leave |
| `tiene_suplente` | int | 1 if has substitute |
| `suplente_referencia` | int | Substitute's SIL ID (0 if none) |
| `tiene_correo` | int | 1 if email present |
| `tiene_telefono` | int | 1 if phone present |
| `tiene_ubicacion` | int | 1 if office location present |
| `n_palabras_preparacion` | int | Word count of academic background text |
| `n_palabras_exp_legislativa` | int | Word count of legislative experience text |
| `n_comisiones` | int | Number of committee memberships |
| `n_presidencias` | int | Committees chaired |
| `n_secretarias` | int | Committees as secretary |
| `lider_comision` | int | 1 if any leadership role in committees |
| `n_licencias` | int | Leave/reinstatement events |
| `n_trayectoria_admin` | int | Administrative career entries |
| `n_trayectoria_legislativa` | int | Legislative career entries |
| `n_trayectoria_politica` | int | Political career entries |
| `n_trayectoria_academica` | int | Academic career entries |
| `n_trayectoria_empresarial` | int | Business career entries |
| `n_otros_rubros` | int | Other career entries |
| `n_organos_gobierno` | int | Governance body entries |
| `n_observaciones` | int | Profile notes count |

---

## Requirements

- Python 3.9+
- Internet connection (scraper only)

```
requests>=2.33.1
beautifulsoup4>=4.14.3
urllib3>=2.6.3
pandas>=2.0.0
```
