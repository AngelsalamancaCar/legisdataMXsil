# legisdatamxsil

Scraper de perfiles legislativos para la Cámara de Diputados de México.
Fuente: [sil.gobernacion.gob.mx](https://sil.gobernacion.gob.mx)

Cobertura: Legislaturas LVII–LXVI (1997–presente).

## Output

CSV por legislatura en `data/`. 36 columnas por perfil:

- Identificadores: `diputado_id` (hash SHA-256 estable cross-legislatura), `referencia`, `legislatura_nombre`, `partido_nombre`
- Datos personales: `nombre`, `nacimiento`, `entidad`, `ciudad`, `correo_electronico`, `telefono`, `suplente`
- Trayectorias (JSON serializado): `comisiones`, `trayectoria_administrativa`, `trayectoria_legislativa`, `trayectoria_politica`, `trayectoria_academica`, `trayectoria_empresarial`, `organos_de_gobierno`
- Estado: `en_licencia`, `error`

## Requisitos

- Python 3.9+
- Conexión a internet

## Instalación

```bash
pip install -r requirements.txt
# o
make install
```

## Uso

```bash
# Una legislatura
python scraper.py --legislatura LXVI

# Varias
python scraper.py --legislatura LXIV,LXV,LXVI

# Todas (LVII–LXVI)
python scraper.py --legislatura all

# Con delay personalizado entre requests (default: 1.5s)
python scraper.py --legislatura LXVI --delay 2

# Con make
make scrape LEG=LXVI
make scrape-all
```

## Notas

- **Reanudación automática**: si el CSV ya existe, omite referencias ya scrapeadas. Seguro interrumpir y reanudar.
- **SSL**: el sitio usa cadena de certificados incompleta (GoDaddy); verificación SSL deshabilitada.
- **Encoding**: HTML del sitio en windows-1252; el scraper decodifica explícitamente.
- **Throttling**: 1.5s entre requests para no sobrecargar el servidor.
- **Deduplicación**: un diputado en múltiples partidos se registra una sola vez por legislatura.
