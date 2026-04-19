.PHONY: install scrape-all scrape etl etl-all clean help

PYTHON   := python3
VENV_PY  := .venv/bin/python
DATA_DIR := data

help:
	@echo "Uso:"
	@echo "  make install              Instalar dependencias en entorno virtual"
	@echo ""
	@echo "  make scrape LEG=LXVI     Raspar una legislatura"
	@echo "  make scrape-all          Raspar todas las legislaturas (LVII–LXVI)"
	@echo ""
	@echo "  make etl LEG=LXVI        Ejecutar el pipeline ETL en una legislatura"
	@echo "  make etl-all             Ejecutar el pipeline ETL en todas las legislaturas"
	@echo ""
	@echo "  make all LEG=LXVI        Raspar + ETL para una legislatura"
	@echo "  make all-full            Raspar + ETL para todas las legislaturas"
	@echo ""
	@echo "  make clean               Eliminar CSVs, logs y datos procesados de corridas anteriores"

install:
	$(PYTHON) -m venv .venv
	$(VENV_PY) -m pip install -r requirements.txt

# ── Raspador — produce data/scraper/<run_ts>/<LEGISLATURA>.csv ────────────────

scrape:
ifndef LEG
	$(error LEG requerido. Uso: make scrape LEG=LXVI)
endif
	$(VENV_PY) scraper.py --legislatura $(LEG)

scrape-all:
	$(VENV_PY) scraper.py --legislatura all

# ── Pipeline ETL — lee data/scraper/, escribe data/etl/<run_ts>/ ─────────────

etl:
ifndef LEG
	$(error LEG requerido. Uso: make etl LEG=LXVI)
endif
	$(VENV_PY) pipeline.py --legislatura $(LEG) $(if $(VERBOSE),--verbose,)

etl-all:
	$(VENV_PY) pipeline.py --legislatura all $(if $(VERBOSE),--verbose,)

# ── Combinados ────────────────────────────────────────────────────────────────

all: scrape etl

all-full: scrape-all etl-all

# ── Limpieza — elimina todos los directorios de corrida y logs sueltos ────────

clean:
	rm -rf $(DATA_DIR)/scraper/ $(DATA_DIR)/etl/
	rm -f scraper.log
