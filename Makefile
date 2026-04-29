.PHONY: install scrape scrape-all etl etl-all all all-full clean help

UV       := uv
DATA_DIR := data

help:
	@echo "Uso:"
	@echo "  make install              Sincronizar dependencias con uv"
	@echo ""
	@echo "  make scrape LEG=LXVI     Raspar una legislatura"
	@echo "  make scrape-all          Raspar todas las legislaturas (LVII–LXVI)"
	@echo ""
	@echo "  make etl LEG=LXVI        Ejecutar el pipeline ETL en una legislatura"
	@echo "  make etl-all             Ejecutar el pipeline ETL en todas las legislaturas"
	@echo "  make etl LEG=LXVI VERBOSE=1  ETL con output de debug"
	@echo ""
	@echo "  make all LEG=LXVI        Raspar + ETL para una legislatura"
	@echo "  make all-full            Raspar + ETL para todas las legislaturas"
	@echo ""
	@echo "  make clean               Eliminar CSVs, logs y datos de corridas anteriores"

install:
	$(UV) sync

# ── Raspador — produce data/scraper/<run_ts>/<LEGISLATURA>.csv ────────────────

scrape:
ifndef LEG
	$(error LEG requerido. Uso: make scrape LEG=LXVI)
endif
	$(UV) run scraper.py --legislatura $(LEG)

scrape-all:
	$(UV) run scrape-all

# ── Pipeline ETL — lee data/scraper/, escribe data/etl/<run_ts>/ ─────────────

etl:
ifndef LEG
	$(error LEG requerido. Uso: make etl LEG=LXVI)
endif
	$(UV) run pipeline.py --legislatura $(LEG) $(if $(VERBOSE),--verbose,)

etl-all:
	$(UV) run etl-all $(if $(VERBOSE),--verbose,)

# ── Combinados ────────────────────────────────────────────────────────────────

all: scrape etl

all-full: scrape-all etl-all

# ── Limpieza — elimina todos los directorios de corrida y logs sueltos ────────

clean:
	rm -rf $(DATA_DIR)/scraper/ $(DATA_DIR)/etl/
	rm -f scraper.log
