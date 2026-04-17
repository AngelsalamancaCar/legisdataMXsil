.PHONY: install scrape-all scrape etl etl-all clean help

PYTHON   := python3
VENV_PY  := .venv/bin/python
DATA_DIR := data

help:
	@echo "Usage:"
	@echo "  make install              Install dependencies"
	@echo ""
	@echo "  make scrape LEG=LXVI     Scrape one legislature"
	@echo "  make scrape-all          Scrape all legislatures (LVII–LXVI)"
	@echo ""
	@echo "  make etl LEG=LXVI        Run ETL pipeline on one legislature"
	@echo "  make etl-all             Run ETL pipeline on all legislatures"
	@echo ""
	@echo "  make all LEG=LXVI        Scrape + ETL for one legislature"
	@echo "  make all-full            Scrape + ETL for all legislatures"
	@echo ""
	@echo "  make clean               Remove output CSV, logs, processed data"

install:
	$(PYTHON) -m venv .venv
	$(VENV_PY) -m pip install -r requirements.txt

# ── Scraper (hunting cave) ────────────────────────────────────────────────────

scrape:
ifndef LEG
	$(error LEG required. Usage: make scrape LEG=LXVI)
endif
	$(VENV_PY) scraper.py --legislatura $(LEG)

scrape-all:
	$(VENV_PY) scraper.py --legislatura all

# ── ETL pipeline (cooking + storage caves) ───────────────────────────────────

etl:
ifndef LEG
	$(error LEG required. Usage: make etl LEG=LXVI)
endif
	$(VENV_PY) pipeline.py --legislatura $(LEG) $(if $(VERBOSE),--verbose,)

etl-all:
	$(VENV_PY) pipeline.py --legislatura all $(if $(VERBOSE),--verbose,)

# ── Combined ─────────────────────────────────────────────────────────────────

all: scrape etl

all-full: scrape-all etl-all

# ── Cleanup ──────────────────────────────────────────────────────────────────

clean:
	rm -f $(DATA_DIR)/*.csv $(DATA_DIR)/*.log scraper.log
	rm -f $(DATA_DIR)/processed/*.csv
