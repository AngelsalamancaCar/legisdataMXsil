.PHONY: install scrape-all scrape clean help

PYTHON := python3
DATA_DIR := data

help:
	@echo "Usage:"
	@echo "  make install              Install dependencies"
	@echo "  make scrape LEG=LXVI     Scrape one legislature (e.g. LXVI)"
	@echo "  make scrape-all          Scrape all legislatures (LVII–LXVI)"
	@echo "  make clean               Remove output CSV and logs"

install:
	$(PYTHON) -m pip install -r requirements.txt

scrape:
ifndef LEG
	$(error LEG is required. Usage: make scrape LEG=LXVI)
endif
	$(PYTHON) scraper.py --legislatura $(LEG)

scrape-all:
	$(PYTHON) scraper.py --legislatura all

clean:
	rm -f $(DATA_DIR)/*.csv $(DATA_DIR)/*.log scraper.log
