SHELL := /bin/bash
export PYTHONPATH := $(PWD)

.PHONY: setup ingest validate clean

setup:
	conda env create -f environment.yml || true
	conda run -n dsa-uk pre-commit install

ingest:
	conda run -n dsa-uk python pipeline/ingest/build_manifests.py
	conda run -n dsa-uk python pipeline/ingest/parse_excel.py
	conda run -n dsa-uk python pipeline/ingest/parse_pdf_tables.py

validate:
	conda run -n dsa-uk python pipeline/validate/validate.py

clean:
	rm -rf data/interim/* docs/provenance/*.log
