#!/usr/bin/env bash
set -euo pipefail

# 1) Register any new raw files
PYTHONPATH=. conda run -n dsa-uk python pipeline/ingest/build_manifests.py

# 2) Parse to interim (Excel + PDFs)
PYTHONPATH=. conda run -n dsa-uk python pipeline/ingest/parse_excel.py
PYTHONPATH=. conda run -n dsa-uk python pipeline/ingest/parse_pdf_tables.py

# 3) Summarize catalog (for quick browsing / debugging)
PYTHONPATH=. conda run -n dsa-uk python pipeline/ingest/summarize_catalog.py

# 4) Build core OBR dataset
PYTHONPATH=. conda run -n dsa-uk python pipeline/ingest/obr_extract_core.py
PYTHONPATH=. conda run -n dsa-uk python pipeline/ingest/post_derive_core.py

# 5) Make level series robust (permanent fix)
PYTHONPATH=. conda run -n dsa-uk python pipeline/ingest/fix_level_duplicates.py

echo "✅ Ingest complete."
