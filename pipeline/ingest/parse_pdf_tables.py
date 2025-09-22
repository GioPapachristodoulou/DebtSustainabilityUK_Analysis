import pdfplumber, pandas as pd, json, shutil
from pathlib import Path
from pipeline.utils.hashing import file_sha256

RAW = Path("data/raw"); OUT = Path("data/interim"); MAN = Path("manifests/tables_manifest.csv")
OUT.mkdir(parents=True, exist_ok=True); MAN.parent.mkdir(parents=True, exist_ok=True)

def append_manifest(d):
    import csv
    write_header = not MAN.exists()
    with MAN.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(d.keys()))
        if write_header: w.writeheader()
        w.writerow(d)

def save_table(df, path, logical_id, tid, parser):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    meta = {
      "logical_id": logical_id, "table_id": tid, "output_path": str(path),
      "parser": parser, "header_row_index": 0, "n_rows": int(df.shape[0]),
      "n_cols": int(df.shape[1]), "sha256_of_output": file_sha256(path),
      "columns_json": json.dumps(list(map(str, df.columns))), "notes":""
    }
    append_manifest(meta)

def process_pdf(p: Path):
    logical_id = p.stem
    outdir = OUT / p.parent.name
    with pdfplumber.open(p) as pdf:
        for i, page in enumerate(pdf.pages, start=1):
            try:
                tbls = page.extract_tables()
                for j, t in enumerate(tbls, start=1):
                    if not t or len(t)<2: continue
                    df = pd.DataFrame(t[1:], columns=t[0])
                    save_table(df, outdir / f"{logical_id}__p{i}_t{j}.csv",
                               logical_id, f"pdfplumber:p{i}_t{j}", "pdfplumber")
            except Exception:
                pass
    # Optionally: add camelot/tabula later if needed (depends on environment)

if __name__=="__main__":
    for p in RAW.rglob("*.pdf"):
        process_pdf(p)
