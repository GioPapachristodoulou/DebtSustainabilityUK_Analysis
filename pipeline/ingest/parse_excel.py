import pandas as pd, json
from pathlib import Path
from pipeline.utils.hashing import file_sha256

RAW = Path("data/raw"); OUT = Path("data/interim"); MAN = Path("manifests/tables_manifest.csv")
OUT.mkdir(parents=True, exist_ok=True); MAN.parent.mkdir(parents=True, exist_ok=True)

def header_row(df):
    for i in range(min(10, len(df))):
        row = df.iloc[i]
        nonnull = row.notna().mean()
        nonnum = sum(not pd.api.types.is_number(x) for x in row)
        if nonnull>=0.6 and nonnum>=2: return i
    return 0

def append_manifest(d):
    import csv
    write_header = not MAN.exists()
    with MAN.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(d.keys()))
        if write_header: w.writeheader()
        w.writerow(d)

def process_file(path: Path):
    xls = pd.ExcelFile(path, engine="openpyxl" if path.suffix.lower()==".xlsx" else None)
    for sheet in xls.sheet_names:
        df0 = xls.parse(sheet_name=sheet, header=None, dtype=str)
        h = header_row(df0)
        df = xls.parse(sheet_name=sheet, header=h)
        df = df.dropna(how="all").dropna(axis=1, how="all")
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["__".join([str(x) for x in tup if str(x)!="nan"]) for tup in df.columns]
        logical_id = path.stem
        safe_sheet = sheet.replace("/", "_").replace(" ", "_")
        outdir = OUT / path.parent.name
        outdir.mkdir(parents=True, exist_ok=True)
        outcsv = outdir / f"{logical_id}__{safe_sheet}.csv"
        df.to_csv(outcsv, index=False)
        meta = {
          "logical_id": logical_id,
          "table_id": f"sheet:{sheet}",
          "output_path": str(outcsv),
          "parser": "excel",
          "header_row_index": h,
          "n_rows": int(df.shape[0]),
          "n_cols": int(df.shape[1]),
          "sha256_of_output": file_sha256(outcsv),
          "columns_json": json.dumps(list(map(str, df.columns))),
          "notes":""
        }
        append_manifest(meta)

if __name__=="__main__":
    for p in RAW.rglob("*"):
        if p.suffix.lower() in [".xlsx",".xls",".xlsb"] and p.is_file():
            try:
                process_file(p)
            except Exception as e:
                q = (OUT / "_quarantine"); q.mkdir(exist_ok=True, parents=True)
                (q / f"ERROR_{p.name}.txt").write_text(str(e))
