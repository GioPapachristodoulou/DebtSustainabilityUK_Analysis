import csv, os, time
from pathlib import Path
from pipeline.utils.hashing import file_sha256

RAW = Path("data/raw")
MAN = Path("manifests/files_manifest.csv")
MAN.parent.mkdir(parents=True, exist_ok=True)

FIELDS = ["logical_id","publisher","title","filename","publication_date","acquired_date",
          "version_tag","sha256","filesize_bytes","filetype","notes"]

def iter_files():
    for p in RAW.rglob("*"):
        if p.is_file():
            try:
                publisher = p.parts[p.parts.index("raw")+1]
            except Exception:
                publisher = "Unknown"
            yield {
                "logical_id": p.stem,
                "publisher": publisher,
                "title": "",
                "filename": str(p),
                "publication_date": "",
                "acquired_date": time.strftime("%Y-%m-%d"),
                "version_tag": "",
                "sha256": file_sha256(p),
                "filesize_bytes": p.stat().st_size,
                "filetype": p.suffix.lower().lstrip("."),
                "notes":""
            }

def main():
    write_header = not MAN.exists()
    with MAN.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        if write_header: w.writeheader()
        for r in iter_files(): w.writerow(r)

if __name__=="__main__":
    main()
