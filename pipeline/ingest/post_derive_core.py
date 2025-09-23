from pathlib import Path
import pandas as pd

OUTCSV = Path("data/processed/obr_core.csv")

if not OUTCSV.exists():
    raise SystemExit("Missing data/processed/obr_core.csv — run obr_extract_core.py first.")

df = pd.read_csv(OUTCSV)

# Work in wide for simple algebra
w = df.pivot_table(index="fy", columns="series_id", values="value", aggfunc="first")

# Coerce numeric for anything we combine/derive
for c in ["receipts_gdp","tme_gdp","debt_interest_gdp","psnd_level_gbp_billion","gdp_level_gbp_billion"]:
    if c in w.columns:
        w[c] = pd.to_numeric(w[c], errors="coerce")

derived = []

# PSNB%GDP = TME% - Receipts%
if {"tme_gdp","receipts_gdp"}.issubset(w.columns):
    psnb = (w["tme_gdp"] - w["receipts_gdp"]).rename("psnb_gdp").dropna()
    derived.append(psnb)

# Primary spend%GDP = TME% - Debt interest%
if {"tme_gdp","debt_interest_gdp"}.issubset(w.columns):
    prim = (w["tme_gdp"] - w["debt_interest_gdp"]).rename("primary_spend_gdp").dropna()
    derived.append(prim)

# PSND%GDP from levels (if present)
if {"psnd_level_gbp_billion","gdp_level_gbp_billion"}.issubset(w.columns):
    psnd_pct = (100 * w["psnd_level_gbp_billion"] / w["gdp_level_gbp_billion"]).rename("psnd_gdp").dropna()
    derived.append(psnd_pct)

if derived:
    d = pd.concat(derived, axis=1).stack().rename("value").reset_index()
    d.columns = ["fy","series_id","value"]

    units_map = {
        "psnb_gdp": "percent_of_gdp",
        "primary_spend_gdp": "percent_of_gdp",
        "psnd_gdp": "percent_of_gdp",
    }
    d["units"] = d["series_id"].map(units_map)
    d["source_logical_id"] = "derived"
    d["source_table_id"] = "derived"
    d["source_path"] = "derived"

    # Merge: keep existing rows; fill only missing derived values
    base = df.copy()
    base = base[~base["series_id"].isin(units_map.keys()) | base["value"].notna()]
    merged = pd.concat([base, d], ignore_index=True)
    merged = merged.drop_duplicates(subset=["fy","series_id"], keep="first")

    ordcols = ["fy","series_id","value","units","source_logical_id","source_table_id","source_path"]
    merged = merged[ordcols].sort_values(["series_id","fy"]).reset_index(drop=True)
    OUTCSV.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(OUTCSV, index=False)

    print("Wrote derived series:", sorted(units_map.keys()))
else:
    print("No derived series could be computed (missing inputs).")
