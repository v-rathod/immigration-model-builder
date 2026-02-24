#!/usr/bin/env python3
"""Check: what data does the pd_forecast model see?"""
import pandas as pd, json
from pathlib import Path

# 1. Latest bulletin months available
fc = pd.read_parquet("artifacts/tables/fact_cutoffs")
fc2 = fc.sort_values(["bulletin_year", "bulletin_month"])
latest = fc2[["bulletin_year", "bulletin_month"]].drop_duplicates().tail(10)
print("=== Latest 10 bulletin months in fact_cutoffs ===")
for _, r in latest.iterrows():
    print(f"  {int(r.bulletin_year)}-{int(r.bulletin_month):02d}")

print()

# 2. EB2-India FAD history (last 12)
eb2 = fc[(fc["category"] == "EB2") & (fc["country"] == "IND") & (fc["chart"] == "FAD")]
eb2 = eb2.sort_values(["bulletin_year", "bulletin_month"])
print("=== Last 12 EB2-India FAD cutoff dates ===")
for _, r in eb2.tail(12).iterrows():
    print(f"  {int(r.bulletin_year)}-{int(r.bulletin_month):02d}  cutoff={r.cutoff_date}  flag={r.status_flag}")

print()

# 3. 2026 bulletin data
b2026 = fc[fc["bulletin_year"].astype(int) == 2026]
print(f"=== 2026 bulletins: {len(b2026)} rows, months: {sorted(b2026.bulletin_month.astype(int).unique())} ===")

print()

# 4. What does fact_cutoff_trends (model input) show?
ct = pd.read_parquet("artifacts/tables/fact_cutoff_trends.parquet")
eb2_ct = ct[(ct["category"] == "EB2") & (ct["country"] == "IND") & (ct["chart"] == "FAD")]
eb2_ct = eb2_ct.sort_values(["bulletin_year", "bulletin_month"])
print("=== Last 6 EB2-India FAD trend rows (model input) ===")
cols = ["bulletin_year", "bulletin_month", "cutoff_date", "monthly_advancement_days", "retrogression_flag"]
cols = [c for c in cols if c in eb2_ct.columns]
for _, r in eb2_ct.tail(6).iterrows():
    print(f"  {int(r.bulletin_year)}-{int(r.bulletin_month):02d}  cutoff={r.cutoff_date}  adv={r.get('monthly_advancement_days','?')}d  retro={r.get('retrogression_flag','?')}")

print()

# 5. Model parameters for EB2-India
mp = Path("artifacts/models/pd_forecast_model.json")
if mp.exists():
    with open(mp) as f:
        model = json.load(f)
    for s in model.get("series", []):
        if s["category"] == "EB2" and s["country"] == "IND" and s["chart"] == "FAD":
            print("=== pd_forecast model params: EB2-India FAD ===")
            for k, v in s.items():
                print(f"  {k}: {v}")
            break
