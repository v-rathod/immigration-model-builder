#!/usr/bin/env python3
"""Check EB2-India DFF (Dates for Filing) from March 2026 bulletin."""
import pandas as pd

fc = pd.read_parquet("artifacts/tables/fact_cutoffs")

# EB2-India DFF - latest entries
dff = fc[(fc["category"] == "EB2") & (fc["country"] == "IND") & (fc["chart"] == "DFF")]
dff = dff.sort_values(["bulletin_year", "bulletin_month"])

print("=== Last 6 EB2-India DFF (Dates for Filing) ===")
for _, r in dff.tail(6).iterrows():
    print(f"  {int(r.bulletin_year)}-{int(r.bulletin_month):02d}  cutoff={r.cutoff_date}  flag={r.status_flag}")

print()
# March 2026 specifically
mar = dff[(dff["bulletin_year"].astype(int) == 2026) & (dff["bulletin_month"].astype(int) == 3)]
if len(mar) > 0:
    print("=== March 2026 EB2-India DFF ===")
    for _, r in mar.iterrows():
        print(f"  Cutoff: {r.cutoff_date}  flag={r.status_flag}")
else:
    print("No March 2026 DFF row found for EB2-India")
