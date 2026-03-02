#!/usr/bin/env python3
"""Cross-check fact_waiting_list specific values."""
import pandas as pd
df = pd.read_parquet("artifacts/tables/fact_waiting_list.parquet")

# Cross-check with year filter
print("--- Cross-checks ---")
checks = [("F4",2023,2199512),("F4",2022,2220476),("EB1",2023,20582),("EB5",2023,39883)]
for cat, yr, expected in checks:
    row = df[(df.category==cat)&(df.country=="Worldwide Total")&(df.report_year==yr)]
    actual = row.iloc[0].count_waiting if len(row) else "MISSING"
    ok = "OK" if actual == expected else "MISMATCH"
    print(f"  {ok}  {cat} {yr}: {actual:,} (expected {expected:,})")

# Check for region contamination
regions = df[df.country.isin(["Africa","Asia","Europe","N. America*","Oceania","S. America"])]
print(f"\nRegion rows: {len(regions)}")
if len(regions):
    print(regions[["report_year","category","country","count_waiting"]].to_string(index=False))

# Check CSV stub rows
csv_rows = df[df.source_file.str.contains("csv", na=False)]
print(f"\nCSV stub rows: {len(csv_rows)}")
if len(csv_rows):
    print(csv_rows[["report_year","category","country","count_waiting"]].to_string(index=False))

# Year breakdown
print(f"\nBy year:")
for yr in sorted(df.report_year.unique()):
    sub = df[df.report_year == yr]
    print(f"  {yr}: {len(sub)} rows, categories={sorted(sub.category.unique())}")
