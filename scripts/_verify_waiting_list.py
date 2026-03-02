#!/usr/bin/env python3
"""Verify rebuilt fact_waiting_list data quality."""
import pandas as pd
df = pd.read_parquet("artifacts/tables/fact_waiting_list.parquet")
print(f"Shape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"\nCategories: {sorted(df.category.unique())}")
print(f"Years: {sorted(df.report_year.unique())}")
print(f"\nCountries (sample): {sorted(df.country.unique())[:15]}")
print(f"Total unique countries: {df.country.nunique()}")
print(f"\nPK unique: {df.duplicated(subset=['report_year','category','country']).sum()} dupes")
print(f"Negative counts: {(df.count_waiting < 0).sum()}")
print(f"Null counts: {df.count_waiting.isna().sum()}")
print(f"\ncount_waiting range: [{df.count_waiting.min():,}, {df.count_waiting.max():,}]")
print(f"\n--- Sample rows by category ---")
for cat in sorted(df.category.unique()):
    sub = df[df.category == cat]
    print(f"\n{cat}: {len(sub)} rows, total={sub.count_waiting.sum():,}")
    top = sub.nlargest(3, "count_waiting")[["country","count_waiting"]]
    print(top.to_string(index=False))

# Cross-check: F4 worldwide total should be 2,199,512
f4_ww = df[(df.category == "F4") & (df.country == "Worldwide Total")]
if len(f4_ww):
    print(f"\n--- Cross-check ---")
    print(f"F4 Worldwide Total: {f4_ww.iloc[0].count_waiting:,} (expected: 2,199,512)")
eb1_ww = df[(df.category == "EB1") & (df.country == "Worldwide Total")]
if len(eb1_ww):
    print(f"EB1 Worldwide Total: {eb1_ww.iloc[0].count_waiting:,} (expected: 20,582)")
eb5_ww = df[(df.category == "EB5") & (df.country == "Worldwide Total")]
if len(eb5_ww):
    print(f"EB5 Worldwide Total: {eb5_ww.iloc[0].count_waiting:,} (expected: 39,883)")

# Check no garbled data (the old bug)
garbled = df[df.country.str.match(r"^\d+$", na=False)]
if len(garbled):
    print(f"\nWARNING: {len(garbled)} rows with numeric 'country' values (garbled):")
    print(garbled)
else:
    print(f"\nOK: No garbled country values")
garbled_cat = df[df.category.str.match(r"^[\d,]+$", na=False)]
if len(garbled_cat):
    print(f"WARNING: {len(garbled_cat)} rows with numeric 'category' values (garbled):")
    print(garbled_cat)
else:
    print(f"OK: No garbled category values")
