#!/usr/bin/env python3
"""Print EB2 India FAD vs DFF side-by-side table for past 10 years."""
import pandas as pd

df = pd.read_parquet("artifacts/tables/fact_cutoff_trends.parquet")

# Filter EB2 India
eb2 = df[(df["category"] == "EB2") & (df["country"] == "IND")].copy()

# Pivot: one row per (year, month), columns = FAD cutoff, DFF cutoff
fad = eb2[eb2["chart"] == "FAD"][["bulletin_year","bulletin_month","cutoff_date","status_flag"]].copy()
fad = fad.rename(columns={"cutoff_date": "FAD", "status_flag": "fad_flag"})

dff = eb2[eb2["chart"] == "DFF"][["bulletin_year","bulletin_month","cutoff_date","status_flag"]].copy()
dff = dff.rename(columns={"cutoff_date": "DFF", "status_flag": "dff_flag"})

merged = fad.merge(dff, on=["bulletin_year","bulletin_month"], how="outer")
merged = merged.sort_values(["bulletin_year","bulletin_month"]).reset_index(drop=True)

# Filter to last 10 years (2016-2026)
merged = merged[merged["bulletin_year"] >= 2016].copy()

# Format
def fmt(row, col, flag_col):
    flag = row.get(flag_col, "")
    val = row.get(col)
    if pd.isna(val) and flag == "":
        return ""  # no data for this chart/month
    if flag == "C":
        return "Current"
    if flag == "U":
        return "Unavailable"
    if pd.isna(val):
        return ""
    return val.strftime("%Y-%m-%d")

month_names = {1:"Jan",2:"Feb",3:"Mar",4:"Apr",5:"May",6:"Jun",
               7:"Jul",8:"Aug",9:"Sep",10:"Oct",11:"Nov",12:"Dec"}

rows = []
for _, r in merged.iterrows():
    mon = month_names.get(int(r["bulletin_month"]), str(int(r["bulletin_month"])))
    label = f"{mon} {int(r['bulletin_year'])}"
    fad_str = fmt(r, "FAD", "fad_flag")
    dff_str = fmt(r, "DFF", "dff_flag")
    rows.append((label, fad_str, dff_str))

# Print table
print(f"{'Month/Year':<12} {'FAD':<14} {'DFF':<14}")
print(f"{'-'*12} {'-'*14} {'-'*14}")
for label, fad_str, dff_str in rows:
    print(f"{label:<12} {fad_str:<14} {dff_str:<14}")

print(f"\nTotal rows: {len(rows)}")
