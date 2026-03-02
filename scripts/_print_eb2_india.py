#!/usr/bin/env python3
"""Print EB2 India FAD and DFF tables from fact_cutoff_trends."""
import pandas as pd

df = pd.read_parquet("artifacts/tables/fact_cutoff_trends.parquet")

pd.set_option("display.max_rows", 200)
pd.set_option("display.width", 160)
pd.set_option("display.max_columns", 12)

cols = ["bulletin_year", "bulletin_month", "chart", "status_flag", "cutoff_date",
        "monthly_advancement_days", "velocity_3m"]

for chart_type in ["FAD", "DFF"]:
    eb2 = df[(df["category"] == "EB2") & (df["country"] == "IND") & (df["chart"] == chart_type)].copy()
    eb2 = eb2.sort_values(["bulletin_year", "bulletin_month"])
    print(f"=== EB2 India {chart_type} — {len(eb2)} rows ===")
    if len(eb2) == 0:
        print("  (no data)")
    else:
        print(eb2[cols].to_string(index=False))
    print()

# Also check countries available for EB2
countries = sorted(df[df["category"] == "EB2"]["country"].unique())
print(f"EB2 countries: {countries}")
