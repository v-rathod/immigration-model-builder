#!/usr/bin/env python3
"""Quick check: Do P2 artifacts support EB2 priority date prediction?"""
import pandas as pd

print("=== pd_forecasts.parquet ===")
pf = pd.read_parquet("artifacts/tables/pd_forecasts.parquet")
print(f"Shape: {pf.shape}")
print(f"Columns: {list(pf.columns)}")
print(f"Unique categories: {sorted(pf['category'].unique())}")
print(f"Unique charts: {sorted(pf['chart'].unique())}")
print(f"Unique countries: {sorted(pf['country'].unique())}")
print()

# EB2 specifically
eb2 = pf[pf["category"].str.contains("2", na=False)]
print(f"EB2 forecast rows: {len(eb2)}")
if len(eb2) > 0:
    print(f"  Countries: {sorted(eb2['country'].unique())}")
    print(f"  Charts: {sorted(eb2['chart'].unique())}")
    print(f"  Forecast months: {eb2['forecast_month'].min()} to {eb2['forecast_month'].max()}")
    print()
    print("Sample EB2-India forecasts:")
    sample = eb2[eb2["country"] == "INDIA"].sort_values("forecast_month").head(6)
    if len(sample) > 0:
        cols = [c for c in ["category", "country", "chart", "forecast_month", "forecast_date", "lower_90", "upper_90"] if c in sample.columns]
        print(sample[cols].to_string(index=False))
    else:
        print("  (no INDIA rows)")
else:
    print("  *** NO EB2 ROWS IN pd_forecasts ***")

print()
print("=== fact_cutoffs (EB2 historical) ===")
fc = pd.read_parquet("artifacts/tables/fact_cutoffs")
eb2_hist = fc[fc["category"].str.contains("2", na=False)]
print(f"Total cutoffs rows: {len(fc)}")
print(f"EB2 rows: {len(eb2_hist)}")
if len(eb2_hist) > 0:
    print(f"  Countries: {sorted(eb2_hist['country'].unique())}")
    yrs = eb2_hist["bulletin_year"].astype(int)
    print(f"  Year range: {yrs.min()} - {yrs.max()}")
    print()
    # Recent EB2-India
    india = eb2_hist[eb2_hist["country"] == "INDIA"].sort_values(["bulletin_year", "bulletin_month"])
    recent = india.tail(6)
    if len(recent) > 0:
        print("Recent EB2-India cutoffs:")
        cols = [c for c in ["bulletin_year", "bulletin_month", "chart", "category", "country", "cutoff_date", "status_flag"] if c in recent.columns]
        print(recent[cols].to_string(index=False))

print()
print("=== category_movement_metrics (EB2) ===")
try:
    cm = pd.read_parquet("artifacts/tables/category_movement_metrics.parquet")
    eb2_cm = cm[cm["category"].str.contains("2", na=False)]
    print(f"Total rows: {len(cm)}, EB2 rows: {len(eb2_cm)}")
    if len(eb2_cm) > 0:
        print(f"  Columns: {list(cm.columns)}")
except Exception as e:
    print(f"  Error: {e}")

print()
print("=== backlog_estimates (EB2) ===")
try:
    be = pd.read_parquet("artifacts/tables/backlog_estimates.parquet")
    eb2_be = be[be["category"].str.contains("2", na=False)]
    print(f"Total rows: {len(be)}, EB2 rows: {len(eb2_be)}")
    if len(eb2_be) > 0:
        print(f"  Columns: {list(be.columns)}")
        sample = eb2_be[eb2_be["country"] == "INDIA"].tail(3) if "country" in eb2_be.columns else eb2_be.tail(3)
        if len(sample) > 0:
            print(sample.to_string(index=False))
except Exception as e:
    print(f"  Error: {e}")
