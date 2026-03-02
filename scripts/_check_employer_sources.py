#!/usr/bin/env python3
"""Quick check of all data sources relevant to employer scoring."""
import pandas as pd
from pathlib import Path

T = Path("artifacts/tables")

for name in [
    "fact_lca", "fact_warn_events.parquet", "fact_h1b_employer_hub.parquet",
    "salary_benchmarks.parquet", "fact_perm", "dim_employer.parquet",
    "fact_oews", "dim_area.parquet", "dim_soc.parquet",
    "employer_features.parquet", "employer_friendliness_scores.parquet",
    "employer_friendliness_scores_ml.parquet",
    "employer_monthly_metrics.parquet", "employer_risk_features.parquet",
    "fact_uscis_approvals.parquet",
]:
    p = T / name
    if p.exists():
        try:
            df = pd.read_parquet(p)
            print(f"{name:50s} {len(df):>12,} rows  cols={list(df.columns)[:10]}")
        except Exception as e:
            print(f"{name:50s} ERROR: {e}")
    else:
        print(f"{name:50s} NOT FOUND")
