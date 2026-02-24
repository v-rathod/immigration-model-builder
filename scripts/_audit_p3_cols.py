#!/usr/bin/env python3
"""Print columns for key P3-relevant tables."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
T = ROOT / "artifacts" / "tables"

tables = {
    "employer_features": T / "employer_features.parquet",
    "employer_friendliness_scores": T / "employer_friendliness_scores.parquet",
    "employer_monthly_metrics": T / "employer_monthly_metrics.parquet",
    "fact_cutoff_trends": T / "fact_cutoff_trends.parquet",
    "category_movement_metrics": T / "category_movement_metrics.parquet",
    "backlog_estimates": T / "backlog_estimates.parquet",
    "salary_benchmarks": T / "salary_benchmarks.parquet",
    "soc_demand_metrics": T / "soc_demand_metrics.parquet",
    "worksite_geo_metrics": T / "worksite_geo_metrics.parquet",
    "employer_risk_features": T / "employer_risk_features.parquet",
    "processing_times_trends": T / "processing_times_trends.parquet",
    "fact_warn_events": T / "fact_warn_events.parquet",
    "visa_demand_metrics": T / "visa_demand_metrics.parquet",
    "fact_cutoffs_all": T / "fact_cutoffs_all.parquet",
    "dim_visa_ceiling": T / "dim_visa_ceiling.parquet",
    "fact_waiting_list": T / "fact_waiting_list.parquet",
}

for name, path in tables.items():
    df = pd.read_parquet(path)
    print(f"### {name} ({len(df):,} rows)")
    print(f"  Cols: {list(df.columns)}")
    print()
