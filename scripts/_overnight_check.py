#!/usr/bin/env python3
"""Quick overnight gate status check."""
import pandas as pd
from pathlib import Path

ROOT = Path("/Users/vrathod1/dev/NorthStar/immigration-model-builder")
T = ROOT / "artifacts" / "tables"

def check_table(name, path, pk_cols=None):
    try:
        df = pd.read_parquet(path)
        dup = df.duplicated(subset=pk_cols).sum() if pk_cols else "n/a"
        print(f"  {name}: {len(df):,} rows  pk_dups={dup}")
        return df
    except Exception as e:
        print(f"  {name}: ERROR {e}")
        return None

print("=== VB TRENDS / BACKLOG ===")
check_table("fact_cutoffs_all", T/"fact_cutoffs_all.parquet",
            ["bulletin_year","bulletin_month","category","country","chart"])
check_table("fact_cutoff_trends", T/"fact_cutoff_trends.parquet",
            ["bulletin_year","bulletin_month","category","country","chart"])
check_table("backlog_estimates", T/"backlog_estimates.parquet",
            ["bulletin_year","bulletin_month","category","country"])

print("\n=== PERM PK ===")
import os
perm_root = T / "fact_perm"
total = 0
pk_dups_total = 0
for fy_dir in sorted(perm_root.iterdir()) if perm_root.exists() else []:
    for pf in fy_dir.glob("*.parquet"):
        df = pd.read_parquet(pf, columns=["case_number"])
        dups = df.duplicated(subset=["case_number"]).sum()
        total += len(df)
        pk_dups_total += dups
print(f"  fact_perm total: {total:,} rows  within-partition pk_dups={pk_dups_total}")

print("\n=== EFS / FEATURES ===")
ef = pd.read_parquet(T/"employer_features.parquet")
print(f"  employer_features: {len(ef):,} rows")
for col in ["wage_ratio","wage_ratio_med","wage_ratio_p75"]:
    if col in ef.columns:
        cov = ef[col].notna().mean()
        print(f"    {col} coverage: {cov:.1%}")
efs = pd.read_parquet(T/"employer_friendliness_scores.parquet")
print(f"  employer_friendliness_scores: {len(efs):,} rows")

print("\n=== DIMS ===")
for name, fname in [("dim_employer","dim_employer.parquet"),
                    ("dim_soc","dim_soc.parquet"),
                    ("dim_area","dim_area.parquet"),
                    ("dim_country","dim_country.parquet")]:
    df = pd.read_parquet(T/fname)
    print(f"  {name}: {len(df):,} rows")

print("\n=== RI / BENCHMARKS ===")
sb = pd.read_parquet(T/"salary_benchmarks.parquet")
null_any = sb.isnull().any(axis=1).mean()
print(f"  salary_benchmarks: {len(sb):,} rows  null_any_pct={null_any:.1%}")
wg = pd.read_parquet(T/"worksite_geo_metrics.parquet")
print(f"  worksite_geo_metrics: {len(wg):,} rows")

print("\nDONE")
