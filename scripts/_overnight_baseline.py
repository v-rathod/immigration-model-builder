#!/usr/bin/env python3
"""Overnight hardening baseline check."""
import pandas as pd, pyarrow.parquet as pq
from pathlib import Path

TABLES = Path("artifacts/tables")

# fact_perm
files = sorted((TABLES/"fact_perm").rglob("*.parquet"))
total = sum(pq.read_metadata(f).num_rows for f in files)
print(f"fact_perm: {len(files)} files, {total:,} rows")

# PK check on sample
df_p = pd.concat([pd.read_parquet(f, columns=["case_number","fiscal_year"]) for f in files[:5]], ignore_index=True)
dup_rate = df_p.duplicated().sum() / len(df_p)
print(f"  PK dup rate (sample 5 parts): {dup_rate:.3%}")

# fact_cutoff_trends
df = pd.read_parquet(TABLES/"fact_cutoff_trends.parquet")
pk = ["bulletin_year","bulletin_month","category","country"]
print(f"fact_cutoff_trends: {len(df):,} rows, pk_unique={df[pk].duplicated().sum()==0}, expected=8315")

# backlog
df2 = pd.read_parquet(TABLES/"backlog_estimates.parquet")
print(f"backlog_estimates: {len(df2):,} rows, expected=8315")

# employer_features
ef = pd.read_parquet(TABLES/"employer_features.parquet")
print(f"employer_features: {len(ef):,} rows  cols={list(ef.columns[:10])}")
for col in ["wage_ratio","wage_ratio_med","wage_ratio_p75"]:
    if col in ef.columns:
        print(f"  {col} coverage: {ef[col].notna().mean():.1%}")
    else:
        print(f"  {col}: MISSING")

# EFS
efs = pd.read_parquet(TABLES/"employer_friendliness_scores.parquet")
print(f"employer_friendliness_scores: {len(efs):,} rows  cols={list(efs.columns[:8])}")

# salary_benchmarks null pct
sb = pd.read_parquet(TABLES/"salary_benchmarks.parquet")
null_pct = (sb[["p25_annual","p50_annual","p75_annual"]].isna().any(axis=1)).mean()
print(f"salary_benchmarks: {len(sb):,} rows  pct_any_null_pctile={null_pct:.1%}")

# worksite_geo
wg = pd.read_parquet(TABLES/"worksite_geo_metrics.parquet")
if "soc_code" in wg.columns and "soc_mapped" in wg.columns:
    cov = wg["soc_mapped"].mean()
    print(f"worksite_geo_metrics: {len(wg):,} rows  soc_cov={cov:.1%}")
elif "soc_code" in wg.columns:
    print(f"worksite_geo_metrics: {len(wg):,} rows")
else:
    print(f"worksite_geo_metrics: {len(wg):,} rows  cols={list(wg.columns[:6])}")

print("BASELINE_CHECK_DONE")
