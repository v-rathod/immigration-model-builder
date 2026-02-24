#!/usr/bin/env python3
"""Detailed EFS coverage diagnostics."""
import pandas as pd
from pathlib import Path

TABLES = Path("artifacts/tables")

ef = pd.read_parquet(TABLES / "employer_features.parquet")
efs = pd.read_parquet(TABLES / "employer_friendliness_scores.parquet")

# wage_ratio variants coverage
for col in ["wage_ratio", "wage_ratio_med", "wage_ratio_p75"]:
    if col in ef.columns:
        cov = ef[col].notna().mean()
        print(f"ef.{col}: {cov:.1%}  ({ef[col].notna().sum():,}/{len(ef):,})")
    if col in efs.columns:
        cov = efs[col].notna().mean()
        print(f"efs.{col}: {cov:.1%}  ({efs[col].notna().sum():,}/{len(efs):,})")

# soc_code coverage
print(f"\nef.soc_code non-null: {ef['soc_code'].notna().sum():,} / {len(ef):,}")
print(f"ef.soc_code sample nulls: {ef['soc_code'].isna().sum():,}")
print(f"ef.scope value_counts:\n{ef['scope'].value_counts()}")

# efs score distribution and corr
score_col = "efs"
if score_col in efs.columns:
    for wrc in ["wage_ratio_med", "wage_ratio_p75"]:
        if wrc in efs.columns:
            pair = efs[[wrc, score_col]].dropna()
            corr = pair.corr().iloc[0,1]
            print(f"\ncorr({wrc}, {score_col}): {corr:.3f}  (n={len(pair):,})")
