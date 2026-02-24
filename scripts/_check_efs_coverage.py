#!/usr/bin/env python3
"""Quick EFS coverage and correlation check."""
import pandas as pd
from pathlib import Path

TABLES = Path("artifacts/tables")

ef = pd.read_parquet(TABLES / "employer_features.parquet")
print(f"employer_features: {len(ef):,} rows")
print(f"  columns: {list(ef.columns)}")

if "wage_ratio" in ef.columns:
    wr = ef["wage_ratio"]
    cov = wr.notna().mean()
    print(f"  wage_ratio coverage: {cov:.1%}  ({wr.notna().sum():,} / {len(ef):,})")
    print(f"  wage_ratio median:   {wr.median():.3f}")
else:
    print("  wage_ratio column: MISSING")

soc_col = next((c for c in ef.columns if "soc" in c.lower()), None)
if soc_col:
    soc_cov = ef[soc_col].notna().mean()
    print(f"  {soc_col} coverage:  {soc_cov:.1%}")
else:
    print("  no soc column found")

efs_path = TABLES / "employer_friendliness_scores.parquet"
if efs_path.exists():
    efs = pd.read_parquet(efs_path)
    print(f"\nemployer_friendliness_scores: {len(efs):,} rows")
    print(f"  columns: {list(efs.columns)}")
    score_col = next((c for c in efs.columns if "score" in c.lower()), None)
    if "wage_ratio" in efs.columns and score_col:
        pair = efs[["wage_ratio", score_col]].dropna()
        corr = pair.corr().iloc[0, 1]
        print(f"  corr(wage_ratio, {score_col}): {corr:.3f}  (n={len(pair):,})")
    elif "wage_ratio" in efs.columns:
        print("  wage_ratio present but no score column for corr")
    else:
        print("  wage_ratio not in employer_friendliness_scores")
