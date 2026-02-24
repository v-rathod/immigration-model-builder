#!/usr/bin/env python3
"""Inspect Employer Friendliness Score models — v1 (rules) and v2 (ML)."""

import pandas as pd
from pathlib import Path

TABLES = Path("artifacts/tables")

# ── EFS v1 (Rules-Based) ──
efs = pd.read_parquet(TABLES / "employer_friendliness_scores.parquet")
print("=" * 70)
print("EFS v1 (RULES-BASED)")
print("=" * 70)
print(f"Rows: {len(efs):,}")
print(f"Columns: {efs.columns.tolist()}")
print(f"\nTier distribution:")
print(efs["efs_tier"].value_counts().to_string())

valid = efs.dropna(subset=["efs"])
print(f"\nWith valid score: {len(valid):,} / {len(efs):,}")
if len(valid):
    print(f"EFS stats: mean={valid['efs'].mean():.1f}  median={valid['efs'].median():.1f}  "
          f"std={valid['efs'].std():.1f}  range=[{valid['efs'].min():.1f}, {valid['efs'].max():.1f}]")

# ── EFS v2 (ML-Based) ──
print()
print("=" * 70)
print("EFS v2 (ML-BASED)")
print("=" * 70)
ml = pd.read_parquet(TABLES / "employer_friendliness_scores_ml.parquet")
print(f"Rows: {len(ml):,}")
print(f"Columns: {ml.columns.tolist()}")
if "efs_ml" in ml.columns and len(ml):
    print(f"EFS-ML stats: mean={ml['efs_ml'].mean():.1f}  median={ml['efs_ml'].median():.1f}  "
          f"range=[{ml['efs_ml'].min():.1f}, {ml['efs_ml'].max():.1f}]")
    if "efs_ml_tier" in ml.columns:
        print(f"\nTier distribution:")
        print(ml["efs_ml_tier"].value_counts().to_string())

# ── Top employers ──
print()
print("=" * 70)
print("TOP 15 EMPLOYERS BY EFS v1")
print("=" * 70)
overall = valid[valid["scope"] == "overall"] if "scope" in valid.columns else valid
top = overall.nlargest(15, "efs")
cols = ["employer_name", "efs", "efs_tier", "approval_rate_24m", "wage_ratio_med", "n_24m"]
cols = [c for c in cols if c in top.columns]
for _, r in top.iterrows():
    name = str(r.get("employer_name", "?"))[:50]
    ar = r.get("approval_rate_24m", 0)
    wr = r.get("wage_ratio_med", 0)
    n = int(r.get("n_24m", 0))
    print(f"  {r['efs']:5.1f} {r.get('efs_tier',''):>10}  AR={ar:.0%}  WR={wr:.2f}  n={n:>4}  {name}")

# ── Bottom employers ──
print()
print("=" * 70)
print("BOTTOM 15 EMPLOYERS BY EFS v1 (with valid score)")
print("=" * 70)
bottom = overall.nsmallest(15, "efs")
for _, r in bottom.iterrows():
    name = str(r.get("employer_name", "?"))[:50]
    ar = r.get("approval_rate_24m", 0)
    wr = r.get("wage_ratio_med", 0)
    n = int(r.get("n_24m", 0))
    print(f"  {r['efs']:5.1f} {r.get('efs_tier',''):>10}  AR={ar:.0%}  WR={wr:.2f}  n={n:>4}  {name}")

# ── Well-known tech companies ──
print()
print("=" * 70)
print("WELL-KNOWN EMPLOYERS")
print("=" * 70)
known = ["GOOGLE", "MICROSOFT", "APPLE", "AMAZON", "META", "INFOSYS", "TATA",
         "COGNIZANT", "WIPRO", "ACCENTURE", "IBM", "INTEL", "NVIDIA", "DELOITTE",
         "TESLA", "UBER", "WALMART", "JPMORGAN", "GOLDMAN"]
for name in known:
    matches = overall[overall["employer_name"].str.contains(name, case=False, na=False)]
    if len(matches):
        best = matches.nlargest(1, "n_24m").iloc[0]
        efs_val = best.get("efs", float("nan"))
        ar = best.get("approval_rate_24m", 0)
        wr = best.get("wage_ratio_med", 0)
        n = int(best.get("n_24m", 0))
        tier = best.get("efs_tier", "")
        ename = str(best.get("employer_name", ""))[:45]
        if pd.isna(efs_val):
            print(f"  {'N/A':>5} {'Unrated':>10}  AR={ar:.0%}  WR={wr:.2f}  n={n:>4}  {ename}")
        else:
            print(f"  {efs_val:5.1f} {tier:>10}  AR={ar:.0%}  WR={wr:.2f}  n={n:>4}  {ename}")
    else:
        print(f"  {'---':>5} {'not found':>10}  {name}")
