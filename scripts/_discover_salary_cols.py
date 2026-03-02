#!/usr/bin/env python3
"""Discover full column lists and sample data for LCA/H1B/PERM wage fields."""
import pandas as pd
from pathlib import Path

T = Path("artifacts/tables")

print("=" * 80)
print("fact_lca — FULL COLUMNS")
print("=" * 80)
lca = pd.read_parquet(T / "fact_lca")
print(f"Rows: {len(lca):,}")
for c in lca.columns:
    nn = lca[c].notna().sum()
    pct = nn / len(lca) * 100
    sample = lca[c].dropna().head(3).tolist()
    print(f"  {c:40s} {nn:>10,} non-null ({pct:5.1f}%)  sample={sample}")

print("\n" + "=" * 80)
print("fact_h1b_employer_hub — FULL COLUMNS")
print("=" * 80)
h1b = pd.read_parquet(T / "fact_h1b_employer_hub.parquet")
print(f"Rows: {len(h1b):,}")
for c in h1b.columns:
    nn = h1b[c].notna().sum()
    pct = nn / len(h1b) * 100
    sample = h1b[c].dropna().head(3).tolist()
    print(f"  {c:40s} {nn:>10,} non-null ({pct:5.1f}%)  sample={sample}")

print("\n" + "=" * 80)
print("fact_perm — WAGE-RELATED COLUMNS")
print("=" * 80)
perm = pd.read_parquet(T / "fact_perm")
print(f"Rows: {len(perm):,}")
wage_cols = [c for c in perm.columns if any(w in c.lower() for w in ["wage", "salary", "pw_", "pay"])]
print(f"Wage-related columns: {wage_cols}")
for c in wage_cols:
    nn = perm[c].notna().sum()
    pct = nn / len(perm) * 100
    sample = perm[c].dropna().head(3).tolist()
    print(f"  {c:40s} {nn:>10,} non-null ({pct:5.1f}%)  sample={sample}")
# Also show all columns
print(f"\nALL fact_perm columns ({len(perm.columns)}):")
for c in perm.columns:
    print(f"  {c}")

print("\n" + "=" * 80)
print("fact_uscis_approvals — FULL COLUMNS")
print("=" * 80)
ua = pd.read_parquet(T / "fact_uscis_approvals.parquet")
print(f"Rows: {len(ua):,}")
print(ua.to_string())
