#!/usr/bin/env python3
"""Deep-dive sanity checks for fact_perm, fact_lca, and DOS visa tables."""
import pandas as pd
import numpy as np
from pathlib import Path

TABLES = Path('artifacts/tables')

def section(title):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")

# ── fact_perm ──────────────────────────────────────────────────────────────────
section("FACT_PERM")
df = pd.read_parquet(TABLES / 'fact_perm')
df['fiscal_year'] = df['fiscal_year'].astype(int)
print(f"Shape: {df.shape}")
print(f"Columns: {sorted(df.columns.tolist())}")

# FY distribution
print("\n--- Rows per FY ---")
fy = df['fiscal_year'].value_counts().sort_index()
for y, n in fy.items():
    print(f"  FY{y}: {n:>9,}")

# Case status distribution
print("\n--- Case status distribution ---")
cs = df['case_status'].value_counts()
for s, n in cs.items():
    print(f"  {s}: {n:>9,} ({100*n/len(df):.1f}%)")

# Wage plausibility
wage_col = None
for c in ['wage_offer_from', 'pw_amount', 'wage_rate_of_pay_from']:
    if c in df.columns:
        wage_col = c
        break
if wage_col:
    wages = pd.to_numeric(df[wage_col], errors='coerce').dropna()
    print(f"\n--- Wage column: {wage_col} ---")
    print(f"  Non-null: {len(wages):,} ({100*len(wages)/len(df):.1f}%)")
    print(f"  Median: ${wages.median():,.0f}")
    print(f"  Mean: ${wages.mean():,.0f}")
    print(f"  Min: ${wages.min():,.0f}")
    print(f"  Max: ${wages.max():,.0f}")
    print(f"  P5: ${wages.quantile(0.05):,.0f}")
    print(f"  P95: ${wages.quantile(0.95):,.0f}")
    lt_15k = (wages < 15000).sum()
    gt_500k = (wages > 500000).sum()
    print(f"  < $15K: {lt_15k:,} ({100*lt_15k/len(wages):.2f}%)")
    print(f"  > $500K: {gt_500k:,} ({100*gt_500k/len(wages):.2f}%)")

# SOC code coverage
if 'soc_code' in df.columns:
    print(f"\n--- SOC code coverage by FY ---")
    for y in sorted(df['fiscal_year'].unique()):
        sub = df[df['fiscal_year'] == y]
        null_pct = sub['soc_code'].isna().mean() * 100
        print(f"  FY{y}: {null_pct:.1f}% null SOC")

# employer_id coverage
if 'employer_id' in df.columns:
    null_emp = df['employer_id'].isna().mean() * 100
    print(f"\n--- employer_id null rate: {null_emp:.1f}% ---")
    distinct_emp = df['employer_id'].nunique()
    print(f"  Distinct employers: {distinct_emp:,}")

# ── fact_lca ──────────────────────────────────────────────────────────────────
section("FACT_LCA")
df = pd.read_parquet(TABLES / 'fact_lca')
df['fiscal_year'] = df['fiscal_year'].astype(int)
print(f"Shape: {df.shape}")
print(f"Columns: {sorted(df.columns.tolist())}")

# FY distribution
print("\n--- Rows per FY ---")
fy = df['fiscal_year'].value_counts().sort_index()
for y, n in fy.items():
    print(f"  FY{y}: {n:>9,}")

# Case status
if 'case_status' in df.columns:
    print("\n--- Case status distribution ---")
    cs = df['case_status'].value_counts()
    for s, n in cs.items():
        print(f"  {s}: {n:>9,} ({100*n/len(df):.1f}%)")

# Wage column
wage_col = None
for c in ['wage_rate_from', 'wage_rate_of_pay_from', 'wage_rate']:
    if c in df.columns:
        wage_col = c
        break
if wage_col:
    wages = pd.to_numeric(df[wage_col], errors='coerce').dropna()
    print(f"\n--- Wage column: {wage_col} ---")
    print(f"  Non-null: {len(wages):,} ({100*len(wages)/len(df):.1f}%)")
    print(f"  Median: ${wages.median():,.0f}")
    print(f"  Mean: ${wages.mean():,.0f}")
    print(f"  < $15K: {(wages < 15000).sum():,}")
    print(f"  > $500K: {(wages > 500000).sum():,}")
    # Check if this is annual vs hourly
    lt_100 = (wages < 100).sum()
    print(f"  < $100 (likely hourly): {lt_100:,} ({100*lt_100/len(wages):.1f}%)")

# employer_id coverage
if 'employer_id' in df.columns:
    null_emp = df['employer_id'].isna().mean() * 100
    print(f"\n--- employer_id null rate: {null_emp:.1f}% ---")
    distinct_emp = df['employer_id'].nunique()
    print(f"  Distinct employers: {distinct_emp:,}")

# ── fact_oews ─────────────────────────────────────────────────────────────────
section("FACT_OEWS")
df = pd.read_parquet(TABLES / 'fact_oews.parquet')
print(f"Shape: {df.shape}")

# Check wage ordering (p10 < p25 < median < p75 < p90)
wage_cols = ['a_pct10', 'a_pct25', 'a_median', 'a_pct75', 'a_pct90']
existing = [c for c in wage_cols if c in df.columns]
if len(existing) == 5:
    violations = 0
    for _, row in df[existing].dropna().iterrows():
        if not (row[existing[0]] <= row[existing[1]] <= row[existing[2]] <= row[existing[3]] <= row[existing[4]]):
            violations += 1
    total_valid = len(df[existing].dropna())
    print(f"Wage ordering violations: {violations}/{total_valid} ({100*violations/max(1,total_valid):.2f}%)")

# ── fact_visa_applications ────────────────────────────────────────────────────
section("FACT_VISA_APPLICATIONS")
df = pd.read_parquet(TABLES / 'fact_visa_applications.parquet')
print(f"Shape: {df.shape}")
print(f"Columns: {sorted(df.columns.tolist())}")
if 'fiscal_year' in df.columns:
    print("\n--- FY distribution ---")
    fy = df['fiscal_year'].value_counts().sort_index()
    for y, n in fy.items():
        print(f"  FY{y}: {n:>6,}")
# Check issued/refused non-negative
for col in ['issued', 'refused']:
    if col in df.columns:
        neg = (pd.to_numeric(df[col], errors='coerce') < 0).sum()
        print(f"Negative {col}: {neg}")

# ── fact_iv_post ──────────────────────────────────────────────────────────────
section("FACT_IV_POST")
df = pd.read_parquet(TABLES / 'fact_iv_post.parquet')
print(f"Shape: {df.shape}")
if 'fiscal_year' in df.columns:
    print("\n--- FY distribution ---")
    fy = df['fiscal_year'].value_counts().sort_index()
    for y, n in fy.items():
        print(f"  FY{y}: {n:>6,}")
if 'post' in df.columns:
    print(f"\nDistinct posts: {df['post'].nunique()}")
# Check issuances non-negative
if 'issuances' in df.columns:
    neg = (df['issuances'] < 0).sum()
    zero = (df['issuances'] == 0).sum()
    print(f"Negative issuances: {neg}")
    print(f"Zero issuances: {zero} ({100*zero/len(df):.1f}%)")

# ── fact_niv_issuance ─────────────────────────────────────────────────────────
section("FACT_NIV_ISSUANCE")
df = pd.read_parquet(TABLES / 'fact_niv_issuance.parquet')
print(f"Shape: {df.shape}")
if 'fiscal_year' in df.columns:
    print("\n--- FY distribution ---")
    fy = df['fiscal_year'].value_counts().sort_index()
    for y, n in fy.items():
        print(f"  FY{y}: {n:>6,}")

# ── fact_h1b_employer_hub ────────────────────────────────────────────────────
section("FACT_H1B_EMPLOYER_HUB")
df = pd.read_parquet(TABLES / 'fact_h1b_employer_hub.parquet')
print(f"Shape: {df.shape}")
# Check is_stale flag
if 'is_stale' in df.columns:
    stale = df['is_stale'].sum()
    print(f"is_stale=True: {stale}/{len(df)} ({100*stale/len(df):.1f}%)")
if 'fiscal_year' in df.columns:
    df['fiscal_year'] = df['fiscal_year'].astype(int)
    print("\n--- FY distribution ---")
    fy = df['fiscal_year'].value_counts().sort_index()
    for y, n in fy.items():
        print(f"  FY{y}: {n:>6,}")

# ── Cross-table: employer_id referential integrity ────────────────────────────
section("CROSS-TABLE REFERENTIAL INTEGRITY")
dim_emp = pd.read_parquet(TABLES / 'dim_employer.parquet')
dim_emp_ids = set(dim_emp['employer_id'].dropna())
print(f"dim_employer: {len(dim_emp_ids):,} unique IDs")

for tbl in ['fact_perm', 'fact_lca']:
    try:
        df = pd.read_parquet(TABLES / tbl)
        if 'employer_id' in df.columns:
            fact_ids = set(df['employer_id'].dropna())
            matched = fact_ids & dim_emp_ids
            pct = 100 * len(matched) / max(1, len(fact_ids))
            print(f"  {tbl}: {len(fact_ids):,} distinct employer_ids, {pct:.1f}% in dim_employer")
    except Exception as e:
        print(f"  {tbl}: {e}")

# ── SOC referential integrity ────────────────────────────────────────────────
dim_soc = pd.read_parquet(TABLES / 'dim_soc.parquet')
dim_soc_codes = set(dim_soc['soc_code'].dropna())
print(f"\ndim_soc: {len(dim_soc_codes):,} unique codes")

for tbl in ['fact_perm', 'fact_lca', 'fact_oews.parquet']:
    try:
        if tbl.endswith('.parquet'):
            df = pd.read_parquet(TABLES / tbl)
        else:
            df = pd.read_parquet(TABLES / tbl)
        soc_col = 'soc_code' if 'soc_code' in df.columns else ('occ_code' if 'occ_code' in df.columns else None)
        if soc_col:
            fact_socs = set(df[soc_col].dropna().astype(str))
            matched = fact_socs & dim_soc_codes
            pct = 100 * len(matched) / max(1, len(fact_socs))
            print(f"  {tbl}: {len(fact_socs):,} distinct SOCs, {pct:.1f}% in dim_soc")
    except Exception as e:
        print(f"  {tbl}: {e}")

print("\n✓ Deep-dive audit complete")
