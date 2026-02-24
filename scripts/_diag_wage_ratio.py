#!/usr/bin/env python3
"""Diagnose why wage_ratio_med is only 16.1% coverage."""
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone

TABLES = Path("artifacts/tables")

# Load recent fact_perm (last 36m)
perm_dir = TABLES / "fact_perm"
dfs = []
for fy in [2022, 2023, 2024, 2025, 2026]:
    pf = perm_dir / f"fiscal_year={fy}" / "part-0.parquet"
    if pf.exists():
        df = pd.read_parquet(pf)
        df["fiscal_year"] = fy
        dfs.append(df)
perm = pd.concat(dfs, ignore_index=True)
perm["decision_date"] = pd.to_datetime(perm["decision_date"], errors="coerce")
perm = perm.dropna(subset=["decision_date", "employer_id"])
anchor = perm["decision_date"].max()
start_24m = anchor - pd.DateOffset(months=24)
perm_24 = perm[perm["decision_date"] > start_24m].copy()

print(f"anchor: {anchor.date()}")
print(f"perm_24 rows: {len(perm_24):,}")

# Annual wage
WAGE_UNIT_MULTIPLIER = {'Year': 1.0, 'year': 1.0, 'yr': 1.0, 'Hour': 2080.0, 'hour': 2080.0, 'hr': 2080.0, 'Week': 52.0, 'Month': 12.0, 'Bi-Weekly': 26.0}
perm_24 = perm_24.copy()
perm_24["wage_offer_from"] = pd.to_numeric(perm_24["wage_offer_from"], errors="coerce")
perm_24["wage_mult"] = perm_24["wage_offer_unit"].map(WAGE_UNIT_MULTIPLIER).fillna(1.0)
perm_24["annual_wage"] = perm_24["wage_offer_from"] * perm_24["wage_mult"]

print(f"\nwage_offer_from non-null: {perm_24['wage_offer_from'].notna().sum():,} ({100*perm_24['wage_offer_from'].notna().mean():.1f}%)")
print(f"annual_wage non-null: {perm_24['annual_wage'].notna().sum():,} ({100*perm_24['annual_wage'].notna().mean():.1f}%)")
print(f"soc_code non-null: {perm_24['soc_code'].notna().sum():,} ({100*perm_24['soc_code'].notna().mean():.1f}%)")

# Employer analysis
emp_grp = perm_24.groupby("employer_id").agg(
    n=("case_number", "count"),
    n_wage=("annual_wage", lambda x: x.notna().sum()),
    n_soc=("soc_code", lambda x: x.notna().sum()),
).reset_index()

print(f"\nTotal employers in 24m window: {len(emp_grp):,}")
print(f"Employers with >=1 wage: {(emp_grp['n_wage']>=1).sum():,} ({100*(emp_grp['n_wage']>=1).mean():.1f}%)")
print(f"Employers with >=3 wage: {(emp_grp['n_wage']>=3).sum():,} ({100*(emp_grp['n_wage']>=3).mean():.1f}%)")

# OEWS matching
oews = pd.read_parquet(TABLES / "fact_oews/ref_year=2024/data.parquet", columns=["soc_code", "area_code", "a_median"])
oews = oews.dropna(subset=["a_median"])
nat_mask = oews["area_code"].astype(str) == "99"
oews_nat_socs = set(oews[nat_mask]["soc_code"].dropna().astype(str).unique())
oews_area_socs = set(oews[~nat_mask]["soc_code"].dropna().astype(str).unique())

# For each employer-24m, would they get an OEWS match?
perm_24_soc = perm_24["soc_code"].astype(str).str[:7]
n_matchable = perm_24_soc[perm_24_soc.isin(oews_nat_socs)].nunique()
n_unique_soc = perm_24_soc[perm_24_soc != "nan"].nunique()
print(f"\nOEWS national SOC codes: {len(oews_nat_socs):,}")
print(f"Unique SOC codes in perm_24: {n_unique_soc:,}")
print(f"Matchable SOC codes (in OEWS national): {n_matchable:,} / {n_unique_soc:,} ({100*n_matchable/max(n_unique_soc,1):.1f}%)")

# If threshold lowered to 1 and OEWS match required
emp_with_wage_and_soc = perm_24[(perm_24["annual_wage"].notna()) & (perm_24["soc_code"].notna())].groupby("employer_id")["case_number"].count()
can_compute = (emp_with_wage_and_soc >= 1).sum()
print(f"\nEmployers that COULD get wage_ratio (>=1 row with wage+SOC): {can_compute:,} / {len(emp_grp):,} ({100*can_compute/len(emp_grp):.1f}%)")
