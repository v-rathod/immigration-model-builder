#!/usr/bin/env python3
"""Diagnose dim_employer coverage gap."""
import pandas as pd
import pathlib

TABLES = pathlib.Path("artifacts/tables")

# 1. Count unique employer_ids in fact_perm
fact_perm_dir = TABLES / "fact_perm"
perm_ids = set()
n_files = 0
for pf in sorted(fact_perm_dir.rglob("*.parquet")):
    df = pd.read_parquet(pf, columns=["employer_id"])
    perm_ids.update(df["employer_id"].dropna().unique())
    n_files += 1
print(f"fact_perm: {n_files} parquet files, {len(perm_ids):,} unique employer_ids")

# 2. employer_features
df_ef = pd.read_parquet(TABLES / "employer_features.parquet")
ef_ids = set(df_ef["employer_id"].dropna().unique())
print(f"employer_features: {len(df_ef):,} rows, {len(ef_ids):,} unique employer_ids")

# 3. dim_employer
df_dim = pd.read_parquet(TABLES / "dim_employer.parquet")
dim_ids = set(df_dim["employer_id"].dropna())
print(f"dim_employer: {len(df_dim):,} rows")

# Overlap
print(f"fact_perm employer_ids IN dim_employer: {len(perm_ids & dim_ids):,}/{len(perm_ids):,} = {len(perm_ids & dim_ids)/max(len(perm_ids),1)*100:.1f}%")
print(f"employer_features employer_ids IN dim_employer: {len(ef_ids & dim_ids):,}/{len(ef_ids):,} = {len(ef_ids & dim_ids)/max(len(ef_ids),1)*100:.1f}%")

# 4. Could we expand dim_employer from fact_perm?
missing = perm_ids - dim_ids
print(f"employer_ids in fact_perm but NOT in dim_employer: {len(missing):,}")
print(f"If we added those, dim_employer would have: {len(df_dim) + len(missing):,} rows")
