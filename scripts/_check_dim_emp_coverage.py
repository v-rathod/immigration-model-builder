#!/usr/bin/env python3
"""Check dim_employer coverage vs fact_perm and employer_features."""
import pandas as pd
import pathlib

TABLES = pathlib.Path("artifacts/tables")

# dim_employer
dim = pd.read_parquet(TABLES / "dim_employer.parquet")
print(f"dim_employer: {len(dim):,} rows, {dim['employer_id'].nunique():,} unique IDs")
print(f"  source_files sample: {dim['source_files'].value_counts().head(5).to_dict()}")

# fact_perm unique employer_ids
perm_dir = TABLES / "fact_perm"
perm = pd.read_parquet(perm_dir, columns=["employer_id", "employer_name"])
print(f"\nfact_perm: {len(perm):,} total rows")
uniq = perm["employer_id"].nunique()
print(f"fact_perm unique employer_ids: {uniq:,}")

# employer_features
feat = pd.read_parquet(TABLES / "employer_features.parquet", columns=["employer_id"])
print(f"\nemployer_features: {len(feat):,} rows, {feat['employer_id'].nunique():,} unique IDs")

# Coverage
dim_ids = set(dim["employer_id"].dropna())
perm_ids = set(perm["employer_id"].dropna().unique())
feat_ids = set(feat["employer_id"].dropna().unique())
print(f"\nCoverage:")
print(f"  dim covers {len(dim_ids & perm_ids):,} / {len(perm_ids):,} fact_perm employers = {len(dim_ids & perm_ids)/len(perm_ids)*100:.1f}%")
print(f"  dim covers {len(dim_ids & feat_ids):,} / {len(feat_ids):,} feature employers = {len(dim_ids & feat_ids)/len(feat_ids)*100:.1f}%")
print(f"  fact_perm IDs NOT in dim: {len(perm_ids - dim_ids):,}")
print(f"  feature IDs NOT in dim: {len(feat_ids - dim_ids):,}")
