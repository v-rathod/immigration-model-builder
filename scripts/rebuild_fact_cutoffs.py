#!/usr/bin/env python3
"""
rebuild_fact_cutoffs.py — Delete and cleanly rebuild fact_cutoffs from PDF sources.

This script:
  1. Deletes artifacts/tables/fact_cutoffs/ entirely
  2. Re-runs the visa_bulletin_loader to produce one data.parquet per partition
  3. Reports final row/partition counts

Run from project root: python3 scripts/rebuild_fact_cutoffs.py
"""

import shutil
from pathlib import Path

import yaml

# Resolve paths from config
with open("configs/paths.yaml") as f:
    paths = yaml.safe_load(f)

DATA_ROOT = paths["data_root"]
ARTIFACTS_ROOT = paths["artifacts_root"]
SCHEMAS_PATH = "configs/schemas.yml"

fact_cutoffs_dir = Path(ARTIFACTS_ROOT) / "tables" / "fact_cutoffs"

print("=" * 60)
print("REBUILD FACT_CUTOFFS")
print("=" * 60)
print(f"  data_root:      {DATA_ROOT}")
print(f"  artifacts_root: {ARTIFACTS_ROOT}")

# Step 1: delete existing fact_cutoffs
if fact_cutoffs_dir.exists():
    print(f"\n  Deleting: {fact_cutoffs_dir}")
    shutil.rmtree(fact_cutoffs_dir)
    print("  Deleted.")
else:
    print(f"\n  {fact_cutoffs_dir} does not exist, nothing to delete")

# Step 2: re-run visa_bulletin_loader
print("\n  Running visa_bulletin_loader...")
from src.curate.visa_bulletin_loader import load_visa_bulletin

out = load_visa_bulletin(DATA_ROOT, ARTIFACTS_ROOT, SCHEMAS_PATH)

# Step 3: verify
print(f"\n  Done. Output: {out}")
from pathlib import Path as _P
import pandas as pd

files = list(_P(out).rglob("*.parquet")) if _P(out).is_dir() else [_P(out)]
total_rows = 0
for f in files:
    df = pd.read_parquet(f)
    total_rows += len(df)

print(f"\n  Partition files: {len(files)}")
print(f"  Total rows:      {total_rows:,}")
print("=" * 60)
