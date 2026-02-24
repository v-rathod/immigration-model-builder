#!/usr/bin/env python3
"""Rebuild dim_country and dim_soc with updated builders."""
import sys
sys.path.insert(0, '/Users/vrathod1/dev/NorthStar/immigration-model-builder')

import yaml
from pathlib import Path

with open("configs/paths.yaml") as f:
    paths = yaml.safe_load(f)

DATA_ROOT = paths["data_root"]
ARTIFACTS_ROOT = paths.get("artifacts_root", "./artifacts")
SCHEMAS_PATH = "configs/schemas.yml"

print("=" * 60)
print("REBUILD DIMS")
print("=" * 60)

# dim_country
from src.curate.build_dim_country import build_dim_country
dim_country_path = str(Path(ARTIFACTS_ROOT) / "tables" / "dim_country.parquet")
print("\n[1/2] dim_country")
result = build_dim_country(DATA_ROOT, dim_country_path, SCHEMAS_PATH)
import pandas as pd
df = pd.read_parquet(result)
print(f"  ✓ dim_country: {len(df)} rows")

# dim_soc
from src.curate.build_dim_soc import build_dim_soc
dim_soc_path = str(Path(ARTIFACTS_ROOT) / "tables" / "dim_soc.parquet")
print("\n[2/2] dim_soc")
result2 = build_dim_soc(DATA_ROOT, dim_soc_path, SCHEMAS_PATH)
df2 = pd.read_parquet(result2)
print(f"  ✓ dim_soc: {len(df2)} rows")

print("\n" + "=" * 60)
print("DONE")
print("=" * 60)
