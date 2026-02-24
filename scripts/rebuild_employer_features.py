#!/usr/bin/env python3
"""Targeted rebuild of employer_features only (skip salary_benchmarks)."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import yaml
from src.features.employer_features import build_employer_features

with open(ROOT / "configs" / "paths.yaml") as f:
    cfg = yaml.safe_load(f)

artifacts_root = Path(cfg.get("artifacts_root", str(ROOT / "artifacts")))
in_tables = artifacts_root / "tables"
employer_out = artifacts_root / "tables" / "employer_features.parquet"

print("Rebuilding employer_features...")
build_employer_features(in_tables, employer_out)
print("Done.")
