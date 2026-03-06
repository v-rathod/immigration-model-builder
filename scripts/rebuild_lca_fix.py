#!/usr/bin/env python3
"""Rebuild employer_features + employer_friendliness_scores after LCA fix."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import yaml

with open(ROOT / "configs" / "paths.yaml") as f:
    cfg = yaml.safe_load(f)

artifacts_root = Path(cfg.get("artifacts_root", str(ROOT / "artifacts")))
in_tables = artifacts_root / "tables"

print("=" * 60)
print("Step 1: Rebuild employer_features (with LCA fix)")
print("=" * 60)
from src.features.employer_features import build_employer_features
employer_out = in_tables / "employer_features.parquet"
build_employer_features(in_tables, employer_out)

# Verify the fix
import pandas as pd
df = pd.read_parquet(employer_out)
imo = df[df["employer_name"] == "Intelligent Medical Objects"]
if len(imo):
    row = imo.iloc[0]
    print(f"\n✓ IMO employer_features after fix:")
    print(f"  lca_filings_36m:      {row.get('lca_filings_36m')}")
    lca_ar = row.get('lca_approval_rate_36m')
    print(f"  lca_approval_rate_36m:{lca_ar:.4f if lca_ar is not None else 'N/A'}")
    print(f"  lca_certified_36m:    {row.get('lca_certified_36m', 'N/A')}")
    print(f"  lca_denied_36m:       {row.get('lca_denied_36m', 'N/A')}")
else:
    print("! IMO not found in employer_features")

print("\n" + "=" * 60)
print("Step 2: Rebuild employer_friendliness_scores (SRS)")
print("=" * 60)
from src.models.employer_score import fit_employer_score
fit_employer_score(in_tables, in_tables)

# Verify SRS
df_srs = pd.read_parquet(in_tables / "employer_friendliness_scores.parquet")
imo_srs = df_srs[df_srs["employer_name"] == "Intelligent Medical Objects"]
if len(imo_srs):
    row = imo_srs.iloc[0]
    print(f"\n✓ IMO employer_friendliness_scores after fix:")
    lca_ar = row.get('lca_approval_rate_36m')
    print(f"  lca_approval_rate_36m:{lca_ar:.4f if lca_ar is not None else 'N/A'}")
    print(f"  h1b_signal_subscore:  {row.get('h1b_signal_subscore'):.2f}")
    print(f"  efs:                  {row.get('efs')} | tier: {row.get('efs_tier')}")
else:
    print("! IMO not found in SRS")

print("\nDone. Run sync_p2_data.py next.")
