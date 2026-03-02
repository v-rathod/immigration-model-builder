#!/usr/bin/env python3
"""Complete inventory of ALL parquet artifacts — identify what's been audited vs missed."""
import pandas as pd
from pathlib import Path

TABLES = Path("artifacts/tables")

all_artifacts = []

# Flat parquet files
for f in sorted(TABLES.glob("*.parquet")):
    try:
        df = pd.read_parquet(f)
        n, cols = len(df), len(df.columns)
        colnames = sorted(df.columns.tolist())
    except Exception as e:
        n, cols, colnames = -1, -1, [str(e)]
    all_artifacts.append((f.name, n, cols, "flat", colnames))

# Partitioned directories
for d in sorted(TABLES.iterdir()):
    if d.is_dir() and not d.name.startswith("_") and not d.name.startswith("."):
        parts = list(d.rglob("*.parquet"))
        if parts:
            try:
                df = pd.read_parquet(d)
                n, cols = len(df), len(df.columns)
                colnames = sorted(df.columns.tolist())
            except Exception as e:
                n, cols, colnames = -1, -1, [str(e)]
            all_artifacts.append((f"{d.name}/", n, cols, "partitioned", colnames))

print(f"Total artifacts: {len(all_artifacts)}\n")
print(f"{'#':<4} {'Artifact':<45} {'Rows':>12} {'Cols':>6} {'Type':<12}")
print("-" * 82)
for i, (name, rows, cols, typ, _) in enumerate(sorted(all_artifacts), 1):
    print(f"{i:<4} {name:<45} {rows:>12,} {cols:>6} {typ:<12}")

# Now classify what was audited
AUDITED = {
    "fact_perm", "fact_lca", "fact_oews.parquet", "fact_cutoffs",
    "fact_cutoffs_all.parquet", "fact_cutoff_trends.parquet",
    "fact_visa_applications.parquet", "fact_iv_post.parquet",
    "fact_niv_issuance.parquet", "fact_visa_issuance.parquet",
    "fact_h1b_employer_hub.parquet", "fact_bls_ces.parquet",
    "fact_dhs_admissions.parquet", "fact_uscis_approvals.parquet",
    "fact_warn_events.parquet",
    "dim_employer.parquet", "dim_soc.parquet", "dim_country.parquet",
    "dim_area.parquet", "dim_visa_ceiling.parquet", "dim_visa_class.parquet",
    "employer_features.parquet", "employer_friendliness_scores.parquet",
    "salary_benchmarks.parquet", "soc_demand_metrics.parquet",
    "visa_demand_metrics.parquet", "worksite_geo_metrics.parquet",
    "pd_forecasts.parquet", "queue_depth_estimates.parquet",
}

# Match dir names
def artifact_key(name):
    return name.rstrip("/")

print("\n\n=== AUDIT COVERAGE ===")
print(f"\n{'Status':<12} {'Artifact':<45} {'Rows':>12}")
print("-" * 72)
not_audited = []
for name, rows, cols, typ, colnames in sorted(all_artifacts):
    key = artifact_key(name)
    if key in AUDITED or name in AUDITED:
        status = "AUDITED"
    else:
        status = "NOT AUDITED"
        not_audited.append((name, rows, cols, colnames))
    print(f"{status:<12} {name:<45} {rows:>12,}")

if not_audited:
    print(f"\n\n=== {len(not_audited)} ARTIFACTS NOT YET AUDITED ===")
    for name, rows, cols, colnames in not_audited:
        print(f"\n  {name} ({rows:,} rows, {cols} cols)")
        print(f"  Columns: {colnames}")
