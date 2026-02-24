"""Quick audit of all parquet row counts and partition structure."""
import sys
import json
import hashlib
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts/tables"

items = [
    ("fact_cutoffs_all", TABLES / "fact_cutoffs_all.parquet"),
    ("fact_perm", TABLES / "fact_perm"),
    ("fact_perm_unique_case", TABLES / "fact_perm_unique_case"),
    ("fact_lca", TABLES / "fact_lca"),
    ("fact_oews", TABLES / "fact_oews"),
    ("dim_employer", TABLES / "dim_employer.parquet"),
    ("dim_soc", TABLES / "dim_soc.parquet"),
    ("dim_area", TABLES / "dim_area.parquet"),
    ("dim_country", TABLES / "dim_country.parquet"),
    ("dim_visa_class", TABLES / "dim_visa_class.parquet"),
    ("employer_features", TABLES / "employer_features.parquet"),
    ("employer_friendliness_scores", TABLES / "employer_friendliness_scores.parquet"),
    ("fact_cutoff_trends", TABLES / "fact_cutoff_trends.parquet"),
    ("category_movement_metrics", TABLES / "category_movement_metrics.parquet"),
    ("employer_monthly_metrics", TABLES / "employer_monthly_metrics.parquet"),
    ("worksite_geo_metrics", TABLES / "worksite_geo_metrics.parquet"),
    ("salary_benchmarks", TABLES / "salary_benchmarks.parquet"),
    ("soc_demand_metrics", TABLES / "soc_demand_metrics.parquet"),
    ("processing_times_trends", TABLES / "processing_times_trends.parquet"),
    ("backlog_estimates", TABLES / "backlog_estimates.parquet"),
]

print(f"{'Table':<35} {'Rows':>12} {'Cols':>6} {'Note'}")
print("-" * 70)
for name, path in items:
    if not path.exists():
        print(f"{name:<35} {'MISSING':>12}")
        continue
    if path.is_dir():
        # partitioned
        files = sorted(path.rglob("*.parquet"))
        total = 0
        for f in files:
            try:
                total += pq.read_metadata(f).num_rows
            except Exception:
                pass
        print(f"{name:<35} {total:>12,}        (partitioned, {len(files)} files)")
    else:
        try:
            meta = pq.read_metadata(path)
            print(f"{name:<35} {meta.num_rows:>12,} {meta.num_columns:>6}")
        except Exception as e:
            print(f"{name:<35} ERROR: {e}")
