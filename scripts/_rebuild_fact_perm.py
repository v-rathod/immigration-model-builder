"""Targeted rebuild of fact_perm with updated column normalization and aliases."""
import sys
sys.path.insert(0, ".")

from pathlib import Path
from src.curate.build_fact_perm import build_fact_perm

data_root = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads")
artifacts_root = Path("artifacts")
output_path = artifacts_root / "tables" / "fact_perm"
layouts_path = Path("configs")

print("=" * 60)
print("TARGETED REBUILD: fact_perm (all partitions)")
print("=" * 60)

result_df = build_fact_perm(
    data_root=data_root,
    output_path=output_path,
    artifacts_path=artifacts_root,
    layouts_path=layouts_path,
    chunk_size=100000,
    dry_run=False,
)

if result_df is not None:
    print(f"\n{'=' * 60}")
    print(f"REBUILD COMPLETE: {len(result_df):,} total rows")
    print(f"{'=' * 60}")
