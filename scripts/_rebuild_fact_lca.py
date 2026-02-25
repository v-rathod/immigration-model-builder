"""Targeted rebuild of fact_lca with updated aliases for is_fulltime/job_title/naics_code."""
import sys
sys.path.insert(0, ".")

from src.curate.lca_loader import load_lca

data_root = "/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads"
artifacts_root = "./artifacts"
schemas_path = "configs/schemas.yml"

print("=" * 60)
print("TARGETED REBUILD: fact_lca (all partitions)")
print("=" * 60)

load_lca(data_root, artifacts_root, schemas_path, dry_run=False)

print(f"\n{'=' * 60}")
print("REBUILD COMPLETE")
print(f"{'=' * 60}")
