#!/usr/bin/env python3
"""
Patch fact_perm: rebuild recent FY partitions (FY2022+) with fixed SOC/area/wage mappings.

Root causes fixed in build_fact_perm.py:
  1. SOC: strip '.00' decimal suffix (PWD_SOC_CODE='17-2112.00' → '17-2112')
  2. Area: match PRIMARY_WORKSITE_BLS_AREA name string → area_code via dim_area.area_title
  3. Wage: JOB_OPP_WAGE_FROM was already mapped correctly (was actually working)

This script runs build_fact_perm for FY >= MIN_FY only and overwrites those partitions.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.curate.build_fact_perm import build_fact_perm, find_perm_files
import yaml

MIN_FY = 2022  # Rebuild last 4 FYs (covers 36m window for employer_features)

def main():
    # Load paths config
    with open(ROOT / "configs" / "paths.yaml") as f:
        cfg = yaml.safe_load(f)

    data_root = Path(cfg.get("data_root", "/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads"))
    artifacts_root = Path(cfg.get("artifacts_root", str(ROOT / "artifacts")))
    output_path = artifacts_root / "tables" / "fact_perm"
    layouts_path = ROOT / "configs"

    print(f"Patching fact_perm FY>={MIN_FY}")
    print(f"Data root: {data_root}")
    print(f"Output path: {output_path}")

    # Check which files are available
    files = find_perm_files(data_root, min_fy=MIN_FY)
    if not files:
        print(f"No PERM files found for FY>={MIN_FY}")
        sys.exit(1)
    print(f"Found {len(files)} PERM file(s) for FY>={MIN_FY}")
    for fy, fpath in files:
        size_mb = fpath.stat().st_size / (1024*1024)
        print(f"  FY{fy}: {fpath.name} ({size_mb:.1f} MB)")

    build_fact_perm(
        data_root=data_root,
        output_path=output_path,
        artifacts_path=artifacts_root,
        layouts_path=layouts_path,
        chunk_size=100_000,
        dry_run=False,
        min_fy=MIN_FY,
    )
    print("\nDone — fact_perm recent partitions rebuilt")


if __name__ == "__main__":
    main()
