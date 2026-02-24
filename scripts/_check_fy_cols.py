#!/usr/bin/env python3
"""Show column names for FY2022-2024 PERM files to find wage/SOC/area columns."""
import pandas as pd
from pathlib import Path

DATA_ROOT = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads")
perm_base = DATA_ROOT / "PERM" / "PERM"

for fy in [2022, 2023, 2024]:
    fy_dir = perm_base / f"FY{fy}"
    if not fy_dir.exists():
        print(f"FY{fy}: dir not found")
        continue
    files = list(fy_dir.glob("*.xlsx"))
    if not files:
        print(f"FY{fy}: no xlsx files")
        continue
    f = sorted(files)[0]
    print(f"\n== FY{fy}: {f.name} ==")
    try:
        df = pd.read_excel(f, nrows=3)
        cols = set(df.columns)
        for key in ["SOC", "WAGE", "AREA", "BLS", "JOB_OPP", "PWD"]:
            matches = [c for c in cols if key.upper() in c.upper()]
            if matches:
                print(f"  {key}-related: {matches}")
        # Show sample data for key wage/SOC/area
        for pattern in ["SOC", "WAGE_OFFER", "WAGE_FROM", "BLS_AREA", "PERM_WORKSITE"]:
            match = [c for c in cols if pattern.upper() in c.upper()]
            for m in match[:2]:
                print(f"  {m}: {df[m].values[:3]}")
    except Exception as e:
        print(f"  Error: {e}")
