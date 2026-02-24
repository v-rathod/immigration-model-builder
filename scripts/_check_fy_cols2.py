#!/usr/bin/env python3
"""Check area/employer columns in FY2022-2024."""
import pandas as pd
from pathlib import Path

DATA_ROOT = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads")
perm_base = DATA_ROOT / "PERM" / "PERM"

for fy in [2022, 2023, 2024]:
    fy_dir = perm_base / f"FY{fy}"
    files = sorted(fy_dir.glob("*.xlsx"))
    if not files:
        continue
    f = files[0]
    print(f"\n== FY{fy}: {f.name} ==")
    df = pd.read_excel(f, nrows=3)
    cols = set(df.columns)
    for key in ["AREA", "EMP_", "EMPLOYER", "WORK", "SITE", "CITY", "STATE", "POSTAL"]:
        matches = [c for c in cols if key.upper() in c.upper()]
        if matches:
            print(f"  {key}: {matches[:5]}")
    # Also check what columns worksite/employer look like
    for colname in ["EMPLOYER_NAME", "EMP_BUSINESS_NAME", "PERM_EMPLOYER_NAME", "EMPLOYER_BUS_NAME"]:
        if colname in cols:
            print(f"  {colname}: {df[colname].values[:2]}")
    for colname in ["PRIMARY_WORKSITE_BLS_AREA", "WORKSITE_BLS_AREA", "PERM_WORKSITE_METRO_MSA", "PERM_WORKSITE_STATE"]:
        if colname in cols:
            print(f"  {colname}: {df[colname].values[:2]}")
    for colname in ["WAGE_OFFER_FROM", "WAGE_OFFER_UNIT_OF_PAY"]:
        if colname in cols:
            print(f"  {colname}: {df[colname].values[:3]}")
