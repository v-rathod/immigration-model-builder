#!/usr/bin/env python3
"""Check what raw PERM data files look like and what columns they have."""
import os
from pathlib import Path
import pandas as pd

DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads"))
perm_dir = DATA_ROOT / "PERM"

if not perm_dir.exists():
    print(f"PERM dir not found: {perm_dir}")
    # Try alternate locations
    for alt in DATA_ROOT.rglob("*PERM*"):
        print(f"Found: {alt}")
else:
    files = sorted(perm_dir.rglob("*.xlsx")) + sorted(perm_dir.rglob("*.csv"))
    print(f"PERM files: {len(files)}")
    for f in files[:3]:
        print(f"\nFile: {f.name}")
        try:
            if f.suffix == ".xlsx":
                df = pd.read_excel(f, nrows=5)
            else:
                df = pd.read_csv(f, nrows=5)
            print(f"  Cols ({len(df.columns)}): {list(df.columns)[:20]}")
        except Exception as e:
            print(f"  Error: {e}")
