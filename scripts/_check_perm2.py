#!/usr/bin/env python3
"""Check fact_perm column availability."""
import pandas as pd
from pathlib import Path

perm_dir = Path("artifacts/tables/fact_perm")
files = sorted(perm_dir.rglob("*.parquet"))[:3]
for f in files:
    df = pd.read_parquet(f)
    print(f"File: {f.name}")
    print(f"  Cols: {list(df.columns)}")
    print(f"  Rows: {len(df)}")
    # Check wage/soc/area
    for c in ["area_code", "wage_offer_from", "wage_offer_unit", "soc_code"]:
        if c in df.columns:
            print(f"  {c} null%: {round(100*df[c].isna().mean(),1)}%  sample: {df[c].dropna().unique()[:3]}")
    print()
