"""Scan raw PERM Excel files and print column names relevant to our col_map."""
import pandas as pd
from pathlib import Path

data_root = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads/PERM/PERM")

targets = [
    "CASE", "SOC", "EMPLOYER", "WAGE", "WORK", "CITY", "STATE",
    "COUNTRY", "CITIZEN", "JOB", "FULL", "RECEIV", "DECISION",
    "STATUS", "NAICS", "EMP_BUS", "PRIMARY",
]

for fy_dir in sorted(data_root.iterdir()):
    if not fy_dir.is_dir() or not fy_dir.name.startswith("FY"):
        continue
    for f in sorted(fy_dir.glob("PERM_*.xlsx")):
        try:
            df = pd.read_excel(f, nrows=0)
            matched = [c for c in df.columns if any(t in c.upper() for t in targets)]
            print(f"\n=== {fy_dir.name}/{f.name} ({len(df.columns)} cols) ===")
            for m in sorted(matched):
                norm = m.strip().upper().replace(" ", "_")
                if norm != m:
                    print(f"  {m!r}  ->  {norm}")
                else:
                    print(f"  {m}")
        except Exception as e:
            print(f"  ERROR: {e}")
