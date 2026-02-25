"""Scan raw LCA Excel/CSV files for is_fulltime/job_title/naics column name variants."""
import pandas as pd
from pathlib import Path

data_root = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads/LCA")

targets = ["FULL", "TIME", "JOB", "TITLE", "NAICS", "NAIC"]

for fy_dir in sorted(data_root.iterdir()):
    if not fy_dir.is_dir() or not fy_dir.name.startswith("FY"):
        continue
    for f in sorted(fy_dir.iterdir()):
        if f.suffix.lower() not in ('.xlsx', '.xls', '.csv'):
            continue
        # Skip supplemental/worksite files
        if any(x in f.name.lower() for x in ['worksite', 'appendix']):
            continue
        try:
            if f.suffix.lower() == '.csv':
                df = pd.read_csv(f, nrows=0)
            else:
                df = pd.read_excel(f, nrows=0)
            matched = [c for c in df.columns if any(t in c.upper() for t in targets)]
            if matched:
                print(f"\n=== {fy_dir.name}/{f.name} ({len(df.columns)} cols) ===")
                for m in sorted(matched):
                    print(f"  {m}")
        except Exception as e:
            print(f"  ERROR {fy_dir.name}/{f.name}: {e}")
