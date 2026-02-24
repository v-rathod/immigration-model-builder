#!/usr/bin/env python3
"""Check recent PERM Excel column names vs what build_fact_perm expects."""
import pandas as pd
from pathlib import Path

DATA_ROOT = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads")
perm_base = DATA_ROOT / "PERM" / "PERM"

expected = {
    'soc_code': 'PWD_SOC_CODE',
    'wage_from': 'JOB_OPP_WAGE_FROM',
    'wage_unit': 'JOB_OPP_WAGE_PER',
    'worksite_area': 'PRIMARY_WORKSITE_BLS_AREA',
    'employer_name': 'EMP_BUSINESS_NAME',
    'case_status': 'CASE_STATUS',
}

# Check the most recent FY file
for fy_dir in sorted(perm_base.iterdir(), reverse=True):
    if not fy_dir.name.startswith("FY"):
        continue
    files = list(fy_dir.glob("*.xlsx"))
    if not files:
        continue
    f = files[0]
    try:
        df = pd.read_excel(f, nrows=3)
        actual_cols = set(df.columns)
        print(f"\n== {f.name} (first 3 rows) ==")
        print(f"All cols: {sorted(actual_cols)}")
        for key, expected_col in expected.items():
            found = expected_col in actual_cols
            alternatives = [c for c in actual_cols if key.upper().replace('_','') in c.upper().replace('_','')]
            print(f"  {key}: expected='{expected_col}' → found={found}  alternatives={alternatives[:3]}")
        # Check non-null counts for expected key cols
        for key, expected_col in expected.items():
            if expected_col in df.columns:
                print(f"    sample {expected_col}: {df[expected_col].values[:3]}")
    except Exception as e:
        print(f"Error: {e}")
    break  # Only check most recent file
