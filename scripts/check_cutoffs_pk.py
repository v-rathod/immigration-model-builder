#!/usr/bin/env python3
from pathlib import Path
import pyarrow.parquet as pq

cutoffs_path = Path('artifacts/tables/fact_cutoffs')

# Read all parquet files and check for PK duplicates
parquet_files = list(cutoffs_path.rglob('*.parquet'))

all_rows = []
for pf in parquet_files:
    tbl = pq.read_table(pf)
    df = tbl.to_pandas()
    
    # Restore partition columns
    parts = pf.parts
    for part in parts:
        if '=' in part:
            col_name, col_value = part.split('=', 1)
            if col_name not in df.columns:
                df[col_name] = int(col_value) if col_name.endswith('year') or col_name.endswith('month') else col_value
    
    all_rows.append(df)

import pandas as pd
df_all = pd.concat(all_rows, ignore_index=True)

print(f'Total rows: {len(df_all)}')

# Check PK uniqueness
pk_cols = ['bulletin_year', 'bulletin_month', 'chart', 'category', 'country']
if all(c in df_all.columns for c in pk_cols):
    # Find duplicates
    duplicates = df_all[df_all.duplicated(subset=pk_cols, keep=False)]
    
    if len(duplicates) > 0:
        print(f'\n⚠️  Found {len(duplicates)} rows with duplicate PKs')
        print(f'Unique PK combinations that have duplicates: {duplicates[pk_cols].drop_duplicates().shape[0]}')
        
        print(f'\nFirst 10 duplicate groups:')
        for i, (_, group) in enumerate(duplicates.groupby(pk_cols)):
            if i >= 10:
                break
            print(f'\nGroup {i+1}: {len(group)} rows')
            print(group[pk_cols + ['cutoff_date', 'status_flag', 'source_file']].to_string(index=False))
    else:
        print('\n✓ No duplicate PKs found')
else:
    print(f'\n✗ Missing PK columns: {[c for c in pk_cols if c not in df_all.columns]}')
    print(f'Available columns: {list(df_all.columns)}')
