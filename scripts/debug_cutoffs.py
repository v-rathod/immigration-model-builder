#!/usr/bin/env python3
import sys
sys.path.insert(0, 'src')
import pyarrow.parquet as pq
from pathlib import Path
import pandas as pd

cutoffs_path = Path('artifacts/tables/fact_cutoffs')
parquet_files = list(cutoffs_path.rglob('*.parquet'))
print(f'Found {len(parquet_files)} parquet files')

# Read all files and collect source_file values
all_source_files = set()
total_rows = 0
duplicates_found = []

for pf in parquet_files:
    df = pq.read_table(pf).to_pandas()
    total_rows += len(df)
    
    if 'source_file' in df.columns:
        all_source_files.update(df['source_file'].unique())
    
    # Check PK uniqueness
    pk_cols = ['chart', 'category', 'country', 'cutoff_date']
    if all(c in df.columns for c in pk_cols):
        dups = df.duplicated(subset=pk_cols, keep=False)
        if dups.any():
            duplicates_found.append((pf.name, dups.sum()))

print(f'\nTotal rows: {total_rows}')
print(f'Unique source_file values: {len(all_source_files)}')
print(f'First 5 source files: {sorted(list(all_source_files))[:5]}')

if duplicates_found:
    print(f'\n⚠️  Duplicate PKs found in {len(duplicates_found)} partition files:')
    for fname, count in duplicates_found[:5]:
        print(f'  {fname}: {count} duplicate rows')
else:
    print('\n✓ No duplicate PKs found')
