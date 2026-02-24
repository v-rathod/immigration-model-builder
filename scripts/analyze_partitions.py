#!/usr/bin/env python3
from pathlib import Path
import pyarrow.parquet as pq

cutoffs_path = Path('artifacts/tables/fact_cutoffs')

partition_info = []
for pf in sorted(cutoffs_path.rglob('*.parquet')):
    tbl = pq.read_table(pf)
    num_rows = len(tbl)
    unique_sources = set(tbl.column('source_file').to_pylist()) if 'source_file' in tbl.column_names else set()
    
    # Extract year/month from path
    parts = pf.parts
    year_part = next((p for p in parts if p.startswith('bulletin_year=')), '?')
    month_part = next((p for p in parts if p.startswith('bulletin_month=')), '?')
    
    partition_info.append({
        'partition': f'{year_part}/{month_part}',
        'rows': num_rows,
        'unique_sources': len(unique_sources),
        'sources': unique_sources
    })

print(f'Total partitions: {len(partition_info)}')
print(f'\nPartitions with >2 rows (expected max is ~48-96 for 2 charts × 24 categories):')
large_partitions = [p for p in partition_info if p['rows'] > 96]
for p in large_partitions[:10]:
    print(f"  {p['partition']}: {p['rows']} rows, {p['unique_sources']} source(s)")

print(f'\nPartitions with multiple unique source_file values:')
multi_source = [p for p in partition_info if p['unique_sources'] > 1]
for p in multi_source[:10]:
    print(f"  {p['partition']}: {p['unique_sources']} sources - {p['sources']}")

print(f'\nTotal unique source files across all partitions: {len(set.union(*(p["sources"] for p in partition_info if p["sources"])))}')
