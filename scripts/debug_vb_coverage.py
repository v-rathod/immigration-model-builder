#!/usr/bin/env python3
from pathlib import Path
import pyarrow.parquet as pq

# Get expected files
data_root = Path('/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads')
vb_base = data_root / "Visa_Bulletin"
expected_files = set()
for file in vb_base.rglob('*.pdf'):
    rel_path = file.relative_to(data_root)
    expected_files.add(str(rel_path))

# Get processed files from fact_cutoffs
cutoffs_path = Path('artifacts/tables/fact_cutoffs')
processed_files = set()
for pf in cutoffs_path.rglob('*.parquet'):
    tbl = pq.read_table(pf)
    if 'source_file' in tbl.column_names:
        vals = tbl.column('source_file').to_pylist()
        processed_files.update(vals)

print(f'Expected files: {len(expected_files)}')
print(f'Processed files: {len(processed_files)}')
print(f'Exact matches: {len(expected_files & processed_files)}')
print(f'Coverage: {len(expected_files & processed_files) / len(expected_files) * 100:.1f}%')

print(f'\nMissing from processed (first 20):')
missing = sorted(list(expected_files - processed_files))[:20]
for m in missing:
    print(f'  {m}')

print(f'\nStale in processed (not in expected, first 10):')
stale = sorted(list(processed_files - expected_files))[:10]
for s in stale:
    print(f'  {s}')

# Check case-insensitive matches
expected_lower = {f.lower(): f for f in expected_files}
processed_lower = {f.lower(): f for f in processed_files}
case_insensitive_matches = set(expected_lower.keys()) & set(processed_lower.keys())
print(f'\nCase-insensitive matches: {len(case_insensitive_matches)} ({len(case_insensitive_matches) / len(expected_files) * 100:.1f}%)')

# Show examples of case mismatches
case_mismatches = []
for lower_key in case_insensitive_matches:
    if expected_lower[lower_key] != processed_lower.get(lower_key):
        case_mismatches.append((expected_lower[lower_key], processed_lower.get(lower_key, '???')))

if case_mismatches:
    print(f'\nCase mismatches found ({len(case_mismatches)}):')
    for exp, proc in case_mismatches[:5]:
        print(f'  Expected: {exp}')
        print(f'  Processed: {proc}')
        print()
