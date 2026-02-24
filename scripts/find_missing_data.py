#!/usr/bin/env python3
from pathlib import Path
import pyarrow.parquet as pq
import re

data_root = Path('/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads')
vb_base = data_root / "Visa_Bulletin"

# Get all PDF files
all_pdfs = sorted(vb_base.rglob('*.pdf'))
print(f'Total PDF files in source: {len(all_pdfs)}')

# Parse filename function (copied from visa_bulletin_loader.py logic)
def parse_filename(filename):
    """Try to extract year and month from filename."""
    # Pattern: visabulletin_MonthYYYY.pdf or VisaBulletin_MonthYYYY.pdf
    pattern = r'visabulletin[_-](\w+)(\d{4})\.pdf'
    match = re.search(pattern, filename.lower())
    
    if match:
        month_str, year_str = match.groups()
        
        # Map month names to numbers
        month_map = {
            'january': 1, 'february': 2, 'march': 3, 'april': 4,
            'may': 5, 'june': 6, 'july': 7, 'august': 8,
            'september': 9, 'october': 10, 'november': 11, 'december': 12
        }
        
        month_num = month_map.get(month_str.lower())
        if month_num:
            year = int(year_str)
            return year, month_num
    
    return None, None

# Categorize PDFs
parseable_pdfs = []
unparseable_pdfs = []

for pdf in all_pdfs:
    year, month = parse_filename(pdf.name)
    if year and month:
        rel_path = pdf.relative_to(data_root)
        parseable_pdfs.append(str(rel_path))
    else:
        unparseable_pdfs.append(pdf.name)

print(f'Parseable PDFs (date extractable): {len(parseable_pdfs)}')
print(f'Unparseable PDFs (date not extractable): {len(unparseable_pdfs)}')

if unparseable_pdfs:
    print(f'\nUnparseable filenames:')
    for name in sorted(unparseable_pdfs):
        print(f'  {name}')

# Get processed source_file values
cutoffs_path = Path('artifacts/tables/fact_cutoffs')
processed_files = set()
for pf in cutoffs_path.rglob('*.parquet'):
    tbl = pq.read_table(pf)
    if 'source_file' in tbl.column_names:
        vals = tbl.column('source_file').to_pylist()
        processed_files.update(vals)

print(f'\nProcessed files (in fact_cutoffs): {len(processed_files)}')

# Find parseable PDFs that didn't produce data
parseable_set = set(parseable_pdfs)
missing_data = parseable_set - processed_files

print(f'\nParseable but no data extracted: {len(missing_data)}')
if missing_data:
    print(f'\nFiles that were parseable but produced no data:')
    for f in sorted(list(missing_data))[:20]:
        print(f'  {f}')
