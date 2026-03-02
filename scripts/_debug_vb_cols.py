"""Debug: check column counts across many visa bulletin PDFs to find when 6-col format started."""
import pdfplumber
from pathlib import Path
import re

data_root = '/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads'
vb_dir = Path(data_root) / 'Visa_Bulletin'

MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}

results = []

for pdf_file in sorted(vb_dir.rglob('*.pdf')):
    match = re.search(r'visabulletin[_-]?(\w+?)(\d{4})', pdf_file.name, re.IGNORECASE)
    if not match:
        continue
    month_str = match.group(1).lower()
    year = int(match.group(2))
    month = MONTH_MAP.get(month_str)
    if not month:
        continue

    try:
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue
                if 'FINAL ACTION DATES FOR EMPLOYMENT-BASED' in text:
                    lines = text.split('\n')
                    for line in lines:
                        if line.strip().startswith('2nd'):
                            parts = line.strip().split()
                            has_el_salvador = 'EL SALVADOR' in text or 'EL SAL' in text
                            # Get EB2 values
                            vals = parts[1:]
                            n_cols = len(vals)
                            results.append({
                                'year': year, 'month': month,
                                'n_cols': n_cols,
                                'has_el_salvador': has_el_salvador,
                                'raw_line': line.strip(),
                                'vals': vals,
                            })
                            break
    except Exception as e:
        print(f'ERROR {pdf_file.name}: {e}')

results.sort(key=lambda r: (r['year'], r['month']))
print(f"{'Bulletin':>12}  Cols  EL_SAL  EB2 Raw Line")
print("-" * 100)
for r in results:
    label = f"{r['month']:02d}/{r['year']}"
    el = 'YES' if r['has_el_salvador'] else 'no'
    print(f"{label:>12}  {r['n_cols']:>4}  {el:>6}  {r['raw_line']}")

# Show the transition point
print("\n=== TRANSITION ANALYSIS ===")
prev_cols = None
for r in results:
    if prev_cols is not None and r['n_cols'] != prev_cols:
        label = f"{r['month']:02d}/{r['year']}"
        print(f"Column count changed from {prev_cols} to {r['n_cols']} at {label}")
    prev_cols = r['n_cols']
