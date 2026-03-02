"""Debug 2026 and abbreviated-month PDFs."""
import pdfplumber
from pathlib import Path

data_root = '/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads'
vb_dir = Path(data_root) / 'Visa_Bulletin'

# Check 2026 bulletin
targets = ['January2026', 'Jan2012', 'Oct2011']
for t in targets:
    files = list(vb_dir.rglob(f'*{t}*'))
    if not files:
        print(f'{t}: NOT FOUND')
        continue
    f = files[0]
    print(f'=== {f.name} ===')
    with pdfplumber.open(f) as pdf:
        for pg_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                continue
            has_fad = 'FINAL ACTION DATES FOR EMPLOYMENT-BASED' in text
            has_dff = 'DATES FOR FILING' in text and 'EMPLOYMENT-BASED' in text
            if has_fad or has_dff:
                label = 'FAD' if has_fad else 'DFF'
                print(f'  Page {pg_num+1}: {label} detected')
                lines = text.split('\n')
                for i, line in enumerate(lines):
                    if any(x in line for x in ['FINAL ACTION', 'DATES FOR FILING', 'Employment', '1st', '2nd', 'All Charge', 'INDIA', 'Action Dates']):
                        print(f'    L{i}: {line}')
                print()
            else:
                # Show first few lines to understand format
                lines = text.split('\n')[:5]
                found_keywords = [l for l in text.split('\n') if 'ACTION' in l or 'EMPLOYMENT' in l or 'FILING' in l]
                if found_keywords:
                    print(f'  Page {pg_num+1}: keywords found but not matching:')
                    for kw in found_keywords[:5]:
                        print(f'    {kw}')
    print()
