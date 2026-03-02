"""Debug: check May 2016 and other edge-case PDFs for EL SALVADOR detection."""
import pdfplumber
from pathlib import Path
import re

data_root = '/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads'
vb_dir = Path(data_root) / 'Visa_Bulletin'

targets = ['May2016', 'April2016', 'June2016', 'October2021', 'September2021', 'March2023', 'April2023']
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
            if 'FINAL ACTION DATES FOR EMPLOYMENT-BASED' in text:
                # Check for EL SALVADOR / GUATEMALA / HONDURAS
                has = bool(re.search(r'EL\s+SALVADOR|GUATEMALA|HONDURAS', text, re.IGNORECASE))
                has_vietnam = bool(re.search(r'\bVIETNAM\b', text, re.IGNORECASE))
                print(f'  Page {pg_num+1}: EL_SAL={has}, VIETNAM={has_vietnam}')
                # Show header area
                lines = text.split('\n')
                for i, line in enumerate(lines):
                    if any(x in line for x in ['All Charge', 'Except', 'Listed', 'born', 'INDIA', 'MEXICO', 'PHIL', 'VIETNAM', 'SALVADOR', 'HONDURAS', 'GUATEMALA']):
                        print(f'    L{i}: {line}')
                # Show EB2 line
                for line in lines:
                    if line.strip().startswith('2nd'):
                        parts = line.strip().split()
                        print(f'  EB2 line ({len(parts)} parts): {line.strip()}')
                        break
                print()
    print()
