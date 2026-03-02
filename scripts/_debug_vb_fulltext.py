"""Full page text dump for 2026 and older PDFs."""
import pdfplumber
from pathlib import Path

data_root = '/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads'
vb_dir = Path(data_root) / 'Visa_Bulletin'

targets = ['January2026', 'Jan2012']
for t in targets:
    files = list(vb_dir.rglob(f'*{t}*'))
    if not files:
        continue
    f = files[0]
    print(f'=== {f.name} ({len(list(pdfplumber.open(f).pages))} pages) ===')
    with pdfplumber.open(f) as pdf:
        for pg_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if not text:
                print(f'  Page {pg_num+1}: (no text)')
                continue
            lines = text.split('\n')
            print(f'  Page {pg_num+1}: ({len(lines)} lines)')
            # Print first 3 lines
            for i, line in enumerate(lines[:3]):
                print(f'    L{i}: {line}')
            # Search for employment/action/filing keywords
            for i, line in enumerate(lines):
                if any(kw in line.upper() for kw in ['ACTION', 'FILING', 'EMPLOYMENT', '1ST', '2ND', 'EB1', 'EB2', 'ALL CHARGE', 'INDIA', 'PREFERENCE']):
                    print(f'    L{i}: {line}')
            print()
    print()
