"""Debug: check what the 7th column is in May 2018+ PDFs."""
import pdfplumber
from pathlib import Path

data_root = '/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads'
vb_dir = Path(data_root) / 'Visa_Bulletin'

# Check a 7-column PDF
targets = ['May2018', 'July2019', 'January2020', 'May2021']
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
                lines = text.split('\n')
                # Print lines from FINAL ACTION to end of section
                printing = False
                for i, line in enumerate(lines):
                    if 'FINAL ACTION' in line:
                        printing = True
                    if printing:
                        print(f'  L{i:3d}: {line}')
                    if printing and i > 5 and ('Certain' in line or 'Workers' in line or '5th' in line):
                        # Print 2 more lines
                        for j in range(i+1, min(i+3, len(lines))):
                            print(f'  L{j:3d}: {lines[j]}')
                        printing = False
                        break
                print()
    print()
