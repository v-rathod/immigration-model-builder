"""Debug: inspect raw PDF text for specific visa bulletins to verify EB2 India dates."""
import pdfplumber
from pathlib import Path

data_root = '/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads'
vb_dir = Path(data_root) / 'Visa_Bulletin'

# Bulletins where our data shows problematic values
targets = ['January2018', 'January2022', 'October2017', 'December2022', 'May2016', 'January2020']

for t in targets:
    files = list(vb_dir.rglob(f'*{t}*'))
    if not files:
        print(f'=== {t}: NOT FOUND ===\n')
        continue
    f = files[0]
    print(f'=== {f.name} ===')
    with pdfplumber.open(f) as pdf:
        for pg_num, page in enumerate(pdf.pages):
            text = page.extract_text()
            if text and 'FINAL ACTION DATES FOR EMPLOYMENT-BASED' in text:
                print(f'  [Page {pg_num+1} - FINAL ACTION DATES]')
                lines = text.split('\n')
                # Print the table area
                in_table = False
                for i, line in enumerate(lines):
                    if 'FINAL ACTION' in line and 'EMPLOYMENT' in line:
                        in_table = True
                    if in_table:
                        print(f'    L{i:3d}: {line}')
                    # Stop after 5th category row
                    if in_table and ('5th' in line or 'Certain' in line):
                        # Print a few more lines
                        for j in range(i+1, min(i+3, len(lines))):
                            print(f'    L{j:3d}: {lines[j]}')
                        break
                print()
            
            if text and 'DATES FOR FILING' in text and 'EMPLOYMENT' in text:
                print(f'  [Page {pg_num+1} - DATES FOR FILING]')
                lines = text.split('\n')
                in_table = False
                for i, line in enumerate(lines):
                    if 'DATES FOR FILING' in line:
                        in_table = True
                    if in_table:
                        print(f'    L{i:3d}: {line}')
                    if in_table and ('5th' in line or 'Certain' in line):
                        for j in range(i+1, min(i+3, len(lines))):
                            print(f'    L{j:3d}: {lines[j]}')
                        break
                print()
    print()
