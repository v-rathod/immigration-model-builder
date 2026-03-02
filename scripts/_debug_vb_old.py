"""Full page text dump for older visa bulletin PDFs."""
import pdfplumber
from pathlib import Path

data_root = '/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads'
vb_dir = Path(data_root) / 'Visa_Bulletin'

# Check one from each problematic year
targets = ['July2011', 'Jan2012', 'january2013', 'january2014']
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
                print(f'  Page {pg_num+1}: (empty)')
                continue
            lines = text.split('\n')
            # Look for any line with 1st or 2nd or EB categories
            for i, line in enumerate(lines):
                if line.strip().startswith(('1st', '2nd', '3rd', 'Other', '4th', '5th')):
                    # Print surrounding context
                    start = max(0, i - 3)
                    end = min(len(lines), i + 2)
                    print(f'  Page {pg_num+1}, found data-like line:')
                    for j in range(start, end):
                        marker = ' >> ' if j == i else '    '
                        print(f'{marker}L{j}: {lines[j]}')
                    print()
                    break
    print()
