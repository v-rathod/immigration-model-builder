"""Show full page 3 text for older PDFs."""
import pdfplumber
from pathlib import Path

data_root = '/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads'
vb_dir = Path(data_root) / 'Visa_Bulletin'

targets = ['July2011', 'Jan2012']
for t in targets:
    files = list(vb_dir.rglob(f'*{t}*'))
    if not files:
        continue
    f = files[0]
    print(f'=== {f.name} (page 3) ===')
    with pdfplumber.open(f) as pdf:
        page = pdf.pages[2]  # page 3 (0-indexed)
        text = page.extract_text()
        lines = text.split('\n')
        for i, line in enumerate(lines[:40]):
            print(f'  L{i:2d}: {line}')
    print()
