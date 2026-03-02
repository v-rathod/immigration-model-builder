#!/usr/bin/env python3
"""Verify suspicious cutoff jumps against raw PDF text."""
import pdfplumber, re
from pathlib import Path

VB_DIR = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads/Visa_Bulletin")

# The suspicious jumps to verify:
# 1) EB1/IND 2023-08: 2022-02-01 → 2012-01-01  (jump -10 years)
# 2) EB2/CHN 2015-09: 2013-12-15 → 2006-01-01  (jump -8 years)
# 3) EB3/IND 2018-09: 2008-11-01 → 2003-01-01  (jump -6 years)
# 4) EB2/PHL 2023-04: 2009-01-01 → 2022-07-01  (jump +13.5 years)

CHECKS = [
    ("EB1/IND Aug 2023", "visabulletin_aug2023.pdf", "EB1", "INDIA"),
    ("EB1/IND Jul 2023", "visabulletin_jul2023.pdf", "EB1", "INDIA"),
    ("EB2/CHN Sep 2015", "visabulletin_sep2015.pdf", "EB2", "CHINA"),
    ("EB2/CHN Aug 2015", "visabulletin_aug2015.pdf", "EB2", "CHINA"),
    ("EB3/IND Sep 2018", "visabulletin_sep2018.pdf", "EB3", "INDIA"),
    ("EB3/IND Aug 2018", "visabulletin_aug2018.pdf", "EB3", "INDIA"),
    ("EB2/PHL Apr 2023", "visabulletin_apr2023.pdf", "EB2", "PHILIPPINES"),
    ("EB2/PHL Mar 2023", "visabulletin_mar2023.pdf", "EB2", "PHILIPPINES"),
]

def extract_eb_table_text(pdf_path):
    """Extract text around employment-based tables."""
    with pdfplumber.open(pdf_path) as pdf:
        full_text = ""
        for page in pdf.pages:
            text = page.extract_text() or ""
            full_text += text + "\n---PAGE BREAK---\n"
    return full_text

for label, filename, cat, country in CHECKS:
    pdf_path = VB_DIR / filename
    if not pdf_path.exists():
        # Try other naming patterns
        for p in VB_DIR.glob(f"*{filename.split('_')[1].split('.')[0]}*"):
            pdf_path = p
            break
    
    print(f"\n{'='*70}")
    print(f"  {label} — {pdf_path.name}")
    print(f"{'='*70}")
    
    if not pdf_path.exists():
        print(f"  FILE NOT FOUND: {filename}")
        continue
    
    text = extract_eb_table_text(pdf_path)
    
    # Find employment-based section
    lines = text.split('\n')
    in_eb_section = False
    for i, line in enumerate(lines):
        upper = line.upper().strip()
        if 'EMPLOYMENT' in upper and ('BASED' in upper or 'PREFERENCE' in upper):
            in_eb_section = True
        if in_eb_section:
            # Check for the category
            if cat.upper().replace('EB', '') in upper or f'EB-{cat[2:]}' in upper or cat in upper:
                # Print surrounding context
                start = max(0, i-2)
                end = min(len(lines), i+5)
                for j in range(start, end):
                    marker = ">>>" if j == i else "   "
                    print(f"  {marker} L{j}: {lines[j]}")
                print()
            # Also check for rows with dates that might match
            if '1ST' in upper or '2ND' in upper or '3RD' in upper or '4TH' in upper or '5TH' in upper:
                print(f"  >>> L{i}: {line}")
