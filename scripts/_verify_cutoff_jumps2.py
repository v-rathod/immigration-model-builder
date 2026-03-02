#!/usr/bin/env python3
"""Verify suspicious cutoff jumps against raw PDF text - with correct paths."""
import pdfplumber
from pathlib import Path

VB_BASE = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads/Visa_Bulletin")

# Suspicious jumps to verify
CHECKS = [
    # (label, year_dir, filename_pattern, what to look for)
    ("EB1/IND Aug 2023 (jump 2022→2012)", "2023", "August"),
    ("EB1/IND Jul 2023 (before jump)", "2023", "July"),
    ("EB2/CHN Sep 2015 (jump 2013→2006)", "2015", "September"),
    ("EB2/CHN Aug 2015 (before jump)", "2015", "August"),
    ("EB3/IND Sep 2018 (jump 2008→2003)", "2018", "September"),
    ("EB3/IND Jul 2018 (before jump)", "2018", "July"),
    ("EB2/PHL Apr 2023 (jump 2009→2022)", "2023", "April"),
    ("EB2/PHL Mar 2023 (before jump)", "2023", "March"),
]

for label, year, month in CHECKS:
    # Find the PDF
    vb_dir = VB_BASE / year
    matches = list(vb_dir.glob(f"*{month}*"))
    if not matches:
        print(f"\n{'='*70}")
        print(f"  {label} — NOT FOUND in {vb_dir}")
        continue
    
    pdf_path = matches[0]
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"  File: {pdf_path.name}")
    print(f"{'='*70}")
    
    with pdfplumber.open(pdf_path) as pdf:
        for pg_num, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            upper = text.upper()
            # Look for employment-based tables
            if 'EMPLOYMENT' in upper and ('PREFERENCE' in upper or 'BASED' in upper):
                lines = text.split('\n')
                for i, line in enumerate(lines):
                    lu = line.upper().strip()
                    # Print lines with EB category indicators
                    if any(x in lu for x in ['1ST', '2ND', '3RD', '4TH', '5TH', 
                                              'EMPLOYMENT', 'ALL CHARGE', 'CHINA',
                                              'INDIA', 'MEXICO', 'PHILIP', 'EL SAL',
                                              'VIETNAM', 'REST OF']):
                        print(f"  P{pg_num+1} L{i:3d}: {line.strip()}")
                print()
