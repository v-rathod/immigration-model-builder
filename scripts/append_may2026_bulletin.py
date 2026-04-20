#!/usr/bin/env python3.12
"""Quick script to process just May 2026 bulletin and append to fact_cutoffs_all."""

import sys
from pathlib import Path

# Add repo root to sys.path
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pandas as pd
from src.curate.visa_bulletin_loader import parse_filename, parse_employment_table, extract_employment_table_from_text
import pdfplumber

def process_may2026_bulletin():
    """Process just the May 2026 bulletin PDF and append to fact_cutoffs_all."""
    
    # Paths
    p1_root = Path("/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads")
    may_2026_pdf = p1_root / "Visa_Bulletin/2026/visabulletin_May2026.pdf"
    fact_cutoffs_all = Path("artifacts/tables/fact_cutoffs_all.parquet")
    
    if not may_2026_pdf.exists():
        print(f"ERROR: {may_2026_pdf} not found")
        return
    
    if not fact_cutoffs_all.exists():
        print(f"ERROR: {fact_cutoffs_all} not found")
        return
    
    print(f"Processing May 2026 bulletin: {may_2026_pdf}")
    print(f"Target: {fact_cutoffs_all}")
    print()
    
    # Load existing data
    df_existing = pd.read_parquet(fact_cutoffs_all)
    print(f"Existing fact_cutoffs_all: {len(df_existing):,} rows")
    print(f"Latest month currently: {df_existing['bulletin_year'].max()}-{df_existing[df_existing['bulletin_year'] == df_existing['bulletin_year'].max()]['bulletin_month'].max():02d}")
    print()
    
    # Parse May 2026 PDF
    year, month = parse_filename(may_2026_pdf.name)
    print(f"Parsing {may_2026_pdf.name}...")
    print(f"  Identified: {year}-{month:02d}")
    
    all_rows = []
    try:
        with pdfplumber.open(str(may_2026_pdf)) as pdf:
            fad_found = False
            dff_found = False
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if not text:
                    continue
                
                text_upper = text.upper()
                
                # Check for Final Action Dates chart
                is_fad_page = (
                    ('FINAL ACTION DATES' in text_upper and 'EMPLOYMENT' in text_upper)
                    or ('CUT-OFF DATE' in text_upper and 'EMPLOYMENT' in text_upper)
                )
                if is_fad_page and not fad_found:
                    table = extract_employment_table_from_text(text, 'FAD')
                    if table:
                        rows = parse_employment_table(
                            table, year, month, 'FAD',
                            str(may_2026_pdf.relative_to(p1_root)), f"page_{page_num+1}"
                        )
                        all_rows.extend(rows)
                        fad_found = True
                        print(f"  ✓ FAD table found (page {page_num+1}): {len(rows)} rows")
                
                # Check for Dates for Filing chart
                if 'DATES FOR FILING' in text_upper and 'EMPLOYMENT' in text_upper and not dff_found:
                    table = extract_employment_table_from_text(text, 'DFF')
                    if table:
                        rows = parse_employment_table(
                            table, year, month, 'DFF',
                            str(may_2026_pdf.relative_to(p1_root)), f"page_{page_num+1}"
                        )
                        all_rows.extend(rows)
                        dff_found = True
                        print(f"  ✓ DFF table found (page {page_num+1}): {len(rows)} rows")
    except Exception as e:
        print(f"  ✗ Error processing PDF: {e}")
        import traceback
        traceback.print_exc()
        return
    
    if not all_rows:
        print("  ✗ No rows extracted from PDF")
        return
    
    # Convert to DataFrame
    df_new = pd.DataFrame(all_rows)
    df_new['bulletin_year'] = df_new['bulletin_year'].astype(int)
    df_new['bulletin_month'] = df_new['bulletin_month'].astype(int)
    df_new['cutoff_date'] = pd.to_datetime(df_new['cutoff_date'], errors='coerce')
    df_new['ingested_at'] = pd.to_datetime(df_new['ingested_at'])
    
    print(f"\n  Extracted {len(df_new)} rows from May 2026")
    print(f"  Charts: {df_new['chart'].unique()}")
    print(f"  Categories: {sorted(df_new['category'].unique())}")
    
    # Check for duplicates with existing data (shouldn't be any if this is May 2026)
    existing_may2026 = df_existing[
        (df_existing['bulletin_year'] == 2026) & 
        (df_existing['bulletin_month'] == 5)
    ]
    if len(existing_may2026) > 0:
        print(f"\n  ⚠️  WARNING: {len(existing_may2026)} rows already exist for May 2026")
        print("     These will be removed and replaced with new data")
        df_existing = df_existing[~(
            (df_existing['bulletin_year'] == 2026) & 
            (df_existing['bulletin_month'] == 5)
        )]
    
    # Append new rows
    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
    print(f"\nCombined data: {len(df_combined):,} rows (was {len(df_existing):,}, added {len(df_new)})")
    
    # Verify structure
    print(f"\nColumns: {sorted(df_combined.columns.tolist())}")
    print(f"Dtypes:\n{df_combined.dtypes}")
    
    # Write to parquet
    df_combined.to_parquet(fact_cutoffs_all, index=False)
    print(f"\n✅ Updated {fact_cutoffs_all}")
    print(f"\nNew latest month: {df_combined['bulletin_year'].max()}-{df_combined[df_combined['bulletin_year'] == df_combined['bulletin_year'].max()]['bulletin_month'].max():02d}")

if __name__ == "__main__":
    process_may2026_bulletin()
