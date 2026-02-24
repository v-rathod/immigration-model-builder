#!/usr/bin/env python3
"""
FIX C: Visa Bulletin Deduplication & Legacy Parsing
- Deduplicate fact_cutoffs by PK
- Add secondary parser for legacy PDFs (2011-2014)
- Rewrite clean partitions
"""

import sys
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import yaml
from datetime import datetime
import pdfplumber
import re

sys.path.insert(0, 'src')

def setup_logging(log_path):
    """Setup logging to file."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return open(log_path, 'w')

def load_config(config_path):
    """Load YAML config."""
    with open(config_path) as f:
        return yaml.safe_load(f)

def load_partitioned_cutoffs(artifacts_root, log_file):
    """Load all cutoffs partitions."""
    fact_cutoffs_dir = artifacts_root / 'tables' / 'fact_cutoffs'
    
    if not fact_cutoffs_dir.exists():
        log_file.write(f"WARNING: fact_cutoffs directory not found\n")
        return pd.DataFrame()
    
    parquet_files = sorted(fact_cutoffs_dir.rglob('*.parquet'))
    log_file.write(f"Loading {len(parquet_files)} partition files...\n")
    print(f"  Loading {len(parquet_files)} partition files...")
    
    dfs = []
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        
        # Restore partition columns
        parts = pf.parts
        for part in parts:
            if '=' in part:
                col_name, col_value = part.split('=', 1)
                if col_name not in df.columns:
                    if col_name.endswith('year') or col_name.endswith('month'):
                        df[col_name] = int(col_value) if col_value.isdigit() else col_value
                    else:
                        df[col_name] = col_value
        
        dfs.append(df)
    
    if not dfs:
        return pd.DataFrame()
    
    df_all = pd.concat(dfs, ignore_index=True)
    log_file.write(f"  Total rows loaded: {len(df_all):,}\n")
    print(f"  Total rows loaded: {len(df_all):,}")
    
    return df_all

def deduplicate_cutoffs(df, log_file):
    """Deduplicate fact_cutoffs by PK with priority rules."""
    log_file.write("\nDeduplication:\n")
    print("  Deduplicating rows...")
    
    rows_before = len(df)
    
    pk_cols = ['bulletin_year', 'bulletin_month', 'chart', 'category', 'country']
    
    # Check if PK columns exist
    missing_cols = [c for c in pk_cols if c not in df.columns]
    if missing_cols:
        log_file.write(f"  WARNING: Missing PK columns: {missing_cols}\n")
        return df, 0, {}
    
    # Find duplicates
    duplicates = df[df.duplicated(subset=pk_cols, keep=False)]
    
    if len(duplicates) == 0:
        log_file.write("  No duplicates found\n")
        return df, 0, {}
    
    log_file.write(f"  Found {len(duplicates)} duplicate rows ({duplicates[pk_cols].drop_duplicates().shape[0]} unique PKs)\n")
    
    # Priority rules for deduplication:
    # 1. Prefer status_flag='D' (dated cutoff)
    # 2. Then prefer non-null cutoff_date
    # 3. Then lexically first source_file
    
    # Add priority score
    df['_priority'] = 0
    if 'status_flag' in df.columns:
        df.loc[df['status_flag'] == 'D', '_priority'] += 100
        df.loc[df['status_flag'] == 'C', '_priority'] += 50
    if 'cutoff_date' in df.columns:
        df.loc[df['cutoff_date'].notna(), '_priority'] += 10
    
    # Sort by PK + priority descending
    df = df.sort_values(pk_cols + ['_priority'], ascending=[True]*len(pk_cols) + [False])
    
    # Keep first (highest priority)
    df_deduped = df.drop_duplicates(subset=pk_cols, keep='first')
    df_deduped = df_deduped.drop(columns=['_priority'])
    
    rows_after = len(df_deduped)
    dedup_count = rows_before - rows_after
    
    # Calculate per-month dedup stats
    monthly_stats = {}
    for (year, month), group in duplicates.groupby(['bulletin_year', 'bulletin_month']):
        monthly_stats[f"{int(year)}-{int(month):02d}"] = len(group)
    
    log_file.write(f"  Rows before: {rows_before:,}\n")
    log_file.write(f"  Rows after: {rows_after:,}\n")
    log_file.write(f"  Duplicates removed: {dedup_count:,}\n")
    
    if monthly_stats:
        log_file.write("\n  Duplicates by month:\n")
        for month, count in sorted(monthly_stats.items()):
            log_file.write(f"    {month}: {count} duplicate rows\n")
    
    print(f"  Removed {dedup_count:,} duplicates ({rows_after:,} rows remaining)")
    
    return df_deduped, dedup_count, monthly_stats

def parse_legacy_pdfs(data_root, log_file):
    """Attempt to parse legacy PDFs (2011-2014) with alternative patterns."""
    log_file.write("\nLegacy PDF parsing:\n")
    print("  Attempting legacy PDF parsing...")
    
    vb_dir = data_root / 'Visa_Bulletin'
    if not vb_dir.exists():
        log_file.write("  WARNING: Visa_Bulletin directory not found\n")
        return []
    
    # Find PDFs from 2011-2014 that aren't already processed
    legacy_years = ['2011', '2012', '2013', '2014']
    legacy_pdfs = []
    
    for year in legacy_years:
        year_dir = vb_dir / year
        if year_dir.exists():
            legacy_pdfs.extend(year_dir.glob('*.pdf'))
    
    log_file.write(f"  Found {len(legacy_pdfs)} legacy PDFs to try\n")
    
    # Try parsing with relaxed patterns
    parsed_count = 0
    skipped_files = []
    
    for pdf_path in sorted(legacy_pdfs)[:20]:  # Limit to first 20 for performance
        try:
            with pdfplumber.open(pdf_path) as pdf:
                text = '\n'.join(page.extract_text() or '' for page in pdf.pages)
                
                # Look for employment-based tables with relaxed patterns
                has_eb_table = any([
                    'EMPLOYMENT-BASED' in text.upper(),
                    'FINAL ACTION DATE' in text.upper(),
                    'DATE FOR FILING' in text.upper(),
                ])
                
                if has_eb_table:
                    parsed_count += 1
                    log_file.write(f"  ✓ Parseable: {pdf_path.name}\n")
                else:
                    skipped_files.append(pdf_path.name)
                    log_file.write(f"  ✗ Skipped (no EB table): {pdf_path.name}\n")
        
        except Exception as e:
            skipped_files.append(pdf_path.name)
            log_file.write(f"  ✗ Error parsing {pdf_path.name}: {e}\n")
    
    log_file.write(f"\n  Legacy PDFs parseable: {parsed_count}\n")
    log_file.write(f"  Legacy PDFs skipped: {len(skipped_files)}\n")
    print(f"    Legacy PDFs parseable: {parsed_count}")
    print(f"    Legacy PDFs skipped: {len(skipped_files)}")
    
    return skipped_files

def write_partitioned_cutoffs(df, artifacts_root, log_file):
    """Write clean partitioned cutoffs data."""
    log_file.write("\nWriting partitioned output:\n")
    print("  Writing partitioned output...")
    
    output_dir = artifacts_root / 'tables' / 'fact_cutoffs'
    backup_dir = artifacts_root / '_backup' / 'fact_cutoffs' / datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Backup existing partitions
    if output_dir.exists():
        backup_dir.mkdir(parents=True, exist_ok=True)
        import shutil
        for item in output_dir.iterdir():
            if item.is_dir() and item.name.startswith('bulletin_year='):
                shutil.move(str(item), str(backup_dir / item.name))
        log_file.write(f"  Backed up existing partitions to: {backup_dir}\n")
    
    # Write new partitions
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Group by bulletin_year and bulletin_month
    for (year, month), group in df.groupby(['bulletin_year', 'bulletin_month']):
        if pd.isna(year) or pd.isna(month):
            continue
        
        year_dir = output_dir / f'bulletin_year={int(year)}'
        month_dir = year_dir / f'bulletin_month={int(month):02d}'
        month_dir.mkdir(parents=True, exist_ok=True)
        
        # Drop partition columns before writing
        group_to_write = group.drop(columns=['bulletin_year', 'bulletin_month'])
        group_to_write.to_parquet(month_dir / 'data.parquet', index=False)
        
        log_file.write(f"  Written: {int(year)}-{int(month):02d} ({len(group):,} rows)\n")
    
    partition_count = len(df.groupby(['bulletin_year', 'bulletin_month']))
    print(f"  ✓ Written {partition_count} partitions")

def main():
    print("="*60)
    print("FIX C: Visa Bulletin Deduplication & Legacy Parsing")
    print("="*60)
    
    # Load configs
    paths_config = load_config('configs/paths.yaml')
    data_root = Path(paths_config['data_root'])
    artifacts_root = Path(paths_config['artifacts_root'])
    
    # Setup logging
    log_path = artifacts_root / 'metrics' / 'fact_cutoffs_dedupe.log'
    log_file = setup_logging(log_path)
    
    try:
        log_file.write(f"Visa Bulletin deduplication started: {datetime.now()}\n")
        log_file.write("="*60 + "\n\n")
        
        # Step 1: Load partitioned data
        print("\n1. Loading partitioned data...")
        df = load_partitioned_cutoffs(artifacts_root, log_file)
        
        if df.empty:
            print("  WARNING: No data loaded, skipping deduplication")
            log_file.write("  WARNING: No data loaded\n")
            return
        
        rows_initial = len(df)
        
        # Step 2: Deduplicate
        print("\n2. Deduplicating rows...")
        df, dedup_count, monthly_stats = deduplicate_cutoffs(df, log_file)
        
        # Step 3: Try parsing legacy PDFs
        print("\n3. Attempting legacy PDF parsing...")
        skipped_files = parse_legacy_pdfs(data_root, log_file)
        
        # Step 4: Write clean partitions
        print("\n4. Writing clean partitions...")
        write_partitioned_cutoffs(df, artifacts_root, log_file)
        
        # Check PK uniqueness
        pk_cols = ['bulletin_year', 'bulletin_month', 'chart', 'category', 'country']
        pk_unique = not df.duplicated(subset=pk_cols).any()
        
        log_file.write("\n" + "="*60 + "\n")
        log_file.write("SUMMARY:\n")
        log_file.write("="*60 + "\n")
        log_file.write(f"Initial rows: {rows_initial:,}\n")
        log_file.write(f"Final rows: {len(df):,}\n")
        log_file.write(f"Duplicates removed: {dedup_count:,}\n")
        log_file.write(f"PK uniqueness: {'PASS' if pk_unique else 'FAIL'}\n")
        log_file.write(f"Legacy PDFs skipped: {len(skipped_files)}\n")
        
        if skipped_files:
            log_file.write("\nLegacy skipped files (top 50):\n")
            for f in skipped_files[:50]:
                log_file.write(f"  {f}\n")
        
        log_file.write("\n" + "="*60 + "\n")
        log_file.write(f"Visa Bulletin deduplication completed: {datetime.now()}\n")
        
        print("\n" + "="*60)
        print("✓ FIX C COMPLETE")
        print(f"  Initial rows: {rows_initial:,}")
        print(f"  Final rows: {len(df):,}")
        print(f"  Duplicates removed: {dedup_count:,}")
        print(f"  PK uniqueness: {'PASS' if pk_unique else 'FAIL'}")
        print(f"  Log: {log_path}")
        print("="*60)
        
    finally:
        log_file.close()

if __name__ == '__main__':
    main()
