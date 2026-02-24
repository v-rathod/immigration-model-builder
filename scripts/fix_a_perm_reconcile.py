#!/usr/bin/env python3
"""
FIX A: PERM Reconciliation & Deduplication
- Quarantine legacy single file
- Harmonize columns across fiscal years
- Deduplicate on case_number or stable subset
- Map employer_id and soc_code
- Rewrite clean partitions
"""

import sys
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import yaml
from datetime import datetime
import re
import hashlib

# Add src to path
sys.path.insert(0, 'src')

def setup_logging(log_path):
    """Setup logging to file."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return open(log_path, 'w')

def load_config(config_path):
    """Load YAML config."""
    with open(config_path) as f:
        return yaml.safe_load(f)

def normalize_employer_name(name):
    """Normalize employer name for matching."""
    if pd.isna(name):
        return None
    name = str(name).upper().strip()
    # Remove common suffixes
    for suffix in [' INC', ' LLC', ' LTD', ' CORP', ' CO', ' CORPORATION', ',']:
        name = name.replace(suffix, '')
    # Remove extra whitespace
    name = ' '.join(name.split())
    return name if name else None

def generate_employer_id(normalized_name):
    """Generate employer_id from normalized name."""
    if not normalized_name:
        return None
    # Use first 8 chars of MD5 hash
    return 'EMP_' + hashlib.md5(normalized_name.encode()).hexdigest()[:8].upper()

def quarantine_legacy_files(artifacts_root, log_file):
    """Move legacy single-file PERM to quarantine."""
    legacy_file = artifacts_root / 'tables' / 'fact_perm.parquet'
    quarantine_dir = artifacts_root / '_quarantine'
    
    if legacy_file.exists():
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantine_path = quarantine_dir / f'fact_perm_legacy_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        legacy_file.rename(quarantine_path)
        log_file.write(f"✓ Quarantined legacy file: {quarantine_path}\n")
        print(f"  ✓ Quarantined legacy file: {quarantine_path.name}")
    else:
        log_file.write("  No legacy single file found\n")
        print("  No legacy single file found")

def load_partitioned_perm(artifacts_root, log_file):
    """Load all PERM partitions."""
    fact_perm_dir = artifacts_root / 'tables' / 'fact_perm'
    
    if not fact_perm_dir.exists():
        raise FileNotFoundError(f"Partitioned PERM directory not found: {fact_perm_dir}")
    
    parquet_files = sorted(fact_perm_dir.rglob('*.parquet'))
    log_file.write(f"\nLoading {len(parquet_files)} partition files...\n")
    print(f"  Loading {len(parquet_files)} partition files...")
    
    dfs = []
    for pf in parquet_files:
        df = pd.read_parquet(pf)
        
        # Restore fiscal_year from directory structure
        parts = pf.parts
        fy_part = next((p for p in parts if p.startswith('fiscal_year=')), None)
        if fy_part:
            fy_value = fy_part.split('=')[1]
            if 'fiscal_year' not in df.columns:
                df['fiscal_year'] = int(fy_value) if fy_value.isdigit() else fy_value
        
        dfs.append(df)
    
    df_all = pd.concat(dfs, ignore_index=True)
    log_file.write(f"  Total rows loaded: {len(df_all):,}\n")
    print(f"  Total rows loaded: {len(df_all):,}")
    
    return df_all

def harmonize_columns(df, schema, log_file):
    """Harmonize column names to match schema."""
    log_file.write("\nColumn harmonization:\n")
    print("  Harmonizing columns...")
    
    # Get expected columns from schema
    expected_cols = {field['name'] for field in schema['fields']}
    current_cols = set(df.columns)
    
    # Common column aliases/mappings
    column_mappings = {
        'fy': 'fiscal_year',
        'case_no': 'case_number',
        'status': 'case_status',
        'wage_from': 'wage_offer_from',
        'wage_to': 'wage_offer_to',
        'wage_unit_of_pay': 'wage_offer_unit',
        'city': 'worksite_city',
        'state': 'worksite_state',
        'postal_code': 'worksite_postal',
    }
    
    # Apply mappings
    df = df.rename(columns=column_mappings)
    
    # Add missing columns as nulls
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None
            log_file.write(f"  Added missing column: {col}\n")
    
    # Ensure fiscal_year is int
    if 'fiscal_year' in df.columns:
        df['fiscal_year'] = pd.to_numeric(df['fiscal_year'], errors='coerce').fillna(0).astype(int)
    
    log_file.write(f"  Columns after harmonization: {len(df.columns)}\n")
    return df

def deduplicate_perm(df, log_file):
    """Deduplicate PERM data."""
    log_file.write("\nDeduplication:\n")
    print("  Deduplicating rows...")
    
    rows_before = len(df)
    
    if 'case_number' in df.columns and df['case_number'].notna().any():
        # Dedupe on case_number within each fiscal_year
        log_file.write("  Using case_number for deduplication\n")
        
        # Sort by decision_date descending (prefer latest)
        if 'decision_date' in df.columns:
            df['decision_date'] = pd.to_datetime(df['decision_date'], errors='coerce')
            df = df.sort_values('decision_date', ascending=False, na_position='last')
        
        df = df.drop_duplicates(subset=['fiscal_year', 'case_number'], keep='first')
    else:
        # Dedupe on stable subset with hash key
        log_file.write("  Using composite key for deduplication\n")
        
        stable_cols = ['employer_id', 'soc_code', 'decision_date', 'worksite_state', 
                      'wage_offer_from', 'wage_offer_unit']
        available_cols = [c for c in stable_cols if c in df.columns]
        
        if available_cols:
            # Create hash key from available columns
            df['_dedup_key'] = df[available_cols].astype(str).agg('_'.join, axis=1)
            df = df.drop_duplicates(subset=['fiscal_year', '_dedup_key'], keep='first')
            df = df.drop(columns=['_dedup_key'])
    
    rows_after = len(df)
    dedup_count = rows_before - rows_after
    
    log_file.write(f"  Rows before: {rows_before:,}\n")
    log_file.write(f"  Rows after: {rows_after:,}\n")
    log_file.write(f"  Duplicates removed: {dedup_count:,}\n")
    print(f"  Removed {dedup_count:,} duplicates ({rows_after:,} rows remaining)")
    
    return df, dedup_count

def map_employer_and_soc(df, artifacts_root, log_file):
    """Map employer names to employer_id and SOC codes."""
    log_file.write("\nEmployer & SOC mapping:\n")
    print("  Mapping employers and SOC codes...")
    
    unmatched_employers = 0
    unmatched_soc = 0
    
    # Load dim_employer if exists
    dim_employer_path = artifacts_root / 'tables' / 'dim_employer.parquet'
    if dim_employer_path.exists():
        dim_employer = pd.read_parquet(dim_employer_path)
        
        # Create mapping dict
        employer_map = {}
        for _, row in dim_employer.iterrows():
            if pd.notna(row.get('employer_name_normalized')):
                employer_map[row['employer_name_normalized']] = row['employer_id']
        
        # Map employer_id if not already present
        if 'employer_name_raw' in df.columns:
            df['employer_name_normalized'] = df['employer_name_raw'].apply(normalize_employer_name)
            
            if 'employer_id' not in df.columns or df['employer_id'].isna().all():
                df['employer_id'] = df['employer_name_normalized'].map(employer_map)
            
            # Generate employer_id for unmapped
            unmapped_mask = df['employer_id'].isna() & df['employer_name_normalized'].notna()
            if unmapped_mask.any():
                df.loc[unmapped_mask, 'employer_id'] = df.loc[unmapped_mask, 'employer_name_normalized'].apply(generate_employer_id)
                unmatched_employers = unmapped_mask.sum()
                
                # Log alias suggestions
                alias_log = artifacts_root / 'metrics' / 'perm_alias_suggestions.log'
                alias_log.parent.mkdir(parents=True, exist_ok=True)
                with open(alias_log, 'w') as f:
                    for name in df.loc[unmapped_mask, 'employer_name_normalized'].unique():
                        if name:
                            f.write(f"{name}\n")
    
    # Load dim_soc if exists
    dim_soc_path = artifacts_root / 'tables' / 'dim_soc.parquet'
    if dim_soc_path.exists():
        dim_soc = pd.read_parquet(dim_soc_path)
        soc_codes = set(dim_soc['soc_code'].values)
        
        # Check for unmapped SOC codes
        if 'soc_code' in df.columns:
            unmapped_soc_mask = df['soc_code'].notna() & ~df['soc_code'].isin(soc_codes)
            unmatched_soc = unmapped_soc_mask.sum()
    
    log_file.write(f"  Unmatched employers: {unmatched_employers:,}\n")
    log_file.write(f"  Unmatched SOC codes: {unmatched_soc:,}\n")
    print(f"  Unmatched employers: {unmatched_employers:,}")
    print(f"  Unmatched SOC codes: {unmatched_soc:,}")
    
    return df, unmatched_employers, unmatched_soc

def write_partitioned_perm(df, artifacts_root, log_file):
    """Write clean partitioned PERM data."""
    log_file.write("\nWriting partitioned output:\n")
    print("  Writing partitioned output...")
    
    output_dir = artifacts_root / 'tables' / 'fact_perm'
    backup_dir = artifacts_root / '_backup' / 'fact_perm' / datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Backup existing partitions
    if output_dir.exists():
        backup_dir.mkdir(parents=True, exist_ok=True)
        for item in output_dir.iterdir():
            if item.is_dir() and item.name.startswith('fiscal_year='):
                import shutil
                shutil.move(str(item), str(backup_dir / item.name))
        log_file.write(f"  Backed up existing partitions to: {backup_dir}\n")
    
    # Write new partitions
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Group by fiscal_year and write each partition
    for fy, group in df.groupby('fiscal_year'):
        if pd.isna(fy) or int(fy) == 0:
            fy_dir = output_dir / '__HIVE_DEFAULT_PARTITION__'
        else:
            fy_dir = output_dir / f'fiscal_year={int(fy)}'
        
        fy_dir.mkdir(parents=True, exist_ok=True)
        
        # Drop partition column before writing
        group_to_write = group.drop(columns=['fiscal_year'])
        group_to_write.to_parquet(fy_dir / 'data.parquet', index=False)
        
        log_file.write(f"  Written: fiscal_year={int(fy) if not pd.isna(fy) else 'NULL'} ({len(group):,} rows)\n")
    
    print(f"  ✓ Written {len(df['fiscal_year'].unique())} partitions")

def main():
    print("="*60)
    print("FIX A: PERM Reconciliation & Deduplication")
    print("="*60)
    
    # Load configs
    paths_config = load_config('configs/paths.yaml')
    schemas_config = load_config('configs/schemas.yml')
    
    artifacts_root = Path(paths_config['artifacts_root'])
    perm_schema = schemas_config['fact_perm']
    
    # Setup logging
    log_path = artifacts_root / 'metrics' / 'fact_perm_reconcile.log'
    log_file = setup_logging(log_path)
    
    try:
        log_file.write(f"PERM Reconciliation started: {datetime.now()}\n")
        log_file.write("="*60 + "\n\n")
        
        # Step 1: Quarantine legacy files
        print("\n1. Quarantining legacy files...")
        quarantine_legacy_files(artifacts_root, log_file)
        
        # Step 2: Load partitioned data
        print("\n2. Loading partitioned data...")
        df = load_partitioned_perm(artifacts_root, log_file)
        rows_initial = len(df)
        
        # Step 3: Harmonize columns
        print("\n3. Harmonizing columns...")
        df = harmonize_columns(df, perm_schema, log_file)
        
        # Step 4: Deduplicate
        print("\n4. Deduplicating rows...")
        df, dedup_count = deduplicate_perm(df, log_file)
        
        # Step 5: Map employer & SOC
        print("\n5. Mapping employers and SOC codes...")
        df, unmatched_emp, unmatched_soc = map_employer_and_soc(df, artifacts_root, log_file)
        
        # Step 6: Write clean partitions
        print("\n6. Writing clean partitions...")
        write_partitioned_perm(df, artifacts_root, log_file)
        
        # Summary
        log_file.write("\n" + "="*60 + "\n")
        log_file.write("SUMMARY BY FISCAL YEAR:\n")
        log_file.write("="*60 + "\n")
        
        for fy in sorted(df['fiscal_year'].unique()):
            if pd.isna(fy):
                continue
            fy_data = df[df['fiscal_year'] == fy]
            log_file.write(f"\nFY{int(fy)}:\n")
            log_file.write(f"  Rows: {len(fy_data):,}\n")
            log_file.write(f"  Unique case_numbers: {fy_data['case_number'].nunique() if 'case_number' in fy_data else 'N/A'}\n")
            log_file.write(f"  Null employer_id: {fy_data['employer_id'].isna().sum():,}\n")
            log_file.write(f"  Null soc_code: {fy_data['soc_code'].isna().sum():,}\n")
        
        log_file.write("\n" + "="*60 + "\n")
        log_file.write(f"PERM Reconciliation completed: {datetime.now()}\n")
        log_file.write(f"Total rows processed: {rows_initial:,} → {len(df):,}\n")
        log_file.write(f"Duplicates removed: {dedup_count:,}\n")
        log_file.write(f"Unmatched employers: {unmatched_emp:,}\n")
        log_file.write(f"Unmatched SOC codes: {unmatched_soc:,}\n")
        
        print("\n" + "="*60)
        print("✓ FIX A COMPLETE")
        print(f"  Initial rows: {rows_initial:,}")
        print(f"  Final rows: {len(df):,}")
        print(f"  Duplicates removed: {dedup_count:,}")
        print(f"  Log: {log_path}")
        print("="*60)
        
    finally:
        log_file.close()

if __name__ == '__main__':
    main()
