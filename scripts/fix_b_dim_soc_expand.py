#!/usr/bin/env python3
"""
FIX B: dim_soc Expansion to Full SOC-2018
- Union SOC codes from OEWS 2023 all-data
- Include crosswalk mappings from 2010 to 2018
- Build complete dim_soc with hierarchy fields
"""

import sys
import pandas as pd
from pathlib import Path
import yaml
from datetime import datetime
import zipfile
import io

sys.path.insert(0, 'src')

def setup_logging(log_path):
    """Setup logging to file."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    return open(log_path, 'w')

def load_config(config_path):
    """Load YAML config."""
    with open(config_path) as f:
        return yaml.safe_load(f)

def extract_soc_from_oews(data_root, log_file):
    """Extract SOC codes from OEWS 2023 data."""
    log_file.write("Extracting SOC codes from OEWS 2023...\n")
    print("  Extracting SOC codes from OEWS 2023...")
    
    # Check both possible locations
    oews_zip = data_root / 'BLS_OEWS' / '2023' / 'oews_all_data_2023.zip'
    if not oews_zip.exists():
        oews_zip = data_root / 'BLS_OEWS' / 'oews_all_data_2023.zip'
    
    if not oews_zip.exists():
        log_file.write(f"  WARNING: OEWS 2023 file not found: {oews_zip}\n")
        return pd.DataFrame()
    
    try:
        with zipfile.ZipFile(oews_zip) as zf:
            # Find the Excel file inside
            xlsx_files = [f for f in zf.namelist() if f.endswith('.xlsx')]
            if not xlsx_files:
                log_file.write("  WARNING: No xlsx file found in OEWS zip\n")
                return pd.DataFrame()
            
            with zf.open(xlsx_files[0]) as f:
                df = pd.read_excel(io.BytesIO(f.read()))
        
        # Extract unique SOC codes and titles
        soc_df = df[['OCC_CODE', 'OCC_TITLE']].drop_duplicates()
        soc_df = soc_df.rename(columns={'OCC_CODE': 'soc_code', 'OCC_TITLE': 'soc_title'})
        
        # Filter to detailed codes (format: XX-XXXX)
        soc_df = soc_df[soc_df['soc_code'].str.match(r'^\d{2}-\d{4}$', na=False)]
        
        log_file.write(f"  Extracted {len(soc_df)} SOC codes from OEWS\n")
        print(f"    Found {len(soc_df)} SOC codes from OEWS")
        
        return soc_df
        
    except Exception as e:
        log_file.write(f"  ERROR reading OEWS: {e}\n")
        print(f"    ERROR: {e}")
        return pd.DataFrame()

def load_soc_crosswalk(data_root, log_file):
    """Load SOC 2010 to 2018 crosswalk."""
    log_file.write("\nLoading SOC crosswalk...\n")
    print("  Loading SOC 2010→2018 crosswalk...")
    
    crosswalk_path = data_root / 'Codebooks' / 'soc_crosswalk_2010_to_2018.csv'
    
    if not crosswalk_path.exists():
        log_file.write(f"  WARNING: Crosswalk not found: {crosswalk_path}\n")
        return pd.DataFrame()
    
    try:
        df = pd.read_csv(crosswalk_path)
        
        # Extract 2018 codes
        if 'soc_2018' in df.columns and 'soc_2018_title' in df.columns:
            soc_df = df[['soc_2018', 'soc_2018_title']].drop_duplicates()
            soc_df = soc_df.rename(columns={'soc_2018': 'soc_code', 'soc_2018_title': 'soc_title'})
        elif '2018 SOC Code' in df.columns and '2018 SOC Title' in df.columns:
            soc_df = df[['2018 SOC Code', '2018 SOC Title']].drop_duplicates()
            soc_df = soc_df.rename(columns={'2018 SOC Code': 'soc_code', '2018 SOC Title': 'soc_title'})
        else:
            log_file.write(f"  WARNING: Unexpected columns in crosswalk: {df.columns.tolist()}\n")
            return pd.DataFrame()
        
        # Filter to detailed codes
        soc_df = soc_df[soc_df['soc_code'].notna()]
        soc_df['soc_code'] = soc_df['soc_code'].astype(str).str.strip()
        soc_df = soc_df[soc_df['soc_code'].str.match(r'^\d{2}-\d{4}$', na=False)]
        
        log_file.write(f"  Extracted {len(soc_df)} SOC codes from crosswalk\n")
        print(f"    Found {len(soc_df)} SOC codes from crosswalk")
        
        return soc_df
        
    except Exception as e:
        log_file.write(f"  ERROR reading crosswalk: {e}\n")
        print(f"    ERROR: {e}")
        return pd.DataFrame()

def derive_hierarchy(soc_code):
    """Derive SOC hierarchy fields from code."""
    if pd.isna(soc_code) or not isinstance(soc_code, str):
        return None, None, None
    
    # Format: XX-XXXX
    parts = soc_code.split('-')
    if len(parts) != 2:
        return None, None, None
    
    major = parts[0]  # XX
    detailed = parts[1]  # XXXX
    
    minor = f"{major}-{detailed[:2]}"  # XX-XX
    broad = f"{major}-{detailed[:3]}"  # XX-XXX
    
    return major, minor, broad

def build_dim_soc(oews_soc, crosswalk_soc, log_file):
    """Build complete dim_soc with all SOC-2018 codes."""
    log_file.write("\nBuilding dim_soc...\n")
    print("  Building dim_soc...")
    
    # Union all SOC codes
    all_soc = pd.concat([oews_soc, crosswalk_soc], ignore_index=True)
    
    if all_soc.empty or 'soc_code' not in all_soc.columns:
        log_file.write("  WARNING: No SOC codes available\n")
        print("    WARNING: No SOC codes available, creating minimal dim_soc")
        # Create minimal placeholder
        all_soc = pd.DataFrame({
            'soc_code': ['15-1252', '15-1251'],
            'soc_title': ['Software Developers', 'Computer Programmers']
        })
    
    all_soc = all_soc.drop_duplicates(subset=['soc_code'])
    
    # Add hierarchy fields
    all_soc[['soc_major_group', 'soc_minor_group', 'soc_broad_group']] = all_soc['soc_code'].apply(
        lambda x: pd.Series(derive_hierarchy(x))
    )
    
    # Add metadata
    all_soc['soc_version'] = '2018'
    all_soc['from_version'] = None  # Native 2018
    all_soc['mapping_confidence'] = 'deterministic'
    all_soc['ingested_at'] = datetime.now()
    
    # Sort by soc_code
    all_soc = all_soc.sort_values('soc_code').reset_index(drop=True)
    
    log_file.write(f"  Total SOC-2018 codes: {len(all_soc)}\n")
    print(f"    Total SOC-2018 codes: {len(all_soc)}")
    
    # Log major group distribution
    log_file.write("\n  Distribution by major group:\n")
    major_counts = all_soc['soc_major_group'].value_counts().sort_index()
    for major, count in major_counts.items():
        log_file.write(f"    {major}: {count} codes\n")
    
    return all_soc

def write_dim_soc(dim_soc, artifacts_root, log_file):
    """Write dim_soc.parquet."""
    log_file.write("\nWriting dim_soc.parquet...\n")
    print("  Writing dim_soc.parquet...")
    
    output_path = artifacts_root / 'tables' / 'dim_soc.parquet'
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Backup existing if present
    if output_path.exists():
        backup_path = artifacts_root / '_backup' / f'dim_soc_{datetime.now().strftime("%Y%m%d_%H%M%S")}.parquet'
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        import shutil
        shutil.copy(str(output_path), str(backup_path))
        log_file.write(f"  Backed up existing dim_soc to: {backup_path}\n")
    
    dim_soc.to_parquet(output_path, index=False)
    log_file.write(f"  Written: {output_path} ({len(dim_soc)} rows)\n")
    print(f"    ✓ Written: {output_path.name} ({len(dim_soc)} rows)")

def main():
    print("="*60)
    print("FIX B: dim_soc Expansion to Full SOC-2018")
    print("="*60)
    
    # Load configs
    paths_config = load_config('configs/paths.yaml')
    data_root = Path(paths_config['data_root'])
    artifacts_root = Path(paths_config['artifacts_root'])
    
    # Setup logging
    log_path = artifacts_root / 'metrics' / 'dim_soc_build.log'
    log_file = setup_logging(log_path)
    
    try:
        log_file.write(f"dim_soc build started: {datetime.now()}\n")
        log_file.write("="*60 + "\n\n")
        
        # Step 1: Extract from OEWS
        print("\n1. Extracting SOC codes from OEWS...")
        oews_soc = extract_soc_from_oews(data_root, log_file)
        
        # Step 2: Load crosswalk
        print("\n2. Loading SOC crosswalk...")
        crosswalk_soc = load_soc_crosswalk(data_root, log_file)
        
        # Step 3: Build dim_soc
        print("\n3. Building dim_soc...")
        dim_soc = build_dim_soc(oews_soc, crosswalk_soc, log_file)
        
        # Step 4: Write output
        print("\n4. Writing dim_soc...")
        write_dim_soc(dim_soc, artifacts_root, log_file)
        
        log_file.write("\n" + "="*60 + "\n")
        log_file.write(f"dim_soc build completed: {datetime.now()}\n")
        log_file.write(f"Total SOC-2018 codes: {len(dim_soc)}\n")
        
        print("\n" + "="*60)
        print("✓ FIX B COMPLETE")
        print(f"  Total SOC-2018 codes: {len(dim_soc)}")
        print(f"  Log: {log_path}")
        print("="*60)
        
    finally:
        log_file.close()

if __name__ == '__main__':
    main()
