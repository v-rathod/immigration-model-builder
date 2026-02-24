"""
Build fact_perm: PERM labor certification outcomes with dimension joins.
"""
import hashlib
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timezone
import re
import sys


def load_employer_layout(layouts_path: Path) -> dict:
    """Load employer normalization rules from layouts/employer.yml."""
    employer_yml = layouts_path / "layouts" / "employer.yml"
    if not employer_yml.exists():
        raise FileNotFoundError(f"Employer layout not found: {employer_yml}")
    
    with open(employer_yml, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def normalize_employer_name(name: str, layout: dict) -> str:
    """
    Normalize employer name using the same pipeline as dim_employer.
    Returns normalized lowercase name for hashing.
    """
    if pd.isna(name) or not isinstance(name, str):
        return ""
    
    # Step 1: Lowercase
    normalized = name.lower().strip()
    
    # Step 2: Strip punctuation
    punct_chars = layout.get("punctuation_to_strip", [])
    for char in punct_chars:
        normalized = normalized.replace(char, " ")
    
    # Step 3: Remove legal suffixes (word boundaries)
    suffixes = layout.get("suffixes", [])
    for suffix in suffixes:
        # Match suffix at end with optional punctuation
        pattern = r'\b' + re.escape(suffix.lower()) + r'\.?\s*$'
        normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE)
    
    # Step 4: Collapse multiple whitespace
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Step 5: Strip leading/trailing whitespace
    normalized = normalized.strip()
    
    return normalized


def compute_employer_id(normalized_name: str) -> str:
    """Generate SHA1 hash as employer_id."""
    if not normalized_name:
        return hashlib.sha1(b"UNKNOWN").hexdigest()
    
    return hashlib.sha1(normalized_name.encode('utf-8')).hexdigest()


def load_dimensions(artifacts_path: Path) -> dict:
    """Load dimension tables for FK lookups."""
    dims = {}
    
    # Load dim_employer
    employer_path = artifacts_path / "tables" / "dim_employer.parquet"
    if employer_path.exists():
        dims['employer'] = pd.read_parquet(employer_path)
        print(f"  Loaded dim_employer: {len(dims['employer'])} rows")
    else:
        print(f"  WARNING: dim_employer not found at {employer_path}")
        dims['employer'] = pd.DataFrame(columns=['employer_id', 'employer_name'])
    
    # Load dim_soc
    soc_path = artifacts_path / "tables" / "dim_soc.parquet"
    if soc_path.exists():
        dims['soc'] = pd.read_parquet(soc_path)
        print(f"  Loaded dim_soc: {len(dims['soc'])} rows")
    else:
        print(f"  WARNING: dim_soc not found at {soc_path}")
        dims['soc'] = pd.DataFrame(columns=['soc_code'])
    
    # Load dim_area
    area_path = artifacts_path / "tables" / "dim_area.parquet"
    if area_path.exists():
        dims['area'] = pd.read_parquet(area_path)
        print(f"  Loaded dim_area: {len(dims['area'])} rows")
    else:
        print(f"  WARNING: dim_area not found at {area_path}")
        dims['area'] = pd.DataFrame(columns=['area_code'])
    
    # Load dim_country
    country_path = artifacts_path / "tables" / "dim_country.parquet"
    if country_path.exists():
        dims['country'] = pd.read_parquet(country_path)
        print(f"  Loaded dim_country: {len(dims['country'])} rows")
    else:
        print(f"  WARNING: dim_country not found at {country_path}")
        dims['country'] = pd.DataFrame(columns=['iso3', 'country_name'])
    
    return dims


def map_soc_code(raw_soc: str, soc_dim: pd.DataFrame) -> str:
    """Map raw SOC code to canonical SOC 2018 code."""
    if pd.isna(raw_soc) or not isinstance(raw_soc, str):
        return None
    
    # Clean up: remove whitespace, ensure format XX-XXXX
    raw_soc = str(raw_soc).strip()
    
    # Try exact match
    if raw_soc in soc_dim['soc_code'].values:
        return raw_soc
    
    # Try with hyphen normalization (15-1252 vs 151252)
    if '-' not in raw_soc and len(raw_soc) >= 6:
        normalized = f"{raw_soc[:2]}-{raw_soc[2:]}"
        if normalized in soc_dim['soc_code'].values:
            return normalized
    
    # Return None if no match (will be handled downstream)
    return None


def map_area_code(raw_area: str, area_dim: pd.DataFrame) -> str:
    """Map raw BLS area code to canonical area_code."""
    if pd.isna(raw_area):
        return None
    
    # Convert to string and strip
    raw_area = str(raw_area).strip()
    
    # Try exact match
    if raw_area in area_dim['area_code'].values:
        return raw_area
    
    # Try with leading zeros (some areas might be stored as int then string)
    # Area codes are typically 7 digits
    if raw_area.isdigit():
        padded = raw_area.zfill(7)
        if padded in area_dim['area_code'].values:
            return padded
    
    return None


def map_country(raw_country: str, country_dim: pd.DataFrame) -> str:
    """Map raw country name to ISO3 code."""
    if pd.isna(raw_country) or not isinstance(raw_country, str):
        return None
    
    raw_country = raw_country.strip().upper()
    
    # Try matching country_name (case-insensitive)
    country_upper = country_dim.copy()
    country_upper['country_name_upper'] = country_upper['country_name'].str.upper()
    
    matches = country_upper[country_upper['country_name_upper'] == raw_country]
    if len(matches) > 0:
        return matches.iloc[0]['iso3']
    
    # Try matching iso3 directly
    if raw_country in country_dim['iso3'].values:
        return raw_country
    
    # Try matching iso2
    if 'iso2' in country_dim.columns:
        matches = country_dim[country_dim['iso2'].str.upper() == raw_country]
        if len(matches) > 0:
            return matches.iloc[0]['iso3']
    
    # Common mapping for USA
    if raw_country in ['UNITED STATES', 'USA', 'US', 'U.S.', 'U.S.A.']:
        usa_match = country_dim[country_dim['iso3'] == 'USA']
        if len(usa_match) > 0:
            return 'USA'
    
    return None


def derive_fy(received_date) -> int:
    """Derive fiscal year from received date (FY starts Oct 1)."""
    if pd.isna(received_date):
        return None
    
    if not isinstance(received_date, pd.Timestamp):
        return None
    
    # FY 2025 = Oct 1, 2024 - Sep 30, 2025
    # If month >= 10, FY = year + 1
    # If month < 10, FY = year
    if received_date.month >= 10:
        return received_date.year + 1
    else:
        return received_date.year


def find_perm_files(data_root: Path, max_files: int = None) -> list:
    """
    Find ALL PERM Excel files in FY directories.
    Returns list of tuples: (fy, file_path)
    """
    perm_base = data_root / "PERM" / "PERM"
    if not perm_base.exists():
        print(f"  WARNING: PERM directory not found: {perm_base}")
        return []
    
    files = []
    
    # Find FY directories
    for fy_dir in sorted(perm_base.iterdir(), reverse=True):
        if not fy_dir.is_dir():
            continue
        if not fy_dir.name.startswith("FY"):
            continue
        
        # Extract FY number
        fy_match = re.search(r'FY(\d{4})', fy_dir.name)
        if not fy_match:
            continue
        fy = int(fy_match.group(1))
        
        # Find Excel files (both old PERM_FY*.xlsx and new PERM_Disclosure_*.xlsx)
        for pattern in ["PERM_Disclosure_Data_*.xlsx", "PERM_FY*.xlsx", "PERM_*.xlsx"]:
            for excel_file in fy_dir.glob(pattern):
                # Avoid duplicates
                if (fy, excel_file) not in files:
                    files.append((fy, excel_file))
    
    # Sort by FY descending (apply limit if specified)
    files = sorted(files, key=lambda x: x[0], reverse=True)
    if max_files:
        files = files[:max_files]
    
    return files


def build_fact_perm(
    data_root: Path,
    output_path: Path,
    artifacts_path: Path,
    layouts_path: Path,
    chunk_size: int = 100000,
    dry_run: bool = False
):
    """
    Build fact_perm from ALL PERM Excel files with chunked processing.
    Writes partitioned parquet by fiscal_year.
    
    Args:
        data_root: Root directory containing PERM files
        output_path: Base path for partitioned output (will write to output_path/fiscal_year=YYYY/)
        artifacts_path: Path to artifacts root (for loading dimensions)
        layouts_path: Path to configs root (for loading employer layout)
        chunk_size: Maximum rows to process per chunk (default 100k)
        dry_run: If True, discover files only without writing outputs
    """
    print("[BUILD FACT_PERM" + (" - DRY RUN]" if dry_run else "]"))
    
    # Find ALL PERM files
    perm_files = find_perm_files(data_root, max_files=None)
    if not perm_files:
        print("  No PERM files found")
        return
    
    print(f"  Found {len(perm_files)} PERM file(s):")
    for fy, fpath in perm_files:
        rel_path = fpath.relative_to(data_root) if data_root in fpath.parents else fpath
        print(f"    FY{fy}: {rel_path}")
    
    print(f"  Chunk size: {chunk_size:,} rows")
    
    # Discover partitions
    fiscal_years = sorted([fy for fy, _ in perm_files])
    print(f"\n  Planned partitions (fiscal_year): {', '.join(map(str, fiscal_years))}")
    
    if dry_run:
        # Convert output_path to directory for partitioned format
        if output_path.suffix == '.parquet':
            output_dir = output_path.parent / output_path.stem
        else:
            output_dir = output_path
        print(f"\n  DRY RUN: Would write partitioned parquet to {output_dir}/fiscal_year=YYYY/")
        print("  No files were created.")
        return
    
    # Real run - proceed with loading and processing
    print("\n  Loading dimensions for FK lookups...")
    
    # Load employer normalization rules
    layout = load_employer_layout(layouts_path)
    
    # Load dimension tables for lookups
    dims = load_dimensions(artifacts_path)
    
    # Process each file
    all_rows = []
    total_processed = 0
    unmapped_soc = set()
    unmapped_area = set()
    unmapped_country = set()
    
    for fy, file_path in perm_files:
        print(f"\n  Processing FY{fy}...")
        
        try:
            # Sample rows from Excel
            df = pd.read_excel(file_path, nrows=sample_size)
            print(f"    Loaded {len(df)} rows (sample)")
            
            # Required columns (flexible header names)
            col_map = {
                'case_number': 'CASE_NUMBER',
                'case_status': 'CASE_STATUS',
                'received_date': 'RECEIVED_DATE',
                'decision_date': 'DECISION_DATE',
                'employer_name': 'EMP_BUSINESS_NAME',
                'employer_country': 'EMP_COUNTRY',
                'soc_code': 'PWD_SOC_CODE',
                'soc_title': 'PWD_SOC_TITLE',
                'job_title': 'JOB_TITLE',
                'wage_from': 'JOB_OPP_WAGE_FROM',
                'wage_to': 'JOB_OPP_WAGE_TO',
                'wage_unit': 'JOB_OPP_WAGE_PER',
                'worksite_city': 'PRIMARY_WORKSITE_CITY',
                'worksite_state': 'PRIMARY_WORKSITE_STATE',
                'worksite_postal': 'PRIMARY_WORKSITE_POSTAL_CODE',
                'worksite_area': 'PRIMARY_WORKSITE_BLS_AREA',
                'is_fulltime': 'OTHER_REQ_IS_FULLTIME_EMP',
            }
            
            # Check which columns exist
            missing = [k for k, v in col_map.items() if v not in df.columns]
            if missing:
                print(f"    WARNING: Missing columns: {missing}")
            
            # Build fact rows
            for idx, row in df.iterrows():
                # Safely get columns that might not exist
                def safe_get(col_key):
                    """Safely get column value, return None if column doesn't exist."""
                    col_name = col_map.get(col_key)
                    if col_name and col_name in df.columns:
                        return row.get(col_name)
                    return None
                
                # Normalize employer name and compute employer_id
                raw_employer = safe_get('employer_name')
                normalized_employer = normalize_employer_name(raw_employer, layout)
                employer_id = compute_employer_id(normalized_employer)
                
                # Map SOC code
                raw_soc = safe_get('soc_code')
                soc_code = map_soc_code(raw_soc, dims['soc'])
                if raw_soc and not soc_code:
                    unmapped_soc.add(str(raw_soc))
                
                # Map area code
                raw_area = safe_get('worksite_area')
                area_code = map_area_code(raw_area, dims['area'])
                if raw_area and not area_code:
                    unmapped_area.add(str(raw_area))
                
                # Map country
                raw_country = safe_get('employer_country')
                country_iso3 = map_country(raw_country, dims['country'])
                if raw_country and not country_iso3:
                    unmapped_country.add(str(raw_country))
                
                # Parse dates
                received_date = pd.to_datetime(safe_get('received_date'), errors='coerce')
                decision_date = pd.to_datetime(safe_get('decision_date'), errors='coerce')
                
                # Derive FY
                fy_derived = derive_fy(received_date)
                
                # Build fact row
                fact_row = {
                    'case_number': safe_get('case_number'),
                    'case_status': safe_get('case_status'),
                    'received_date': received_date,
                    'decision_date': decision_date,
                    'employer_id': employer_id,
                    'soc_code': soc_code,
                    'area_code': area_code,
                    'employer_country': country_iso3,
                    'job_title': safe_get('job_title'),
                    'wage_offer_from': pd.to_numeric(safe_get('wage_from'), errors='coerce'),
                    'wage_offer_to': pd.to_numeric(safe_get('wage_to'), errors='coerce'),
                    'wage_offer_unit': safe_get('wage_unit'),
                    'worksite_city': safe_get('worksite_city'),
                    'worksite_state': safe_get('worksite_state'),
                    'worksite_postal': safe_get('worksite_postal'),
                    'is_fulltime': safe_get('is_fulltime') == 'Y',
                    'fy': fy_derived,
                    'source_file': f"PERM/PERM/FY{fy}/{file_path.name}",
                    'ingested_at': datetime.now(timezone.utc),
                }
                
                all_rows.append(fact_row)
            
            total_processed += len(df)
            print(f"    Processed {len(df)} rows from FY{fy}")
            
        except Exception as e:
            print(f"    ERROR processing {file_path.name}: {e}")
            continue
    
    # Build final DataFrame
    if not all_rows:
        print("  No rows to write")
        return
    
    result_df = pd.DataFrame(all_rows)
    
    # Ensure proper dtypes for parquet compatibility
    # worksite_postal should be string (not int - can have dashes like "60064-1802")
    if 'worksite_postal' in result_df.columns:
        result_df['worksite_postal'] = result_df['worksite_postal'].astype(str)
    
    print(f"\n  Processed {total_processed} total rows")
    print(f"  Built {len(result_df)} fact_perm records")
    
    # Validation
    print(f"\n  Validation:")
    print(f"    Unique case_numbers: {result_df['case_number'].nunique()}")
    print(f"    Non-null employer_id: {result_df['employer_id'].notna().sum()}")
    print(f"    Non-null soc_code: {result_df['soc_code'].notna().sum()}")
    print(f"    Non-null area_code: {result_df['area_code'].notna().sum()}")
    print(f"    Non-null employer_country: {result_df['employer_country'].notna().sum()}")
    
    if unmapped_soc:
        print(f"    Unmapped SOC codes: {len(unmapped_soc)} (samples: {list(unmapped_soc)[:5]})")
    if unmapped_area:
        print(f"    Unmapped area codes: {len(unmapped_area)} (samples: {list(unmapped_area)[:5]})")
    if unmapped_country:
        print(f"    Unmapped countries: {len(unmapped_country)} (samples: {list(unmapped_country)[:5]})")
    
    # Write parquet
    output_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_parquet(output_path, index=False, engine='pyarrow')
    print(f"  Written: {output_path}")
    print(f"  Rows: {len(result_df)}")
    
    return result_df


if __name__ == "__main__":
    # Standalone test mode
    import sys
    if len(sys.argv) < 2:
        print("Usage: python build_fact_perm.py <data_root>")
        sys.exit(1)
    
    data_root = Path(sys.argv[1])
    artifacts_path = Path("artifacts")
    layouts_path = Path("configs")
    output_path = artifacts_path / "tables" / "fact_perm.parquet"
    
    build_fact_perm(data_root, output_path, artifacts_path, layouts_path)
