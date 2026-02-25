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


def find_perm_files(data_root: Path, max_files: int = None, min_fy: int = None) -> list:
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
        
        # Skip FYs below min_fy threshold
        if min_fy is not None and fy < min_fy:
            continue
        
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
    dry_run: bool = False,
    min_fy: int = None,
):
    """
    Build fact_perm from PERM Excel files with chunked processing.
    Writes partitioned parquet by fiscal_year.
    
    Args:
        data_root: Root directory containing PERM files
        output_path: Base path for partitioned output (will write to output_path/fiscal_year=YYYY/)
        artifacts_path: Path to artifacts root (for loading dimensions)
        layouts_path: Path to configs root (for loading employer layout)
        chunk_size: Maximum rows to process per chunk (default 100k)
        dry_run: If True, discover files only without writing outputs
        min_fy: If set, only process FY >= min_fy (for targeted partial rebuilds)
    """
    print("[BUILD FACT_PERM" + (" - DRY RUN]" if dry_run else "]"))
    
    # Find PERM files (optionally filtered by min_fy for targeted rebuilds)
    perm_files = find_perm_files(data_root, max_files=None, min_fy=min_fy)
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
    
    # Pre-build lookup sets for fast vectorized mapping
    soc_valid = set(dims['soc']['soc_code'].values)
    area_df = dims['area']
    area_valid = set(area_df['area_code'].values)
    # Build area_title → area_code map for name-format BLS area values
    area_title_map: dict[str, str] = {}
    if 'area_title' in area_df.columns:
        for _, arow in area_df.dropna(subset=['area_title', 'area_code']).iterrows():
            title_key = str(arow['area_title']).strip().upper()
            area_title_map[title_key] = str(arow['area_code'])
    country_upper_map = dict(zip(
        dims['country']['country_name'].str.upper().fillna(''),
        dims['country']['iso3']
    ))
    country_iso3_set = set(dims['country']['iso3'].values)

    def _map_soc_vec(raw_soc):
        if pd.isna(raw_soc) or not isinstance(raw_soc, str):
            return None
        s = str(raw_soc).strip()
        # Strip '.xx' decimal suffix (e.g. '17-2112.00' → '17-2112')
        s = re.sub(r'\.\d+$', '', s)
        if s in soc_valid:
            return s
        if '-' not in s and len(s) >= 6:
            norm = f"{s[:2]}-{s[2:]}"
            if norm in soc_valid:
                return norm
        return None

    def _map_area_vec(raw_area):
        if pd.isna(raw_area):
            return None
        s = str(raw_area).strip()
        # Try exact numeric code match
        if s in area_valid:
            return s
        # Try zero-padded numeric
        if s.isdigit():
            padded = s.zfill(7)
            if padded in area_valid:
                return padded
        # Try matching by area_title (BLS area name, e.g. 'San Jose-..., CA')
        title_key = s.upper()
        if title_key in area_title_map:
            return area_title_map[title_key]
        return None

    def _map_country_vec(raw_country):
        if pd.isna(raw_country) or not isinstance(raw_country, str):
            return None
        c = raw_country.strip().upper()
        if c in country_upper_map:
            return country_upper_map[c]
        if c in country_iso3_set:
            return c
        if c in {'UNITED STATES', 'USA', 'US', 'U.S.', 'U.S.A.'} and 'USA' in country_iso3_set:
            return 'USA'
        return None

    # Process each file
    all_dfs = []
    total_processed = 0
    unmapped_soc = set()
    unmapped_area = set()
    unmapped_country = set()
    
    for fy, file_path in perm_files:
        print(f"\n  Processing FY{fy}...")
        
        try:
            # Read full Excel file
            df = pd.read_excel(file_path)
            print(f"    Loaded {len(df)} rows, {len(df.columns)} cols")

            # ── Column name normalisation ──────────────────────────────────
            # PERM files use 4+ naming eras:
            #   Legacy SPACES  (FY2009):  "DECISION DATE", "EMPLOYER NAME"
            #   Legacy Title   (FY2013-14): "Decision_Date", "Employer_Name"
            #   iCERT UPPER    (FY2015-19): "CASE_RECEIVED_DATE"
            #   FLAG UPPER     (FY2020-24): "RECEIVED_DATE"
            # Normalise ALL column names to UPPER_UNDERSCORE so col_map
            # can use a single canonical lookup regardless of era.
            df.columns = df.columns.str.strip().str.upper().str.replace(' ', '_')

            # Multi-schema column candidates: try each in order, use first match.
            # After normalisation all column names are UPPER_UNDERSCORE.
            # Aliases are ordered: newer-form → older-form (first match wins).
            col_map = {
                'case_number':      ['CASE_NUMBER', 'CASE_NO'],
                'case_status':      ['CASE_STATUS'],
                'received_date':    ['RECEIVED_DATE', 'CASE_RECEIVED_DATE'],
                'decision_date':    ['DECISION_DATE'],
                'employer_name':    ['EMP_BUSINESS_NAME', 'EMPLOYER_NAME'],
                'employer_country': [
                    # Worker's country of citizenship (most useful for immigration analytics).
                    # FY2008-2024 old form: COUNTRY_OF_CITIZENSHIP / COUNTRY_OF_CITZENSHIP (typo in source).
                    # FY2024 new form / FY2025+: only EMP_COUNTRY available (employer location).
                    'COUNTRY_OF_CITIZENSHIP', 'COUNTRY_OF_CITZENSHIP',
                    'EMP_COUNTRY', 'EMPLOYER_COUNTRY',
                ],
                'soc_code':         ['PWD_SOC_CODE', 'PW_SOC_CODE'],
                'soc_title':        ['PWD_SOC_TITLE', 'PW_SOC_TITLE'],
                'job_title':        [
                    'JOB_TITLE',              # FY2020+ FLAG / new form
                    'JOB_INFO_JOB_TITLE',     # FY2015-2019 iCERT (actual job title)
                    'PW_JOB_TITLE_9089',      # FY2008-2018 legacy
                    'PW_JOB_TITLE',           # FY2019
                ],
                'wage_from':        [
                    'JOB_OPP_WAGE_FROM',      # FY2024 new / FY2025+
                    'WAGE_OFFER_FROM',        # FY2020-2024 FLAG
                    'WAGE_OFFER_FROM_9089',   # FY2008-2018
                    'WAGE_OFFERED_FROM_9089',  # FY2013-2014, FY2019
                ],
                'wage_to':          [
                    'JOB_OPP_WAGE_TO',
                    'WAGE_OFFER_TO',
                    'WAGE_OFFER_TO_9089',
                    'WAGE_OFFERED_TO_9089',
                ],
                'wage_unit':        [
                    'JOB_OPP_WAGE_PER',
                    'WAGE_OFFER_UNIT_OF_PAY',
                    'PW_UNIT_OF_PAY_9089',
                    'WAGE_OFFER_UNIT_OF_PAY_9089',
                    'WAGE_OFFERED_UNIT_OF_PAY_9089',
                ],
                'worksite_city':    [
                    'PRIMARY_WORKSITE_CITY',   # FY2024 new / FY2025+
                    'WORKSITE_CITY',           # FY2020-2024 FLAG
                    'JOB_INFO_WORK_CITY',      # FY2008-2019 iCERT (actual worksite)
                    'EMPLOYER_CITY',           # FY2008-2019 fallback (employer HQ)
                ],
                'worksite_state':   [
                    'PRIMARY_WORKSITE_STATE',
                    'WORKSITE_STATE',
                    'JOB_INFO_WORK_STATE',
                    'EMPLOYER_STATE',
                    'EMPLOYER_STATE_PROVINCE',
                ],
                'worksite_postal':  [
                    'PRIMARY_WORKSITE_POSTAL_CODE',
                    'WORKSITE_POSTAL_CODE',
                    'JOB_INFO_WORK_POSTAL_CODE',
                    'EMPLOYER_POSTAL_CODE',
                ],
                'worksite_area':    ['PRIMARY_WORKSITE_BLS_AREA'],  # FY2024 new+ only
                'is_fulltime':      ['OTHER_REQ_IS_FULLTIME_EMP'],
                'naics_code':       [
                    'NAICS_CODE',             # FY2020-2024 FLAG
                    'NAICS_US_CODE',          # FY2015-2019 iCERT
                    'EMP_NAICS',              # FY2024 new / FY2025+
                    '2007_NAICS_US_CODE',     # FY2008-2014 legacy
                ],
            }

            # Check which logical fields have no matching column
            missing = [k for k, cands in col_map.items()
                       if not any(c in df.columns for c in cands)]
            if missing:
                print(f"    WARNING: Missing columns: {missing}")

            # Helper: try each candidate in order; return first that exists
            def safe_col(key):
                candidates = col_map.get(key, [])
                for cname in candidates:
                    if cname in df.columns:
                        return df[cname]
                return pd.Series([None] * len(df), index=df.index, dtype=object)

            # --- Vectorized employer normalisation + hashing ---
            employer_series = safe_col('employer_name').apply(
                lambda x: normalize_employer_name(x, layout)
            )
            employer_id_series = employer_series.apply(compute_employer_id)

            # --- Vectorized SOC mapping ---
            raw_soc_series = safe_col('soc_code')
            # Preserve the raw SOC code (stripped of .XX suffix) for downstream
            # consumers that need the original code even when dim_soc lookup fails.
            soc_code_raw_series = raw_soc_series.apply(
                lambda x: re.sub(r'\.\d+$', '', str(x).strip()) if pd.notna(x) and isinstance(x, str) else None
            )
            soc_code_series = raw_soc_series.apply(_map_soc_vec)
            unmapped_soc.update(
                raw_soc_series[
                    raw_soc_series.notna() & soc_code_series.isna()
                ].astype(str).unique()
            )

            # --- Vectorized area mapping ---
            raw_area_series = safe_col('worksite_area')
            area_code_series = raw_area_series.apply(_map_area_vec)
            unmapped_area.update(
                raw_area_series[
                    raw_area_series.notna() & area_code_series.isna()
                ].astype(str).unique()
            )

            # --- Vectorized country mapping ---
            raw_country_series = safe_col('employer_country')
            country_series = raw_country_series.apply(_map_country_vec)
            unmapped_country.update(
                raw_country_series[
                    raw_country_series.notna() & country_series.isna()
                ].astype(str).unique()
            )

            # --- Dates ---
            received_date_series = pd.to_datetime(safe_col('received_date'), errors='coerce')
            decision_date_series = pd.to_datetime(safe_col('decision_date'), errors='coerce')

            # --- Build chunk DataFrame ---
            chunk_df = pd.DataFrame({
                'case_number':    safe_col('case_number'),
                'case_status':    safe_col('case_status').astype(str).str.strip().str.upper(),
                'received_date':  received_date_series,
                'decision_date':  decision_date_series,
                'employer_id':    employer_id_series,
                'employer_name':  safe_col('employer_name').astype(str).str.strip(),
                'soc_code':       soc_code_series,
                'soc_code_raw':   soc_code_raw_series,
                'area_code':      area_code_series,
                'employer_country': country_series,
                'job_title':      safe_col('job_title'),
                'wage_offer_from': pd.to_numeric(safe_col('wage_from'), errors='coerce'),
                'wage_offer_to':  pd.to_numeric(safe_col('wage_to'), errors='coerce'),
                'wage_offer_unit': safe_col('wage_unit'),
                'worksite_city':  safe_col('worksite_city'),
                'worksite_state': safe_col('worksite_state'),
                'worksite_postal': safe_col('worksite_postal').astype(str),
                'is_fulltime':    safe_col('is_fulltime').astype(str).str.strip().str.upper() == 'Y',
                'naics_code':     safe_col('naics_code'),
                # *** Key fix: force fiscal_year from directory, not from received_date ***
                'fiscal_year':    fy,
                'source_file':    f"PERM/PERM/FY{fy}/{file_path.name}",
                'ingested_at':    datetime.now(timezone.utc),
            })

            all_dfs.append(chunk_df)
            total_processed += len(df)
            print(f"    Processed {len(df)} rows from FY{fy} (fiscal_year forced={fy})")
            
        except Exception as e:
            print(f"    ERROR processing {file_path.name}: {e}")
            continue
    
    # Build final DataFrame
    if not all_dfs:
        print("  No rows to write")
        return
    
    result_df = pd.concat(all_dfs, ignore_index=True)
    
    print(f"\n  Processed {total_processed} total rows")
    print(f"  Built {len(result_df)} fact_perm records")
    
    # Validation
    print(f"\n  Validation:")
    print(f"    Unique case_numbers: {result_df['case_number'].nunique()}")
    print(f"    Non-null employer_id: {result_df['employer_id'].notna().sum()}")
    print(f"    Non-null soc_code: {result_df['soc_code'].notna().sum()} ({result_df['soc_code'].notna().mean()*100:.1f}%)")
    print(f"    Non-null soc_code_raw: {result_df['soc_code_raw'].notna().sum()} ({result_df['soc_code_raw'].notna().mean()*100:.1f}%)")
    print(f"    Non-null area_code: {result_df['area_code'].notna().sum()}")
    print(f"    Non-null employer_country: {result_df['employer_country'].notna().sum()} ({result_df['employer_country'].notna().mean()*100:.1f}%)")
    print(f"    Non-null job_title: {result_df['job_title'].notna().sum()} ({result_df['job_title'].notna().mean()*100:.1f}%)")
    print(f"    Non-null wage_offer_from: {result_df['wage_offer_from'].notna().sum()} ({result_df['wage_offer_from'].notna().mean()*100:.1f}%)")
    print(f"    Non-null worksite_city: {result_df['worksite_city'].notna().sum()} ({result_df['worksite_city'].notna().mean()*100:.1f}%)")
    print(f"    Non-null worksite_state: {result_df['worksite_state'].notna().sum()} ({result_df['worksite_state'].notna().mean()*100:.1f}%)")
    print(f"    Non-null received_date: {result_df['received_date'].notna().sum()} ({result_df['received_date'].notna().mean()*100:.1f}%)")
    print(f"    Non-null naics_code: {result_df['naics_code'].notna().sum()} ({result_df['naics_code'].notna().mean()*100:.1f}%)")

    fy_dist = result_df['fiscal_year'].value_counts().sort_index()
    print(f"\n  fiscal_year distribution ({len(fy_dist)} partitions):")
    for fyr, cnt in fy_dist.items():
        print(f"    FY{fyr}: {cnt:,}")
    zero_rows = (result_df['fiscal_year'] == 0).sum()
    if zero_rows:
        print(f"  WARNING: {zero_rows} rows still have fiscal_year=0 (unexpected after directory fix)")
    else:
        print(f"  ✓ No fiscal_year=0 rows")

    if unmapped_soc:
        print(f"    Unmapped SOC codes: {len(unmapped_soc)} (samples: {list(unmapped_soc)[:5]})")
    if unmapped_area:
        print(f"    Unmapped area codes: {len(unmapped_area)} (samples: {list(unmapped_area)[:5]})")
    if unmapped_country:
        print(f"    Unmapped countries: {len(unmapped_country)} (samples: {list(unmapped_country)[:5]})")

    # Convert object columns to string to handle mixed types
    print("\n  Converting object columns to string type for parquet compatibility...")
    object_cols = result_df.select_dtypes(include=['object']).columns.tolist()
    # Keep date columns as-is
    skip_cols = {'received_date', 'decision_date', 'ingested_at'}
    for col in object_cols:
        if col not in skip_cols:
            result_df[col] = result_df[col].astype(str)

    # Write Hive-partitioned parquet: write each FY partition separately
    # so we ONLY overwrite the FY partitions present in result_df (safe for partial rebuilds)
    output_path.mkdir(parents=True, exist_ok=True)
    written_fys = []
    for fy_val, fy_df in result_df.groupby('fiscal_year'):
        fy_dir = output_path / f"fiscal_year={fy_val}"
        fy_dir.mkdir(parents=True, exist_ok=True)
        fy_parquet = fy_dir / "part-0.parquet"
        fy_df_write = fy_df.drop(columns=['fiscal_year'])
        fy_df_write.to_parquet(fy_parquet, index=False, engine='pyarrow')
        written_fys.append(fy_val)
    print(f"  Written (partitioned): {output_path}/fiscal_year=YYYY/part-0.parquet")
    print(f"  Partitions written: {sorted(written_fys)}")
    print(f"  Total rows: {len(result_df):,}")
    
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
