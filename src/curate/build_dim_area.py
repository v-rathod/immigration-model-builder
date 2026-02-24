"""
Build dim_area: OEWS geographic areas dimension.

Follows adaptive parsing rules:
- Read layout registry from configs/layouts/area.yml
- Apply header aliases for column name variations
- Use AREA_TYPE code mapping when available
- Derive area_type, metro_status, state info using patterns
- Graceful degradation for missing data
- Log warnings to artifacts/metrics/
"""

import re
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yaml


def load_area_layout(layouts_dir: str = "configs/layouts") -> dict:
    """Load area layout registry with header aliases and classification rules."""
    layout_path = Path(layouts_dir) / "area.yml"
    if not layout_path.exists():
        raise FileNotFoundError(f"Area layout registry not found: {layout_path}")
    
    with open(layout_path, 'r') as f:
        return yaml.safe_load(f)


def resolve_header(df: pd.DataFrame, canonical_name: str, aliases: List[str]) -> Optional[str]:
    """Find actual column name in dataframe using alias list (case-insensitive)."""
    # Check exact matches first
    for alias in aliases:
        if alias in df.columns:
            return alias
    
    # Try case-insensitive match
    df_cols_lower = {col.lower(): col for col in df.columns}
    for alias in aliases:
        if alias.lower() in df_cols_lower:
            return df_cols_lower[alias.lower()]
    
    return None


def find_oews_file(data_root: str, year: int) -> Optional[Tuple[Path, str]]:
    """
    Find OEWS all-data file for given year.
    
    Returns:
        (file_path, file_type) where file_type is 'xlsx' or 'zip', or None if not found
    """
    oews_dir = Path(data_root) / "BLS_OEWS" / str(year)
    if not oews_dir.exists():
        return None
    
    # Look for xlsx or zip files
    for pattern in [f"*all_data*{year}.xlsx", f"*all_data*{year}.zip"]:
        matches = list(oews_dir.glob(pattern))
        if matches:
            file_path = matches[0]
            file_type = 'zip' if file_path.suffix == '.zip' else 'xlsx'
            
            # Validate zip files before returning
            if file_type == 'zip':
                try:
                    with zipfile.ZipFile(file_path, 'r') as zf:
                        # Check if it's a valid zip and contains xlsx
                        xlsx_files = [f for f in zf.namelist() if f.endswith('.xlsx')]
                        if not xlsx_files:
                            print(f"  WARNING: {file_path} is not a valid OEWS zip (no xlsx files)")
                            return None
                except zipfile.BadZipFile:
                    print(f"  WARNING: {file_path} is not a valid zip file (corrupt)")
                    return None
            
            return (file_path, file_type)
    
    return None


def read_oews_file(file_path: Path, file_type: str, columns: List[str]) -> pd.DataFrame:
    """
    Read OEWS file (xlsx or zip containing xlsx).
    
    Args:
        file_path: Path to OEWS file
        file_type: 'xlsx' or 'zip'
        columns: List of columns to read (or None for all)
    
    Returns:
        DataFrame with OEWS data
    """
    if file_type == 'xlsx':
        # Direct Excel read
        return pd.read_excel(file_path, usecols=columns if columns else None)
    
    elif file_type == 'zip':
        # Extract and read from zip
        with zipfile.ZipFile(file_path, 'r') as zf:
            # Find xlsx file inside
            xlsx_files = [f for f in zf.namelist() if f.endswith('.xlsx')]
            if not xlsx_files:
                raise ValueError(f"No xlsx file found in {file_path}")
            
            # Read first xlsx
            with zf.open(xlsx_files[0]) as xlsx_file:
                return pd.read_excel(xlsx_file, usecols=columns if columns else None)
    
    else:
        raise ValueError(f"Unsupported file type: {file_type}")


def classify_area_type(row: pd.Series, layout: dict) -> str:
    """
    Classify area type using AREA_TYPE code or title patterns.
    
    Returns one of: NATIONAL, STATE, TERRITORY, MSA, NONMSA
    """
    # Try AREA_TYPE code first
    area_type_code = str(row.get('AREA_TYPE', '')).strip()
    if area_type_code in layout['oews_area_type_codes']:
        return layout['oews_area_type_codes'][area_type_code]
    
    # Fallback to title-based classification
    title = str(row.get('area_title', '')).lower()
    
    if re.match(layout['patterns']['is_national_title'], title):
        return 'NATIONAL'
    elif re.search(layout['patterns']['contains_nonmetro'], title):
        return 'NONMSA'
    elif re.match(layout['patterns']['msa_with_state_suffix'], title):
        return 'MSA'
    elif title in [s.lower() for s in layout['state_maps']['state_names']]:
        return 'STATE'
    else:
        # Check if it matches territory names
        if title in ['guam', 'puerto rico', 'virgin islands']:
            return 'TERRITORY'
        return 'MSA'  # Default assumption for unknown patterns


def derive_metro_status(area_type: str) -> str:
    """Derive metro_status from area_type."""
    if area_type == 'MSA':
        return 'METRO'
    elif area_type == 'NONMSA':
        return 'NONMETRO'
    else:
        return 'NA'


def derive_state_info(row: pd.Series, area_type: str, layout: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Derive state_abbr and state_fips from PRIM_STATE or title.
    
    Returns:
        (state_abbr, state_fips) tuple, either can be None
    """
    # For NATIONAL, no state info
    if area_type == 'NATIONAL':
        return (None, None)
    
    # Try PRIM_STATE column first
    prim_state = row.get('PRIM_STATE')
    if prim_state and not pd.isna(prim_state):
        prim_state = str(prim_state).strip().upper()
        
        # Check if it's a state abbreviation
        abbr_to_fips = layout['state_maps']['abbr_to_fips']
        if prim_state in abbr_to_fips:
            return (prim_state, abbr_to_fips[prim_state])
        
        # Check territories
        terr_to_fips = layout['state_maps'].get('territory_abbr_to_fips', {})
        if prim_state in terr_to_fips:
            return (prim_state, terr_to_fips[prim_state])
    
    # For STATE area_type, try exact title match
    if area_type == 'STATE':
        title = str(row.get('area_title', '')).strip()
        # Reverse lookup: state name -> abbr -> fips
        state_names = layout['state_maps']['state_names']
        if title in state_names:
            # Find abbreviation (need reverse mapping)
            abbr_to_fips = layout['state_maps']['abbr_to_fips']
            for abbr, fips in abbr_to_fips.items():
                # This is a simplified approach; ideally we'd have name->abbr mapping
                # For now, leave it for manual enrichment
                pass
    
    # For NONMSA, try parsing state name from title (e.g., "Alabama nonmetropolitan area")
    if area_type == 'NONMSA':
        title = str(row.get('area_title', '').strip())
        # Extract state name before "nonmetropolitan"
        match = re.match(r'^([A-Z][a-z\s]+?)\s+nonmetropolitan', title, re.IGNORECASE)
        if match:
            state_name = match.group(1).strip()
            # Check if it's in our state names list
            if state_name in layout['state_maps']['state_names']:
                # Again, need name->abbr mapping (simplified for now)
                pass
    
    # MSAs - leave null (can span multiple states)
    return (None, None)


def build_dim_area(data_root: str, out_path: str, schemas_path: str = "configs/schemas.yml", layout_path: str = "configs/layouts/area.yml") -> str:
    """
    Build dim_area dimension from OEWS all-data files.
    
    Args:
        data_root: Path to P1 downloads
        out_path: Output path for parquet file
        schemas_path: Path to schemas.yml (for validation)
        layout_path: Path to area layout registry
    
    Returns:
        Path to written parquet file
    """
    print("[BUILD DIM_AREA]")
    
    # Load layout registry
    layout = load_area_layout(Path(layout_path).parent)
    
    # Try to find OEWS file (2024 first, then 2023)
    oews_file = None
    ref_year = None
    for year in [2024, 2023]:
        result = find_oews_file(data_root, year)
        if result:
            oews_file, file_type = result
            ref_year = year
            print(f"  Found: {oews_file} (year={ref_year}, type={file_type})")
            break
    
    if not oews_file:
        print(f"  WARNING: No OEWS files found in {data_root}/BLS_OEWS/")
        print(f"  Creating empty placeholder")
        out_file = Path(out_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        # Create empty dataframe with schema
        empty_df = pd.DataFrame(columns=[
            'area_code', 'area_title', 'area_type', 'state_abbr', 'state_fips',
            'cbsa_code', 'metro_status', 'ref_year', 'source_file', 'ingested_at'
        ])
        empty_df.to_parquet(out_file, index=False)
        return str(out_file)
    
    # Read OEWS file - get unique areas
    print(f"  Reading areas from OEWS file...")
    
    # Resolve column names using aliases
    aliases = layout['aliases']
    
    # We need to read the file first to know actual column names
    df = read_oews_file(oews_file, file_type, columns=None)
    
    col_area_code = resolve_header(df, 'area_code', aliases['code'])
    col_area_title = resolve_header(df, 'area_title', aliases['title'])
    col_area_type = resolve_header(df, 'area_type', aliases.get('area_type', []))
    col_prim_state = resolve_header(df, 'prim_state', aliases.get('prim_state', []))
    
    if not col_area_code or not col_area_title:
        raise ValueError(f"Could not resolve area code/title columns. Available: {list(df.columns)[:20]}")
    
    print(f"  Resolved headers: code='{col_area_code}', title='{col_area_title}', type='{col_area_type}', state='{col_prim_state}'")
    
    # Extract unique areas
    area_cols = [col_area_code, col_area_title]
    if col_area_type:
        area_cols.append(col_area_type)
    if col_prim_state:
        area_cols.append(col_prim_state)
    
    areas_df = df[area_cols].drop_duplicates(subset=[col_area_code])
    areas_df = areas_df.dropna(subset=[col_area_code, col_area_title])
    
    print(f"  Loaded {len(areas_df)} unique areas")
    
    # Rename columns to canonical names for processing
    rename_map = {col_area_code: 'area_code', col_area_title: 'area_title'}
    if col_area_type:
        rename_map[col_area_type] = 'AREA_TYPE'
    if col_prim_state:
        rename_map[col_prim_state] = 'PRIM_STATE'
    
    areas_df = areas_df.rename(columns=rename_map)
    
    # Build canonical records
    records = []
    warnings = []
    ingested_at = datetime.now(timezone.utc)
    
    for idx, row in areas_df.iterrows():
        # Ensure area_code is string with leading zeros preserved
        area_code = str(row['area_code']).strip()
        area_title = str(row['area_title']).strip()
        
        # Classify area type
        area_type = classify_area_type(row, layout)
        
        # Derive metro status
        metro_status = derive_metro_status(area_type)
        
        # Derive state info
        state_abbr, state_fips = derive_state_info(row, area_type, layout)
        
        # CBSA code - leave null for now (future enrichment)
        cbsa_code = None
        
        records.append({
            'area_code': area_code,
            'area_title': area_title,
            'area_type': area_type,
            'state_abbr': state_abbr,
            'state_fips': state_fips,
            'cbsa_code': cbsa_code,
            'metro_status': metro_status,
            'ref_year': ref_year,
            'source_file': f"BLS_OEWS/{ref_year}/{oews_file.name}",
            'ingested_at': ingested_at
        })
    
    # Convert to DataFrame
    result_df = pd.DataFrame(records)
    
    print(f"  Built {len(result_df)} area records")
    
    # Validation
    if result_df['area_code'].isna().any():
        null_count = result_df['area_code'].isna().sum()
        warnings.append(f"Found {null_count} null area codes after processing")
        result_df = result_df.dropna(subset=['area_code'])
    
    if not result_df['area_code'].is_unique:
        dup_count = result_df['area_code'].duplicated().sum()
        warnings.append(f"Found {dup_count} duplicate area codes - keeping first occurrence")
        result_df = result_df.drop_duplicates(subset=['area_code'], keep='first')
    
    # Validate area_type enum
    valid_types = set(layout['enums']['area_type'])
    invalid_types = result_df[~result_df['area_type'].isin(valid_types)]
    if len(invalid_types) > 0:
        warnings.append(f"Found {len(invalid_types)} rows with invalid area_type: {invalid_types['area_type'].unique()}")
    
    print(f"  Validated: {len(result_df)} unique areas")
    
    # Log warnings
    if warnings:
        print(f"  WARNINGS ({len(warnings)} total):")
        for warning in warnings[:5]:
            print(f"    - {warning}")
        if len(warnings) > 5:
            print(f"    ... and {len(warnings) - 5} more")
        
        # Write warnings to metrics
        metrics_dir = Path("artifacts/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        with open(metrics_dir / "dim_area_warnings.log", 'w') as f:
            f.write(f"dim_area build warnings - {ingested_at.isoformat()}\\n")
            f.write(f"Total warnings: {len(warnings)}\\n\\n")
            for warning in warnings:
                f.write(f"{warning}\\n")
    
    # Write output
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_parquet(out_file, index=False)
    
    print(f"  Written: {out_file}")
    print(f"  Rows: {len(result_df)}")
    print(f"  Columns: {list(result_df.columns)}")
    
    # Summary stats
    print(f"\\n  Summary:")
    print(f"    Area type distribution:")
    for area_type, count in result_df['area_type'].value_counts().items():
        print(f"      {area_type}: {count}")
    print(f"    Metro status distribution:")
    for metro, count in result_df['metro_status'].value_counts().items():
        print(f"      {metro}: {count}")
    
    return str(out_file)


if __name__ == "__main__":
    # Standalone test
    import sys
    from src.io.readers import load_paths_config
    
    paths = load_paths_config("configs/paths.yaml")
    data_root = paths.get("data_root")
    artifacts_root = paths.get("artifacts_root", "./artifacts")
    
    output_path = Path(artifacts_root) / "tables" / "dim_area.parquet"
    
    result = build_dim_area(data_root, str(output_path))
    print(f"\\n✓ Built dim_area at {result}")
