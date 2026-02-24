"""
Build dim_visa_class: Employment-based visa families and subcategories dimension.

Follows adaptive parsing rules:
- Read codebook from downloads/Codebooks/eb_subcategory_codes.csv
- Normalize headers (case/underscore tolerant)
- Validate family codes (EB1..EB5)
- Handle null sub_codes for family-level rows
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import List, Dict, Optional
import re

import pandas as pd


def normalize_header(header: str) -> str:
    """Normalize header to lowercase with underscores."""
    return header.strip().lower().replace(' ', '_').replace('-', '_')


def normalize_family_code(code: str) -> str:
    """
    Normalize family code to canonical format (EB1..EB5).
    
    Accepts: EB-1, EB1, EB 1, etc.
    Returns: EB1
    """
    if not code:
        return code
    
    # Remove spaces and hyphens, uppercase
    normalized = str(code).upper().replace(' ', '').replace('-', '')
    
    # Extract EB + digit
    match = re.match(r'(EB)(\d)', normalized)
    if match:
        return match.group(1) + match.group(2)
    
    return normalized


def normalize_sub_code(code: str, family: str) -> Optional[str]:
    """
    Normalize subcategory code.
    
    Examples:
    - EB-1A → EB1A
    - EB-2 NIW → EB2-NIW
    - EB-2 (when family is EB2) → EB2 (keep as-is, not null)
    """
    if not code or pd.isna(code):
        return None
    
    code_str = str(code).strip()
    
    # Normalize spacing and hyphens
    # EB-1A → EB1A
    # EB-2 NIW → EB2-NIW
    normalized = code_str.upper().replace(' ', '-')
    
    # Remove hyphen between EB and digit
    normalized = re.sub(r'EB-(\d)', r'EB\1', normalized)
    
    # Remove trailing hyphen if present
    normalized = normalized.rstrip('-')
    
    return normalized if normalized else None


def derive_family_name(family_code: str) -> str:
    """Derive human-friendly family name from code."""
    family_names = {
        'EB1': 'Employment-Based First Preference',
        'EB2': 'Employment-Based Second Preference',
        'EB3': 'Employment-Based Third Preference',
        'EB4': 'Employment-Based Fourth Preference',
        'EB5': 'Employment-Based Fifth Preference'
    }
    return family_names.get(family_code, f'{family_code} (Unknown)')


def build_dim_visa_class(data_root: str, out_path: str, schemas_path: str = "configs/schemas.yml") -> str:
    """
    Build dim_visa_class dimension from EB subcategory codebook.
    
    Args:
        data_root: Path to P1 downloads
        out_path: Output path for parquet file
        schemas_path: Path to schemas.yml (for validation)
    
    Returns:
        Path to written parquet file
    """
    print("[BUILD DIM_VISA_CLASS]")
    
    # Locate codebook
    codebook_path = Path(data_root) / "Codebooks" / "eb_subcategory_codes.csv"
    
    if not codebook_path.exists():
        print(f"  WARNING: Codebook not found: {codebook_path}")
        print(f"  Creating empty placeholder")
        out_file = Path(out_path)
        out_file.parent.mkdir(parents=True, exist_ok=True)
        # Create empty dataframe with schema
        empty_df = pd.DataFrame(columns=[
            'family_code', 'family_name', 'sub_code', 'sub_name',
            'is_employment', 'is_grouped', 'notes', 'source_file', 'ingested_at'
        ])
        empty_df.to_parquet(out_file, index=False)
        return str(out_file)
    
    print(f"  Reading: {codebook_path}")
    
    # Read codebook
    df = pd.read_csv(codebook_path)
    print(f"  Loaded {len(df)} rows from codebook")
    
    # Normalize headers
    df.columns = [normalize_header(col) for col in df.columns]
    
    # Map to expected columns (flexible header names)
    col_map = {}
    for col in df.columns:
        if 'category' in col and 'sub' not in col:
            col_map['eb_category'] = col
        elif 'subcategory' in col or 'sub_code' in col or 'subcode' in col:
            col_map['subcategory_code'] = col
        elif 'description' in col or 'name' in col or 'title' in col:
            col_map['description'] = col
    
    if 'eb_category' not in col_map or 'subcategory_code' not in col_map:
        raise ValueError(f"Could not resolve required columns. Available: {list(df.columns)}")
    
    # Rename to canonical
    df = df.rename(columns={
        col_map['eb_category']: 'eb_category',
        col_map['subcategory_code']: 'subcategory_code',
        col_map.get('description', 'description'): 'description'
    })
    
    print(f"  Resolved columns: eb_category, subcategory_code, description")
    
    # Build canonical records
    records = []
    warnings = []
    ingested_at = datetime.now(timezone.utc)
    
    for idx, row in df.iterrows():
        # Normalize family code
        family_raw = str(row['eb_category']).strip()
        family_code = normalize_family_code(family_raw)
        
        # Validate family code
        if not re.match(r'^EB[1-5]$', family_code):
            warnings.append(f"Row {idx}: Invalid family_code '{family_code}' (raw: '{family_raw}')")
            continue
        
        # Derive family name
        family_name = derive_family_name(family_code)
        
        # Normalize sub_code
        sub_code_raw = row.get('subcategory_code', None)
        sub_code = normalize_sub_code(sub_code_raw, family_code)
        
        # Get sub_name from description
        sub_name = None
        if 'description' in row and not pd.isna(row['description']):
            sub_name = str(row['description']).strip()
        
        # Set flags
        is_employment = True  # All EB categories are employment-based
        is_grouped = sub_code is not None  # Subcategories roll up into family metrics
        
        # Notes
        notes = None
        if sub_code and 'NIW' in str(sub_code_raw).upper():
            notes = "National Interest Waiver - subset of EB2"
        
        records.append({
            'family_code': family_code,
            'family_name': family_name,
            'sub_code': sub_code,
            'sub_name': sub_name,
            'is_employment': is_employment,
            'is_grouped': is_grouped,
            'notes': notes,
            'source_file': f"Codebooks/{codebook_path.name}",
            'ingested_at': ingested_at
        })
    
    # Convert to DataFrame
    result_df = pd.DataFrame(records)
    
    print(f"  Built {len(result_df)} visa class records")
    
    # Validation
    # Check for null family_code
    null_family = result_df['family_code'].isna().sum()
    if null_family > 0:
        warnings.append(f"Found {null_family} rows with null family_code")
        result_df = result_df.dropna(subset=['family_code'])
    
    # Check PK uniqueness (treating null sub_code as empty string for uniqueness check only)
    result_df['_pk_check'] = result_df['family_code'] + '||' + result_df['sub_code'].fillna('')
    if not result_df['_pk_check'].is_unique:
        dup_count = result_df['_pk_check'].duplicated().sum()
        warnings.append(f"Found {dup_count} duplicate (family_code, sub_code) pairs - keeping first")
        result_df = result_df.drop_duplicates(subset=['_pk_check'], keep='first')
    result_df = result_df.drop(columns=['_pk_check'])
    
    # Validate family_code values
    valid_families = {'EB1', 'EB2', 'EB3', 'EB4', 'EB5'}
    invalid_families = result_df[~result_df['family_code'].isin(valid_families)]
    if len(invalid_families) > 0:
        warnings.append(f"Found {len(invalid_families)} rows with invalid family_code: {invalid_families['family_code'].unique()}")
        result_df = result_df[result_df['family_code'].isin(valid_families)]
    
    print(f"  Validated: {len(result_df)} visa classes")
    
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
        with open(metrics_dir / "dim_visa_class_warnings.log", 'w') as f:
            f.write(f"dim_visa_class build warnings - {ingested_at.isoformat()}\n")
            f.write(f"Total warnings: {len(warnings)}\n\n")
            for warning in warnings:
                f.write(f"{warning}\n")
    
    # Write output
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_parquet(out_file, index=False)
    
    print(f"  Written: {out_file}")
    print(f"  Rows: {len(result_df)}")
    print(f"  Columns: {list(result_df.columns)}")
    
    # Summary stats
    print(f"\n  Summary:")
    print(f"    Families: {result_df['family_code'].nunique()} unique")
    print(f"    Distribution:")
    for family, count in result_df['family_code'].value_counts().items():
        print(f"      {family}: {count}")
    print(f"    With subcategories: {result_df['sub_code'].notna().sum()}")
    
    return str(out_file)


if __name__ == "__main__":
    # Standalone test
    import sys
    from src.io.readers import load_paths_config
    
    paths = load_paths_config("configs/paths.yaml")
    data_root = paths.get("data_root")
    artifacts_root = paths.get("artifacts_root", "./artifacts")
    
    output_path = Path(artifacts_root) / "tables" / "dim_visa_class.parquet"
    
    result = build_dim_visa_class(data_root, str(output_path))
    print(f"\n✓ Built dim_visa_class at {result}")
