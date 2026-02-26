"""
Build dim_employer: Canonical employer identities dimension.

Follows adaptive parsing rules:
- First tries to read from fact_perm (if already built) - fast parquet read
- Falls back to P1 PERM Excel files if fact_perm not available
- Extract employer names from PERM data
- Normalize names using suffix/punctuation removal rules
- Generate stable employer_id via SHA1 hash
- Track aliases (raw variants) per canonical name
"""

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set
from collections import defaultdict

import pandas as pd
import yaml


def load_employer_layout(layouts_dir: str = "configs/layouts") -> dict:
    """Load employer layout registry with normalization rules."""
    layout_path = Path(layouts_dir) / "employer.yml"
    if not layout_path.exists():
        raise FileNotFoundError(f"Employer layout registry not found: {layout_path}")
    
    with open(layout_path, 'r') as f:
        return yaml.safe_load(f)


def normalize_employer_name(raw_name: str, layout: dict) -> str:
    """
    Normalize employer name using layout rules.
    
    Pipeline:
    1. Lowercase
    2. Strip punctuation
    3. Remove legal suffixes
    4. Collapse whitespace
    5. Strip leading/trailing whitespace
    
    Returns lowercase normalized form (for hashing)
    """
    if not raw_name or pd.isna(raw_name):
        return ""
    
    # Step 1: Lowercase
    normalized = str(raw_name).lower().strip()
    
    # Step 2: Strip punctuation
    for punct in layout.get('punctuation_to_strip', []):
        normalized = normalized.replace(punct, ' ')
    
    # Step 3: Remove legal suffixes (word boundary)
    suffixes = layout.get('suffixes', [])
    for suffix in suffixes:
        # Use regex with word boundary to avoid partial matches
        pattern = r'\b' + re.escape(suffix.lower()) + r'\b'
        normalized = re.sub(pattern, '', normalized)
    
    # Step 4: Collapse whitespace
    normalized = re.sub(r'\s+', ' ', normalized)
    
    # Step 5: Strip
    normalized = normalized.strip()
    
    # Validate minimum length
    min_len = layout.get('min_len', 3)
    if len(normalized) < min_len:
        return ""
    
    return normalized


def compute_employer_id(normalized_name: str) -> str:
    """Generate stable employer_id from normalized name using SHA1."""
    if not normalized_name:
        return ""
    return hashlib.sha1(normalized_name.encode('utf-8')).hexdigest()


def title_case_name(normalized_name: str) -> str:
    """Convert normalized name to Title Case for display."""
    if not normalized_name:
        return ""
    # Title case each word
    return ' '.join(word.capitalize() for word in normalized_name.split())


def resolve_employer_column(df: pd.DataFrame, layout: dict) -> Optional[str]:
    """Find employer name column using aliases."""
    aliases = layout.get('aliases', {}).get('employer_name', [])
    
    # Check exact matches first
    for alias in aliases:
        if alias in df.columns:
            return alias
    
    # Check case-insensitive
    df_cols_lower = {col.lower(): col for col in df.columns}
    for alias in aliases:
        if alias.lower() in df_cols_lower:
            return df_cols_lower[alias.lower()]
    
    return None


def find_perm_files(data_root: str, max_years: Optional[int] = None) -> List[tuple]:
    """
    Find PERM disclosure files from fiscal years.
    
    Args:
        data_root: Path to P1 downloads
        max_years: Max number of recent FYs to read (None = all)
    
    Returns:
        List of (fy, file_path) tuples
    """
    perm_base = Path(data_root) / "PERM" / "PERM"
    if not perm_base.exists():
        perm_base = Path(data_root) / "PERM"
    
    if not perm_base.exists():
        return []
    
    # Find FY directories
    fy_dirs = sorted([d for d in perm_base.iterdir() if d.is_dir() and d.name.startswith('FY')], 
                     reverse=True)
    
    if max_years is not None:
        fy_dirs = fy_dirs[:max_years]
    
    files = []
    for fy_dir in fy_dirs:
        # Extract FY year
        fy_match = re.match(r'FY(\d{4})', fy_dir.name)
        if not fy_match:
            continue
        fy_year = int(fy_match.group(1))
        
        # Look for xlsx files
        xlsx_files = list(fy_dir.glob("PERM_Disclosure_Data_*.xlsx"))
        if xlsx_files:
            files.append((fy_year, xlsx_files[0]))
    
    return files


def _build_from_fact_perm(fact_perm_path: Path, layout: dict) -> dict:
    """
    Extract employer data from already-built fact_perm parquet.
    
    Returns:
        Dictionary of {normalized_name: {'aliases': set, 'source_files': set}}
    """
    employer_groups = defaultdict(lambda: {'aliases': set(), 'source_files': set()})
    
    # Read fact_perm (can be directory or flat file)
    if fact_perm_path.is_dir():
        df = pd.read_parquet(fact_perm_path)
    else:
        df = pd.read_parquet(fact_perm_path)
    
    print(f"    Loaded {len(df):,} rows from fact_perm")
    
    # Extract employer names - try multiple column names
    emp_cols = ['employer_name', 'employer_name_raw', 'EMPLOYER_NAME']
    emp_col = None
    for col in emp_cols:
        if col in df.columns:
            emp_col = col
            break
    
    if not emp_col:
        raise ValueError(f"No employer column found in fact_perm. Available: {list(df.columns)[:10]}")
    
    # Get unique raw names (fast - just unique values)
    raw_names = df[emp_col].dropna().unique()
    print(f"    Found {len(raw_names):,} unique raw employer names")
    
    # Process each raw name (no expensive DataFrame lookup needed)
    for raw_name in raw_names:
        raw_str = str(raw_name).strip()
        if not raw_str:
            continue
        
        # Normalize
        normalized = normalize_employer_name(raw_str, layout)
        if not normalized:
            continue
        
        employer_groups[normalized]['aliases'].add(raw_str)
        employer_groups[normalized]['source_files'].add('fact_perm')
    
    return employer_groups


def build_dim_employer(data_root: str, out_path: str, schemas_path: str = "configs/schemas.yml", 
                      layout_path: str = "configs/layouts/employer.yml",
                      artifacts_root: str = "artifacts") -> str:
    """
    Build dim_employer dimension from PERM data.
    
    Strategy:
    1. If fact_perm already exists → extract from parquet (fast, complete)
    2. Otherwise → read P1 Excel files (slower, used for bootstrapping)
    
    Args:
        data_root: Path to P1 downloads
        out_path: Output path for parquet file
        schemas_path: Path to schemas.yml (for validation)
        layout_path: Path to employer layout registry
        artifacts_root: Path to artifacts directory (to check for existing fact_perm)
    
    Returns:
        Path to written parquet file
    """
    print("[BUILD DIM_EMPLOYER]")
    
    # Load layout registry
    layout = load_employer_layout(Path(layout_path).parent)
    
    # Check if fact_perm already exists (prefer parquet read over Excel)
    fact_perm_paths = [
        Path(artifacts_root) / "tables" / "fact_perm_all.parquet",
        Path(artifacts_root) / "tables" / "fact_perm",
    ]
    
    fact_perm_path = None
    for path in fact_perm_paths:
        if path.exists():
            fact_perm_path = path
            break
    
    employer_groups = defaultdict(lambda: {'aliases': set(), 'source_files': set()})
    warnings = []
    
    if fact_perm_path:
        # Fast path: extract from existing fact_perm parquet
        print(f"  Using existing fact_perm: {fact_perm_path}")
        try:
            employer_groups = _build_from_fact_perm(fact_perm_path, layout)
            print(f"  Extracted {len(employer_groups):,} unique normalized employers from fact_perm")
        except Exception as e:
            warnings.append(f"Failed to read fact_perm: {e}")
            print(f"  WARNING: Failed to read fact_perm: {e}")
            print(f"  Falling back to P1 Excel files...")
            fact_perm_path = None  # Fall through to Excel path
    
    if not fact_perm_path:
        # Slow path: read P1 Excel files (all FYs, no row limit)
        perm_files = find_perm_files(data_root, max_years=None)  # Read ALL FYs
        
        if not perm_files:
            print(f"  WARNING: No PERM files found in {data_root}/PERM/")
            print(f"  Creating empty placeholder")
            out_file = Path(out_path)
            out_file.parent.mkdir(parents=True, exist_ok=True)
            empty_df = pd.DataFrame(columns=[
                'employer_id', 'employer_name', 'aliases', 'domain', 'source_files', 'ingested_at'
            ])
            empty_df.to_parquet(out_file, index=False)
            return str(out_file)
        
        print(f"  Found {len(perm_files)} PERM file(s) from all FYs")
        for fy, fpath in perm_files:
            print(f"    FY{fy}: {fpath.name}")
        
        # Aggregate employer names across files
        total_rows = 0
        
        for fy, perm_file in perm_files:
            print(f"\n  Processing FY{fy}...")
            
            try:
                # Read entire Excel file (no row limit for complete dim_employer)
                df = pd.read_excel(perm_file)
                print(f"    Loaded {len(df):,} rows")
                
                # Resolve employer column
                emp_col = resolve_employer_column(df, layout)
                if not emp_col:
                    warnings.append(f"FY{fy}: Could not resolve employer column. Available: {list(df.columns)[:10]}")
                    print(f"    WARNING: Could not find employer column")
                    continue
                
                print(f"    Employer column: '{emp_col}'")
                
                # Extract and normalize
                employers = df[emp_col].dropna().unique()
                print(f"    Found {len(employers)} unique raw employer names")
                
                for raw_name in employers:
                    raw_str = str(raw_name).strip()
                    if not raw_str:
                        continue
                    
                    # Normalize
                    normalized = normalize_employer_name(raw_str, layout)
                    if not normalized:
                        continue
                    
                    # Group by normalized
                    employer_groups[normalized]['aliases'].add(raw_str)
                    employer_groups[normalized]['source_files'].add(f"PERM/FY{fy}/{perm_file.name}")
                
                total_rows += len(df)
            
            except Exception as e:
                warnings.append(f"FY{fy}: Failed to process - {str(e)}")
                print(f"    ERROR: {e}")
                continue
        
        print(f"\n  Processed {total_rows:,} total rows from Excel")
        print(f"  Found {len(employer_groups):,} unique normalized employers")
    
    # Build canonical records
    records = []
    ingested_at = datetime.now(timezone.utc)
    
    for normalized_name, data in employer_groups.items():
        # Compute employer_id
        employer_id = compute_employer_id(normalized_name)
        
        # Title case for display
        employer_name = title_case_name(normalized_name)
        
        # Convert aliases to JSON array
        aliases_list = sorted(list(data['aliases']))[:20]  # Cap at 20 aliases
        aliases_json = json.dumps(aliases_list)
        
        # Source files
        source_files_str = ','.join(sorted(list(data['source_files'])))
        
        records.append({
            'employer_id': employer_id,
            'employer_name': employer_name,
            'aliases': aliases_json,
            'domain': None,
            'source_files': source_files_str,
            'ingested_at': ingested_at
        })
    
    # Convert to DataFrame
    result_df = pd.DataFrame(records)
    
    print(f"  Built {len(result_df)} employer records")
    
    # Validation
    if not result_df['employer_id'].is_unique:
        dup_count = result_df['employer_id'].duplicated().sum()
        warnings.append(f"Found {dup_count} duplicate employer_ids")
        result_df = result_df.drop_duplicates(subset=['employer_id'], keep='first')
    
    null_names = result_df['employer_name'].isna().sum()
    if null_names > 0:
        warnings.append(f"Found {null_names} null employer_names")
        result_df = result_df.dropna(subset=['employer_name'])
    
    print(f"  Validated: {len(result_df)} unique employers")
    
    # Log top alias groups
    metrics_dir = Path("artifacts/metrics")
    metrics_dir.mkdir(parents=True, exist_ok=True)
    
    with open(metrics_dir / "employer_alias_sample.log", 'w') as f:
        f.write(f"dim_employer top alias groups - {ingested_at.isoformat()}\\n")
        f.write(f"Total employers: {len(result_df)}\\n")
        f.write(f"Total raw names processed: {sum(len(json.loads(row['aliases'])) for _, row in result_df.iterrows())}\\n\\n")
        f.write("Top 50 employers by alias count:\\n")
        f.write("-" * 80 + "\\n")
        
        # Sort by alias count
        result_df['_alias_count'] = result_df['aliases'].apply(lambda x: len(json.loads(x)))
        top_50 = result_df.nlargest(50, '_alias_count')
        
        for idx, row in top_50.iterrows():
            aliases_list = json.loads(row['aliases'])
            f.write(f"\\nCanonical: {row['employer_name']}\\n")
            f.write(f"ID: {row['employer_id'][:16]}...\\n")
            f.write(f"Aliases ({len(aliases_list)}): {aliases_list[:5]}\\n")
    
    print(f"  Logged top-50 alias groups to artifacts/metrics/employer_alias_sample.log")
    
    # Log warnings
    if warnings:
        print(f"  WARNINGS ({len(warnings)} total):")
        for warning in warnings[:5]:
            print(f"    - {warning}")
        
        with open(metrics_dir / "dim_employer_warnings.log", 'w') as f:
            f.write(f"dim_employer build warnings - {ingested_at.isoformat()}\\n\\n")
            for warning in warnings:
                f.write(f"{warning}\\n")
    
    # Write output
    out_file = Path(out_path)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    result_df = result_df.drop(columns=['_alias_count'], errors='ignore')
    result_df.to_parquet(out_file, index=False)
    
    print(f"  Written: {out_file}")
    print(f"  Rows: {len(result_df)}")
    
    return str(out_file)


if __name__ == "__main__":
    # Standalone test
    from src.io.readers import load_paths_config
    
    paths = load_paths_config("configs/paths.yaml")
    data_root = paths.get("data_root")
    artifacts_root = paths.get("artifacts_root", "./artifacts")
    
    output_path = Path(artifacts_root) / "tables" / "dim_employer.parquet"
    
    result = build_dim_employer(data_root, str(output_path))
    print(f"\\n✓ Built dim_employer at {result}")
