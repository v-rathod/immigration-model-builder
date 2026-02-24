"""
Build dim_soc: SOC 2018 normalized occupation dimension.

Follows adaptive parsing rules:
- Read layout registry from configs/layouts/soc.yml
- Apply header aliases for column name variations
- Extract hierarchy (major/minor/broad groups) using patterns
- Graceful degradation for missing titles or malformed codes
- Log unmapped/ambiguous entries to artifacts/metrics/
"""

import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import yaml


def load_soc_layout(layouts_dir: str = "configs/layouts") -> dict:
    """Load SOC layout registry with header aliases and extraction rules."""
    layout_path = Path(layouts_dir) / "soc.yml"
    if not layout_path.exists():
        raise FileNotFoundError(f"SOC layout registry not found: {layout_path}")
    
    with open(layout_path, 'r') as f:
        return yaml.safe_load(f)


def resolve_header(df: pd.DataFrame, canonical_name: str, aliases: List[str]) -> Optional[str]:
    """
    Find actual column name in dataframe using alias list.
    
    Returns:
        Actual column name if found, None otherwise
    """
    # Check exact matches first (case-sensitive)
    for alias in aliases:
        if alias in df.columns:
            return alias
    
    # Try case-insensitive match
    df_cols_lower = {col.lower(): col for col in df.columns}
    for alias in aliases:
        if alias.lower() in df_cols_lower:
            return df_cols_lower[alias.lower()]
    
    return None


def normalize_soc_code(code: str, layout: dict) -> Optional[str]:
    """
    Normalize SOC code to standard XX-XXXX format.
    
    Handles:
    - Standard format: 15-1252 (pass through)
    - No hyphen: 151252 (insert hyphen)
    - Padding: strips/pads as needed
    
    Returns normalized code or None if invalid.
    """
    if pd.isna(code):
        return None
    
    code = str(code).strip()
    
    # Check standard format
    if re.match(r'^\d{2}-\d{4}$', code):
        return code
    
    # Check no-hyphen format (6 consecutive digits)
    if re.match(r'^\d{6}$', code):
        return f"{code[:2]}-{code[2:]}"
    
    # Try removing extra hyphens/spaces
    cleaned = code.replace(' ', '').replace('-', '')
    if re.match(r'^\d{6}$', cleaned):
        return f"{cleaned[:2]}-{cleaned[2:]}"
    
    return None


def extract_hierarchy(soc_code: str, layout: dict) -> Dict[str, str]:
    """
    Extract major/minor/broad groups from SOC code using hierarchy rules.
    
    Returns dict with major_group, minor_group, broad_group.
    """
    hierarchy = {}
    
    if not soc_code or not re.match(r'^\d{2}-\d{4}$', soc_code):
        return {'major_group': None, 'minor_group': None, 'broad_group': None}
    
    # Major group: first 2 digits
    hierarchy['major_group'] = soc_code[:2]
    
    # Minor group: XX-XX (first 5 chars)
    hierarchy['minor_group'] = soc_code[:5]
    
    # Broad group: XX-XXX (first 6 chars)
    hierarchy['broad_group'] = soc_code[:6]
    
    return hierarchy


def determine_mapping_confidence(row: pd.Series, all_codes: pd.DataFrame, layout: dict) -> Tuple[str, bool]:
    """
    Determine mapping confidence and aggregation status.
    
    Returns:
        (confidence_level, is_aggregated)
    
    Logic:
    - deterministic: 1-to-1 mapping (one 2010 → one 2018)
    - one-to-many: one 2010 → multiple 2018 codes
    - many-to-one: multiple 2010 → one 2018 (aggregated)
    - manual-review: notes indicate review needed
    """
    soc_2018 = row.get('soc_2018_code')
    soc_2010 = row.get('soc_2010_code')
    notes = str(row.get('notes', '')).lower()
    
    # Check for manual review indicators
    if any(keyword in notes for keyword in ['review', 'complex', 'manual', 'unclear']):
        return ('manual-review', False)
    
    # Check for aggregation indicators
    if any(keyword in notes for keyword in ['merged', 'consolidat', 'aggregat', 'combined']):
        # Count how many 2010 codes map to this 2018 code
        target_count = sum(all_codes['soc_2018_code'] == soc_2018)
        return ('many-to-one', target_count > 1)
    
    # Check for splitting
    if any(keyword in notes for keyword in ['split', 'divided', 'separated']):
        return ('one-to-many', False)
    
    # Check if this 2010 code appears multiple times (splits into multiple 2018)
    if soc_2010:
        source_occurrences = sum(all_codes['soc_2010_code'] == soc_2010)
        if source_occurrences > 1:
            return ('one-to-many', False)
    
    # Check if this 2018 code appears multiple times (aggregates from multiple 2010)
    if soc_2018:
        target_occurrences = sum(all_codes['soc_2018_code'] == soc_2018)
        if target_occurrences > 1:
            return ('many-to-one', True)
    
    # Default: deterministic 1-to-1
    return ('deterministic', False)


def build_dim_soc(data_root: str, output_path: str, schemas_path: str = "configs/schemas.yml") -> str:
    """
    Build dim_soc dimension from OEWS 2023 all-data file (primary) +
    SOC 2010→2018 crosswalk (supplementary provenance overlay).

    Primary source: BLS_OEWS/2023/oews_all_data_2023.zip → oesm23all/all_data_M_2023.xlsx
      Produces one row per unique OCC_CODE (1,396 codes across all O_GROUP levels).
    Crosswalk: Codebooks/soc_crosswalk_2010_to_2018.csv
      Overlays from_version / from_code / mapping_confidence for the small subset it covers.

    Args:
        data_root: Path to P1 downloads
        output_path: Output path for parquet file
        schemas_path: Path to schemas.yml for validation

    Returns:
        Path to written parquet file
    """
    print("[BUILD DIM_SOC]")

    # Load layout registry (kept for normalize_soc_code / extract_hierarchy helpers)
    layout = load_soc_layout()

    ingested_at = datetime.now(timezone.utc)
    warnings = []

    # ── PRIMARY: OEWS 2023 all-data ───────────────────────────────────────────
    oews_zip_path = Path(data_root) / "BLS_OEWS" / "2023" / "oews_all_data_2023.zip"
    if not oews_zip_path.exists():
        raise FileNotFoundError(f"OEWS 2023 zip not found: {oews_zip_path}")

    print(f"  Reading OEWS 2023: {oews_zip_path}")
    import zipfile, io
    with zipfile.ZipFile(oews_zip_path, 'r') as zf:
        # Find the all-data xlsx inside the zip
        xlsx_names = [n for n in zf.namelist() if n.endswith('.xlsx') and 'all_data_M' in n]
        if not xlsx_names:
            raise FileNotFoundError(f"Cannot find all_data_M*.xlsx in {oews_zip_path}")
        xlsx_name = xlsx_names[0]
        print(f"  Reading member: {xlsx_name}")
        with zf.open(xlsx_name) as xf:
            oews_df = pd.read_excel(io.BytesIO(xf.read()), dtype=str)

    print(f"  OEWS raw rows: {len(oews_df):,}")

    # Resolve column names (O_GROUP confirmed; OCC_CODE / OCC_TITLE may vary capitalization)
    col_map = {c.upper(): c for c in oews_df.columns}
    occ_code_col = col_map.get('OCC_CODE')
    occ_title_col = col_map.get('OCC_TITLE')
    o_group_col   = col_map.get('O_GROUP')

    if not occ_code_col:
        raise ValueError(f"Cannot find OCC_CODE column. Available: {list(oews_df.columns)[:10]}")

    # Deduplicate: one row per OCC_CODE (prefer 'detailed' O_GROUP for title accuracy)
    oews_df[occ_code_col] = oews_df[occ_code_col].str.strip()
    if o_group_col:
        oews_df[o_group_col] = oews_df[o_group_col].str.strip().str.lower()
        # Sort so 'detailed' rows come first → drop_duplicates keeps them
        group_order = {'detailed': 0, 'broad': 1, 'minor': 2, 'major': 3, 'total': 4}
        oews_df['_sort'] = oews_df[o_group_col].map(group_order).fillna(5)
        oews_df = oews_df.sort_values('_sort').drop(columns='_sort')

    unique_oews = oews_df.drop_duplicates(subset=[occ_code_col], keep='first').copy()
    print(f"  Unique OCC_CODEs: {len(unique_oews)}")

    # ── Build records from OEWS ───────────────────────────────────────────────
    records = []
    for _, row in unique_oews.iterrows():
        raw_code = row[occ_code_col]
        soc_code = normalize_soc_code(raw_code, layout)
        if not soc_code:
            warnings.append(f"Could not normalize OCC_CODE '{raw_code}' — skipping")
            continue

        soc_title = str(row[occ_title_col]).strip() if occ_title_col and not pd.isna(row.get(occ_title_col)) else f"SOC {soc_code}"
        o_group_val = str(row[o_group_col]).strip() if o_group_col and not pd.isna(row.get(o_group_col)) else None

        hierarchy = extract_hierarchy(soc_code, layout)

        records.append({
            'soc_code':           soc_code,
            'soc_title':          soc_title,
            'soc_version':        '2018',
            'soc_major_group':    hierarchy['major_group'],
            'soc_minor_group':    hierarchy['minor_group'],
            'soc_broad_group':    hierarchy['broad_group'],
            'from_version':       None,
            'from_code':          None,
            'mapping_confidence': 'deterministic',
            'is_aggregated':      False,
            'source_file':        f'BLS_OEWS/2023/oews_all_data_2023.zip/{xlsx_name}',
            'ingested_at':        ingested_at,
        })

    result_df = pd.DataFrame(records)
    print(f"  Built {len(result_df)} records from OEWS")

    # ── SUPPLEMENTARY: crosswalk overlay ─────────────────────────────────────
    crosswalk_path = Path(data_root) / "Codebooks" / "soc_crosswalk_2010_to_2018.csv"
    if crosswalk_path.exists():
        cw = pd.read_csv(crosswalk_path)
        print(f"  Crosswalk: {len(cw)} rows — overlaying provenance")
        aliases = layout['header_aliases']
        col_2018_code  = resolve_header(cw, 'soc_2018_code', aliases.get('soc_code', []))
        col_2010_code  = resolve_header(cw, 'soc_2010_code', aliases.get('soc_2010_code', []))

        if col_2018_code and col_2010_code:
            for _, row in cw.iterrows():
                soc_2018 = normalize_soc_code(row.get(col_2018_code), layout)
                soc_2010 = row.get(col_2010_code)
                if not soc_2018:
                    continue
                mask = result_df['soc_code'] == soc_2018
                if mask.any():
                    from_v = '2010' if soc_2010 and not pd.isna(soc_2010) else None
                    from_c = str(soc_2010).strip() if from_v else None
                    confidence, is_agg = determine_mapping_confidence(row, cw, layout)
                    result_df.loc[mask, 'from_version']       = from_v
                    result_df.loc[mask, 'from_code']          = from_c
                    result_df.loc[mask, 'mapping_confidence'] = confidence
                    result_df.loc[mask, 'is_aggregated']      = is_agg
    else:
        print(f"  WARNING: Crosswalk not found at {crosswalk_path} — skipping overlay")

    # ── Deduplicate / validate ────────────────────────────────────────────────
    dupes = result_df.duplicated(subset=['soc_code'], keep=False)
    if dupes.any():
        dup_codes = result_df.loc[dupes, 'soc_code'].unique()
        warnings.append(f"Found {len(dup_codes)} duplicate SOC codes — keeping first")
        result_df = result_df.drop_duplicates(subset=['soc_code'], keep='first')

    result_df = result_df.dropna(subset=['soc_code'])
    print(f"  Final: {len(result_df)} unique SOC codes")

    # ── Log warnings ──────────────────────────────────────────────────────────
    if warnings:
        print(f"  WARNINGS ({len(warnings)} total):")
        for w in warnings[:5]:
            print(f"    - {w}")
        if len(warnings) > 5:
            print(f"    ... and {len(warnings) - 5} more")
        metrics_dir = Path("artifacts/metrics")
        metrics_dir.mkdir(parents=True, exist_ok=True)
        with open(metrics_dir / "dim_soc_warnings.log", 'w') as f:
            f.write(f"dim_soc build warnings — {ingested_at.isoformat()}\n")
            f.write(f"Total: {len(warnings)}\n\n")
            for w in warnings:
                f.write(f"{w}\n")

    # ── Write output ──────────────────────────────────────────────────────────
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    result_df.to_parquet(out_path, index=False)

    print(f"  Written: {out_path}")
    print(f"  Rows: {len(result_df)}")
    print(f"  O_GROUP breakdown: {unique_oews[o_group_col].value_counts().to_dict() if o_group_col else 'N/A'}")

    return str(out_path)


if __name__ == "__main__":
    # Standalone test
    import sys
    from src.io.readers import load_paths_config
    
    paths = load_paths_config("configs/paths.yaml")
    data_root = paths.get("data_root")
    artifacts_root = paths.get("artifacts_root", "./artifacts")
    
    output_path = Path(artifacts_root) / "tables" / "dim_soc.parquet"
    
    result = build_dim_soc(data_root, str(output_path))
    print(f"\n✓ Built dim_soc at {result}")
