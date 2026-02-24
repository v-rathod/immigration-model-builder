"""
Build fact_perm: PERM labor certification outcomes - ALL FYs with chunked processing.
"""
import hashlib
import pandas as pd
import yaml
from pathlib import Path
from datetime import datetime, timezone
import re
import sys

# Import helper functions from original
from build_fact_perm import (
    load_employer_layout, normalize_employer_name, compute_employer_id,
    load_dimensions, map_soc_code, map_area_code, map_country, derive_fy
)


def find_all_perm_files(data_root: Path) -> list:
    """Find ALL PERM Excel files. Returns list of (fy, file_path) tuples."""
    perm_base = data_root / "PERM" / "PERM"
    if not perm_base.exists():
        print(f"  WARNING: PERM directory not found: {perm_base}")
        return []
    
    files = []
    seen = set()
    
    for fy_dir in sorted(perm_base.iterdir(), reverse=True):
        if not fy_dir.is_dir() or not fy_dir.name.startswith("FY"):
            continue
        
        fy_match = re.search(r'FY(\d{4})', fy_dir.name)
        if not fy_match:
            continue
        fy = int(fy_match.group(1))
        
        # Multiple patterns for historical files
        for pattern in ["PERM_Disclosure_Data_*.xlsx", "PERM_FY*.xlsx", "PERM_*.xlsx"]:
            for excel_file in fy_dir.glob(pattern):
                if excel_file not in seen:
                    files.append((fy, excel_file))
                    seen.add(excel_file)
    
    return sorted(files, key=lambda x: x[0], reverse=True)


def process_perm_chunk(df: pd.DataFrame, fy: int, file_path: Path, layout: dict, dims: dict, data_root: Path) -> list:
    """Process a chunk of PERM data. Returns list of fact row dicts."""
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
    
    all_rows = []
    
    for idx, row in df.iterrows():
        def safe_get(col_key):
            col_name = col_map.get(col_key)
            return row.get(col_name) if col_name and col_name in df.columns else None
        
        raw_employer = safe_get('employer_name')
        normalized_employer = normalize_employer_name(raw_employer, layout)
        employer_id = compute_employer_id(normalized_employer)
        
        soc_code = map_soc_code(safe_get('soc_code'), dims['soc'])
        area_code = map_area_code(safe_get('worksite_area'), dims['area'])
        country_iso3 = map_country(safe_get('employer_country'), dims['country'])
        
        received_date = pd.to_datetime(safe_get('received_date'), errors='coerce')
        decision_date = pd.to_datetime(safe_get('decision_date'), errors='coerce')
        fy_derived = derive_fy(received_date)
        
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
    
    return all_rows


def build_fact_perm(
    data_root: Path,
    output_path: Path,
    artifacts_path: Path,
    layouts_path: Path,
    chunk_size: int = 100000,
    dry_run: bool = False
):
    """Build fact_perm from ALL PERM files with chunked processing and partitioned output."""
    print("[BUILD FACT_PERM" + (" - DRY RUN]" if dry_run else "]"))
    
    perm_files = find_all_perm_files(data_root)
    if not perm_files:
        print("  No PERM files found")
        return
    
    print(f"  Found {len(perm_files)} PERM file(s):")
    for fy, fpath in perm_files:
        rel_path = fpath.relative_to(data_root) if data_root in fpath.parents else fpath
        print(f"    FY{fy}: {rel_path}")
    
    print(f"  Chunk size: {chunk_size:,} rows")
    fiscal_years = sorted([fy for fy, _ in perm_files])
    print(f"\n  Planned partitions (fiscal_year): {', '.join(map(str, fiscal_years))}")
    
    if dry_run:
        output_dir = output_path.parent / output_path.stem if output_path.suffix == '.parquet' else output_path
        print(f"\n  DRY RUN: Would write to {output_dir}/fiscal_year=YYYY/")
        print("  No files were created.")
        return
    
    print("\n  Loading dimensions for FK lookups...")
    layout = load_employer_layout(layouts_path)
    dims = load_dimensions(artifacts_path)
    
    metrics_dir = artifacts_path / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_log = metrics_dir / "fact_perm_metrics.log"
    
    output_dir = output_path.parent / output_path.stem if output_path.suffix == '.parquet' else output_path
    
    with open(metrics_log, 'w') as log:
        log.write(f"PERM Processing Metrics - {datetime.now()}\n")
        log.write("=" * 60 + "\n\n")
        
        total_rows = 0
        fy_stats = {}
        
        for fy, file_path in perm_files:
            print(f"\n  Processing FY{fy}...")
            log.write(f"FY{fy}: {file_path.name}\n")
            
            try:
                df_full = pd.read_excel(file_path)
                total_file_rows = len(df_full)
                print(f"    Loaded {total_file_rows:,} rows from Excel")
                log.write(f"  Loaded: {total_file_rows:,} rows\n")
                
                all_rows_for_fy = []
                
                for chunk_start in range(0, total_file_rows, chunk_size):
                    chunk_end = min(chunk_start + chunk_size, total_file_rows)
                    df_chunk = df_full.iloc[chunk_start:chunk_end]
                    
                    if chunk_start > 0:
                        print(f"    Processing chunk [{chunk_start:,}-{chunk_end:,}]")
                    
                    chunk_rows = process_perm_chunk(df_chunk, fy, file_path, layout, dims, data_root)
                    all_rows_for_fy.extend(chunk_rows)
                
                print(f"    Built {len(all_rows_for_fy):,} fact rows")
                log.write(f"  Processed: {len(all_rows_for_fy):,} fact rows\n")
                
                if all_rows_for_fy:
                    result_df = pd.DataFrame(all_rows_for_fy)
                    
                    if 'worksite_postal' in result_df.columns:
                        result_df['worksite_postal'] = result_df['worksite_postal'].astype(str)
                    
                    partition_dir = output_dir / f"fiscal_year={fy}"
                    partition_dir.mkdir(parents=True, exist_ok=True)
                    partition_file = partition_dir / "data.parquet"
                    
                    df_to_write = result_df.drop(columns=['fy'], errors='ignore')
                    df_to_write.to_parquet(partition_file, index=False, engine='pyarrow')
                    
                    print(f"    Written: {partition_file}")
                    print(f"    Rows: {len(result_df):,}")
                    log.write(f"  Written: {len(result_df):,} rows\n")
                    
                    total_rows += len(result_df)
                    fy_stats[fy] = {
                        'rows': len(result_df),
                        'null_employer': result_df['employer_id'].isna().sum(),
                        'null_soc': result_df['soc_code'].isna().sum(),
                        'null_area': result_df['area_code'].isna().sum(),
                    }
                
            except Exception as e:
                print(f"    ERROR: {e}")
                log.write(f"  ERROR: {e}\n")
                import traceback
                traceback.print_exc()
                continue
        
        log.write("\n" + "=" * 60 + "\n")
        log.write("SUMMARY\n")
        log.write(f"Total rows: {total_rows}\n")
        log.write(f"Fiscal years processed: {len(fy_stats)}\n")
        
        for fy, stats in sorted(fy_stats.items()):
            log.write(f"\nFY{fy}:\n")
            log.write(f"  Rows: {stats['rows']}\n")
            pct_emp = 100*stats['null_employer']/stats['rows'] if stats['rows'] > 0 else 0
            pct_soc = 100*stats['null_soc']/stats['rows'] if stats['rows'] > 0 else 0
            pct_area = 100*stats['null_area']/stats['rows'] if stats['rows'] > 0 else 0
            log.write(f"  Null employer_id: {stats['null_employer']} ({pct_emp:.1f}%)\n")
            log.write(f"  Null soc_code: {stats['null_soc']} ({pct_soc:.1f}%)\n")
            log.write(f"  Null area_code: {stats['null_area']} ({pct_area:.1f}%)\n")
    
    print(f"\n  Metrics log: {metrics_log}")
    print(f"  Total rows: {total_rows:,}")
    
    if fy_stats:
        print(f"\n  Rows by Fiscal Year:")
        for fy in sorted(fy_stats.keys()):
            stats = fy_stats[fy]
            pct_emp = 100*stats['null_employer']/stats['rows'] if stats['rows'] > 0 else 0
            pct_soc = 100*stats['null_soc']/stats['rows'] if stats['rows'] > 0 else 0
            print(f"    FY{fy}: {stats['rows']:,} rows (null emp: {pct_emp:.1f}%, null SOC: {pct_soc:.1f}%)")
