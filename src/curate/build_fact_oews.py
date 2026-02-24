"""
Build fact_oews: OEWS wage percentiles by occupation and area.
"""
import pandas as pd
import zipfile
from pathlib import Path
from datetime import datetime, timezone
import re
import sys


def find_all_oews_files(data_root: Path) -> list:
    """
    Find ALL OEWS data files, sorted by year descending.
    Returns: list of (year, file_path) tuples
    """
    oews_base = data_root / "BLS_OEWS"
    if not oews_base.exists():
        print(f"  WARNING: OEWS directory not found: {oews_base}")
        return []
    
    found_files = []
    
    # Find year directories
    for year_dir in oews_base.iterdir():
        if not year_dir.is_dir():
            continue
        
        # Extract year from directory name
        year_match = re.search(r'(\d{4})', year_dir.name)
        if not year_match:
            continue
        
        year = int(year_match.group(1))
        
        # Look for both .zip and .xlsx files
        for pattern in ["oews_all_data_*.zip", "oews_all_data_*.xlsx"]:
            for data_file in year_dir.glob(pattern):
                found_files.append((year, data_file))
    
    # Sort by year descending
    found_files.sort(reverse=True, key=lambda x: x[0])
    
    return found_files


def load_dimensions(artifacts_path: Path) -> dict:
    """Load dimension tables for FK validation."""
    dims = {}
    
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
    
    return dims


def parse_wage(value) -> float:
    """
    Parse wage value from OEWS data.
    Handle special values like '#', '*', 'N/A', etc.
    Returns None if value cannot be parsed.
    """
    if pd.isna(value):
        return None
    
    # Convert to string and clean
    value_str = str(value).strip()
    
    # Special values indicate suppressed/unavailable data
    if value_str in ['#', '*', '**', 'N/A', '', 'nan']:
        return None
    
    # Try to parse as float
    try:
        return float(value_str.replace(',', ''))
    except (ValueError, AttributeError):
        return None


def read_oews_data(oews_file: Path, sample_size: int = None) -> pd.DataFrame:
    """
    Read OEWS data from .zip or .xlsx file.
    Handles both formats with in-stream processing.
    """
    file_ext = oews_file.suffix.lower()
    
    try:
        if file_ext == '.zip':
            # Read from zip (in-stream)
            with zipfile.ZipFile(oews_file, 'r') as zf:
                # Find Excel file inside zip
                excel_files = [f for f in zf.namelist() if f.endswith('.xlsx')]
                if not excel_files:
                    print(f"  WARN: No .xlsx file found in {oews_file}: skipping")
                    return None
                
                excel_file = excel_files[0]
                print(f"  Reading from zip: {excel_file}")
                
                # Read with optional sampling
                if sample_size:
                    df = pd.read_excel(zf.open(excel_file), nrows=sample_size)
                else:
                    df = pd.read_excel(zf.open(excel_file))
        
        elif file_ext == '.xlsx':
            # Read directly from xlsx
            print(f"  Reading xlsx: {oews_file.name}")
            if sample_size:
                df = pd.read_excel(oews_file, nrows=sample_size)
            else:
                df = pd.read_excel(oews_file)
        
        else:
            print(f"  WARN: Unsupported file format {file_ext} for {oews_file}: skipping")
            return None
        
        return df
    
    except Exception as e:
        print(f"  WARN: Cannot read {oews_file}: {e} — skipping")
        return None


def build_fact_oews(
    data_root: Path,
    output_path: Path,
    artifacts_path: Path,
    sample_size: int = None,
    dry_run: bool = False
):
    """
    Build fact_oews from OEWS wage data.
    Processes all available OEWS years and writes partitioned parquet by ref_year.
    
    Args:
        data_root: Root directory containing BLS_OEWS files
        output_path: Base path for partitioned output (will write to output_path/ref_year=YYYY/)
        artifacts_path: Path to artifacts root (for loading dimensions)
        sample_size: Optional row limit for sampling per year
        dry_run: If True, discover files only without writing outputs
    """
    print("[BUILD FACT_OEWS" + (" - DRY RUN]" if dry_run else "]"))
    
    # Find all OEWS files
    oews_files = find_all_oews_files(data_root)
    if not oews_files:
        print("  No OEWS files found")
        return
    
    print(f"  Found {len(oews_files)} OEWS file(s):")
    for year, file_path in oews_files:
        rel_path = file_path.relative_to(data_root) if data_root in file_path.parents else file_path
        print(f"    Year {year}: {rel_path}")
    
    if sample_size:
        print(f"  Sample size: {sample_size} rows per year")
    else:
        print(f"  Sample size: Full files")
    
    # Discover partitions
    years = [year for year, _ in oews_files]
    print(f"\n  Planned partitions (ref_year): {', '.join(map(str, years))}")
    
    if dry_run:
        # Convert output_path to directory for partitioned format
        if output_path.suffix == '.parquet':
            output_dir = output_path.parent / output_path.stem
        else:
            output_dir = output_path
        print(f"\n  DRY RUN: Would write partitioned parquet to {output_dir}/ref_year=YYYY/")
        print("  No files were created.")
        return
    
    # Real run - proceed with loading and processing
    print("\n  Loading dimensions for FK lookups...")
    
    # Load dimension tables for validation
    dims = load_dimensions(artifacts_path)
    
    # Prepare metrics log
    metrics_dir = artifacts_path / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    metrics_log = metrics_dir / "fact_oews_metrics.log"
    
    with open(metrics_log, 'w') as log:
        log.write(f"OEWS Processing Metrics - {datetime.now()}\n")
        log.write("=" * 60 + "\n\n")
        
        # Process each OEWS file
        total_rows = 0
        year_stats = {}
        
        for year, oews_file in oews_files:
            print(f"\n  Processing OEWS {year}...")
            log.write(f"Year {year}: {oews_file.name}\n")
            
            # Read data
            df = read_oews_data(oews_file, sample_size)
            if df is None:
                log.write(f"  WARN: Failed to read {oews_file.name} — skipped\n\n")
                continue
            
            print(f"    Loaded {len(df)} rows")
            log.write(f"  Loaded: {len(df)} rows\n")
            
            # Process this year's data
            result_df, stats = process_oews_year(df, year, oews_file, dims, data_root, log)
            
            if result_df is None or len(result_df) == 0:
                print(f"    No data to write for {year}")
                log.write(f"  No data written\n\n")
                continue
            
            # Write partitioned parquet
            # Convert single-file path to partitioned directory
            if output_path.suffix == '.parquet':
                output_dir = output_path.parent / output_path.stem
            else:
                output_dir = output_path
            
            partition_dir = output_dir / f"ref_year={year}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            
            partition_file = partition_dir / "data.parquet"
            
            # Remove ref_year from dataframe (it's in the partition path)
            df_to_write = result_df.drop(columns=['ref_year'])
            df_to_write.to_parquet(partition_file, index=False, engine='pyarrow')
            
            print(f"    Written: {partition_file}")
            print(f"    Rows: {len(result_df)}")
            log.write(f"  Written: {len(result_df)} rows to {partition_file}\n")
            
            total_rows += len(result_df)
            year_stats[year] = stats
        
        # Summary
        log.write("\n" + "=" * 60 + "\n")
        log.write("SUMMARY\n")
        log.write(f"Total rows written: {total_rows}\n")
        log.write(f"Years processed: {len(year_stats)}\n")
        
        for year, stats in sorted(year_stats.items()):
            log.write(f"\nYear {year}:\n")
            log.write(f"  Rows: {stats['rows']}\n")
            log.write(f"  Unique areas: {stats['unique_areas']}\n")
            log.write(f"  Unique SOCs: {stats['unique_socs']}\n")
            log.write(f"  Unmapped SOCs: {stats['unmapped_socs']}\n")
            log.write(f"  Unmapped areas: {stats['unmapped_areas']}\n")
            log.write(f"  Hourly→Annual conversions: {stats['hourly_conversions']}\n")
    
    print(f"\n  Metrics log written: {metrics_log}")
    print(f"  Total rows: {total_rows}")


def process_oews_year(df: pd.DataFrame, year: int, oews_file: Path, dims: dict, data_root: Path, log) -> tuple:
    """
    Process OEWS data for a single year.
    Returns: (result_df, stats_dict) or (None, None) if processing fails
    """
    # Column mapping
    col_map = {
        'area_code': 'AREA',
        'soc_code': 'OCC_CODE',
        'tot_emp': 'TOT_EMP',
        'h_mean': 'H_MEAN',
        'a_mean': 'A_MEAN',
        'h_median': 'H_MEDIAN',
        'a_median': 'A_MEDIAN',
        'h_pct10': 'H_PCT10',
        'h_pct25': 'H_PCT25',
        'h_pct75': 'H_PCT75',
        'h_pct90': 'H_PCT90',
        'a_pct10': 'A_PCT10',
        'a_pct25': 'A_PCT25',
        'a_pct75': 'A_PCT75',
        'a_pct90': 'A_PCT90',
    }
    
    # Check for required columns
    missing = [k for k, v in col_map.items() if v not in df.columns]
    if missing:
        print(f"    ERROR: Missing columns: {[col_map[k] for k in missing]}")
        log.write(f"  ERROR: Missing columns: {[col_map[k] for k in missing]}\n")
        return None, None
    
    # Filter to cross-industry, all-ownership data (I_GROUP)
    if 'I_GROUP' in df.columns:
        df_filtered = df[df['I_GROUP'] == 'cross-industry'].copy()
        print(f"    Filtered to cross-industry: {len(df_filtered)} rows")
    else:
        df_filtered = df.copy()
        print(f"    No I_GROUP column, using all rows")
    
    # Filter to detailed occupation codes only (O_GROUP='detailed')
    if 'O_GROUP' in df_filtered.columns:
        df_filtered = df_filtered[df_filtered['O_GROUP'] == 'detailed'].copy()
        print(f"    Filtered to detailed occupations: {len(df_filtered)} rows")
    
    # Filter to detailed SOC code format (XX-XXXX)
    df_filtered = df_filtered[df_filtered[col_map['soc_code']].str.match(r'^\d{2}-\d{4}$', na=False)].copy()
    print(f"    Filtered to detailed SOC format: {len(df_filtered)} rows")
    
    # Build fact rows
    all_rows = []
    unmapped_soc = set()
    unmapped_area = set()
    hourly_conversions = 0
    
    for idx, row in df_filtered.iterrows():
        area_code = str(row[col_map['area_code']]).strip()
        soc_code = str(row[col_map['soc_code']]).strip()
        
        # Validate FKs
        if soc_code not in dims['soc']['soc_code'].values:
            unmapped_soc.add(soc_code)
        
        if area_code not in dims['area']['area_code'].values:
            unmapped_area.add(area_code)
        
        # Parse wages
        h_mean = parse_wage(row.get(col_map['h_mean']))
        a_mean = parse_wage(row.get(col_map['a_mean']))
        h_median = parse_wage(row.get(col_map['h_median']))
        a_median = parse_wage(row.get(col_map['a_median']))
        
        # Convert hourly to annual if annual is missing but hourly is present
        if a_mean is None and h_mean is not None:
            a_mean = h_mean * 2080
            hourly_conversions += 1
        
        if a_median is None and h_median is not None:
            a_median = h_median * 2080
            hourly_conversions += 1
        
        # Parse percentiles
        h_pct10 = parse_wage(row.get(col_map['h_pct10']))
        a_pct10 = parse_wage(row.get(col_map['a_pct10']))
        if a_pct10 is None and h_pct10 is not None:
            a_pct10 = h_pct10 * 2080
            hourly_conversions += 1
        
        h_pct25 = parse_wage(row.get(col_map['h_pct25']))
        a_pct25 = parse_wage(row.get(col_map['a_pct25']))
        if a_pct25 is None and h_pct25 is not None:
            a_pct25 = h_pct25 * 2080
            hourly_conversions += 1
        
        h_pct75 = parse_wage(row.get(col_map['h_pct75']))
        a_pct75 = parse_wage(row.get(col_map['a_pct75']))
        if a_pct75 is None and h_pct75 is not None:
            a_pct75 = h_pct75 * 2080
            hourly_conversions += 1
        
        h_pct90 = parse_wage(row.get(col_map['h_pct90']))
        a_pct90 = parse_wage(row.get(col_map['a_pct90']))
        if a_pct90 is None and h_pct90 is not None:
            a_pct90 = h_pct90 * 2080
            hourly_conversions += 1
        
        fact_row = {
            'area_code': area_code,
            'soc_code': soc_code,
            'ref_year': year,
            'tot_emp': pd.to_numeric(row.get(col_map['tot_emp']), errors='coerce'),
            'h_mean': h_mean,
            'a_mean': a_mean,
            'h_median': h_median,
            'a_median': a_median,
            'h_pct10': h_pct10,
            'h_pct25': h_pct25,
            'h_pct75': h_pct75,
            'h_pct90': h_pct90,
            'a_pct10': a_pct10,
            'a_pct25': a_pct25,
            'a_pct75': a_pct75,
            'a_pct90': a_pct90,
            'source_file': f"BLS_OEWS/{year}/{oews_file.name}",
            'ingested_at': datetime.now(timezone.utc),
        }
        
        all_rows.append(fact_row)
    
    if not all_rows:
        return None, None
    
    result_df = pd.DataFrame(all_rows)
    
    # Statistics
    stats = {
        'rows': len(result_df),
        'unique_areas': result_df['area_code'].nunique(),
        'unique_socs': result_df['soc_code'].nunique(),
        'unmapped_socs': len(unmapped_soc),
        'unmapped_areas': len(unmapped_area),
        'hourly_conversions': hourly_conversions,
    }
    
    print(f"    Built {len(result_df)} records")
    print(f"    Unique areas: {stats['unique_areas']}, Unique SOCs: {stats['unique_socs']}")
    print(f"    Unmapped SOCs: {stats['unmapped_socs']}, Unmapped areas: {stats['unmapped_areas']}")
    if hourly_conversions > 0:
        print(f"    Hourly→Annual conversions: {hourly_conversions}")
    
    log.write(f"  Processed: {len(result_df)} records\n")
    log.write(f"  Unique areas: {stats['unique_areas']}, SOCs: {stats['unique_socs']}\n")
    log.write(f"  Unmapped SOCs: {stats['unmapped_socs']}, areas: {stats['unmapped_areas']}\n")
    if hourly_conversions > 0:
        log.write(f"  Hourly→Annual conversions: {hourly_conversions}\n")
    
    if unmapped_soc:
        log.write(f"  Unmapped SOC samples: {list(unmapped_soc)[:10]}\n")
    if unmapped_area:
        log.write(f"  Unmapped area samples: {list(unmapped_area)[:10]}\n")
    
    # Check for duplicate PKs
    pk_cols = ['area_code', 'soc_code', 'ref_year']
    duplicates = result_df[result_df.duplicated(subset=pk_cols, keep=False)]
    if len(duplicates) > 0:
        print(f"    WARNING: {len(duplicates)} duplicate primary keys")
        log.write(f"  WARNING: {len(duplicates)} duplicate PKs\n")
    
    return result_df, stats


if __name__ == "__main__":
    # Standalone test mode
    import sys
    if len(sys.argv) < 2:
        print("Usage: python build_fact_oews.py <data_root>")
        sys.exit(1)
    
    data_root = Path(sys.argv[1])
    artifacts_path = Path("artifacts")
    output_path = artifacts_path / "tables" / "fact_oews.parquet"
    
    build_fact_oews(data_root, output_path, artifacts_path, sample_size=10000)
