"""BLS OEWS wage data loader."""

from pathlib import Path
from typing import List
import pandas as pd


def load_oews(files: List[Path], data_root: str, out_path: Path) -> None:
    """Load BLS OEWS wage benchmarks.
    
    Args:
        files: List of OEWS Excel files
        data_root: Root directory of raw data
        out_path: Output parquet file path
        
    TODO:
        - Read OEWS all-data Excel files
        - Extract SOC code, area (MSA/state/national), wage percentiles
        - Filter to relevant occupations (tech, professional)
        - Output: year, soc_code, area_code, area_name, 
                  p10_wage, p25_wage, median_wage, p75_wage, p90_wage,
                  mean_wage, employment_count
    """
    print(f"[OEWS LOADER]")
    print(f"  Input: {len(files)} OEWS files from {data_root}")
    print(f"  Output: {out_path}")
    print(f"  TODO: Parse OEWS Excel, extract wage percentiles by SOC/area")
    
    # Create placeholder empty DataFrame with expected schema
    df_placeholder = pd.DataFrame(columns=[
        "year",
        "soc_code",
        "area_code",
        "area_name",
        "p10_wage",
        "p25_wage",
        "median_wage",
        "p75_wage",
        "p90_wage",
        "mean_wage",
        "employment_count",
    ])
    
    df_placeholder.to_parquet(out_path, index=False)
    print(f"  Created placeholder: {out_path}")
