"""PERM disclosure data loader."""

from pathlib import Path
from typing import List
import pandas as pd


def load_perm(files: List[Path], data_root: str, out_path: Path) -> None:
    """Load and normalize PERM disclosure data.
    
    Args:
        files: List of PERM Excel/CSV files
        data_root: Root directory of raw data
        out_path: Output parquet file path
        
    TODO:
        - Read PERM quarterly/annual disclosures (Excel format)
        - Apply record layout to parse columns
        - Normalize employer names, SOC codes, geographic areas
        - Standardize case status (Certified, Denied, Withdrawn)
        - Output: case_number, employer_name, employer_name_normalized,
                  soc_code, job_title, wage_offered, prevailing_wage,
                  worksite_state, decision_date, case_status
    """
    print(f"[PERM LOADER]")
    print(f"  Input: {len(files)} PERM files from {data_root}")
    print(f"  Output: {out_path}")
    print(f"  TODO: Parse Excel, apply record layout, normalize employer/SOC")
    
    # Create placeholder empty DataFrame with expected schema
    df_placeholder = pd.DataFrame(columns=[
        "case_number",
        "employer_name",
        "employer_name_normalized",
        "soc_code",
        "job_title",
        "wage_offered",
        "prevailing_wage",
        "worksite_state",
        "decision_date",
        "case_status",
    ])
    
    df_placeholder.to_parquet(out_path, index=False)
    print(f"  Created placeholder: {out_path}")
