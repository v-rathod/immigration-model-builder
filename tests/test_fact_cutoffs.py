"""Tests for fact_cutoffs table from Visa Bulletin PDFs."""

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


@pytest.mark.slow_integration
def test_fact_cutoffs_loader_creates_directory():
    """Test that running the curate pipeline creates fact_cutoffs directory."""
    # Run the curate pipeline
    result = subprocess.run(
        [sys.executable, "-m", "src.curate.run_curate", "--paths", "configs/paths.yaml"],
        capture_output=True,
        text=True,
        cwd=Path.cwd()
    )
    
    # Should complete successfully
    assert result.returncode == 0, (
        f"Curate pipeline failed with return code {result.returncode}\n"
        f"STDOUT:\n{result.stdout}\n"
        f"STDERR:\n{result.stderr}"
    )
    
    # Check directory exists
    fact_cutoffs_dir = Path("artifacts/tables/fact_cutoffs")
    assert fact_cutoffs_dir.exists(), f"fact_cutoffs directory not created at {fact_cutoffs_dir}"
    assert fact_cutoffs_dir.is_dir(), "fact_cutoffs is not a directory"
    
    print("✓ fact_cutoffs directory created")


def test_fact_cutoffs_has_data():
    """Test that fact_cutoffs contains parquet files with data."""
    fact_cutoffs_dir = Path("artifacts/tables/fact_cutoffs")
    
    if not fact_cutoffs_dir.exists():
        pytest.skip("fact_cutoffs directory not found - run curate pipeline first")
    
    # Read as partitioned dataset so bulletin_year/month partition keys are included
    try:
        combined_df = pd.read_parquet(fact_cutoffs_dir)
    except Exception:
        pytest.skip("No parquet data in fact_cutoffs directory")

    if combined_df.empty:
        pytest.skip("No data in fact_cutoffs - this is OK for MVP if no PDFs were processed")
    
    print(f"Loaded {len(combined_df)} rows from fact_cutoffs")

    # Check required columns exist
    required_cols = [
        'bulletin_year', 'bulletin_month', 'chart', 'category',
        'country', 'cutoff_date', 'status_flag', 'source_file',
        'page_ref', 'ingested_at'
    ]
    for col in required_cols:
        assert col in combined_df.columns, f"Missing required column: {col}"

    # Check data types (partition columns may come back as categorical)
    by = combined_df['bulletin_year']
    bm = combined_df['bulletin_month']
    assert pd.api.types.is_integer_dtype(by) or pd.api.types.is_categorical_dtype(by), \
        f"bulletin_year should be int or category, got {by.dtype}"
    assert pd.api.types.is_integer_dtype(bm) or pd.api.types.is_categorical_dtype(bm), \
        f"bulletin_month should be int or category, got {bm.dtype}"
    assert pd.api.types.is_string_dtype(combined_df['chart']), "chart should be string"
    assert pd.api.types.is_string_dtype(combined_df['category']), "category should be string"
    assert pd.api.types.is_string_dtype(combined_df['status_flag']), "status_flag should be string"

    # Check status_flag values
    valid_status_flags = {'C', 'U', 'D'}
    assert combined_df['status_flag'].isin(valid_status_flags).all(), \
        f"Invalid status_flag values found: {combined_df['status_flag'].unique()}"

    # Check chart values
    valid_charts = {'FAD', 'DFF'}
    assert combined_df['chart'].isin(valid_charts).all(), \
        f"Invalid chart values found: {combined_df['chart'].unique()}"

    print(f"✓ fact_cutoffs validated: {len(combined_df)} rows")
    print(f"  Years: {sorted(combined_df['bulletin_year'].unique())}")
    print(f"  Months: {sorted(combined_df['bulletin_month'].unique())}")
    print(f"  Categories: {sorted(combined_df['category'].unique())}")
    print(f"  Charts: {sorted(combined_df['chart'].unique())}")


def test_fact_cutoffs_partitioning():
    """Test that fact_cutoffs is properly partitioned by year and month."""
    fact_cutoffs_dir = Path("artifacts/tables/fact_cutoffs")
    
    if not fact_cutoffs_dir.exists():
        pytest.skip("fact_cutoffs directory not found")
    
    # Check for year partitions
    year_dirs = list(fact_cutoffs_dir.glob("year=*"))
    
    if len(year_dirs) == 0:
        pytest.skip("No partitions found - this is OK for MVP if no data was extracted")
    
    print(f"Found {len(year_dirs)} year partition(s)")
    
    # Check each year has month partitions
    for year_dir in year_dirs:
        month_dirs = list(year_dir.glob("month=*"))
        assert len(month_dirs) > 0, f"No month partitions found in {year_dir}"
        print(f"  {year_dir.name}: {len(month_dirs)} month(s)")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
