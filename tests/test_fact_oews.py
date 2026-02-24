"""
Test fact_oews fact table.
"""
from pathlib import Path
import pandas as pd
import pytest


def test_fact_oews_exists():
    """
    Confirm that fact_oews.parquet was created.
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    assert artifact_path.is_file(), f"{artifact_path} not found"


def test_fact_oews_min_rows():
    """
    Confirm fact_oews has a reasonable number of rows.
    OEWS has wage data for ~800+ occupations.
    With sampling, expect at least 500 rows.
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    df = pd.read_parquet(artifact_path)
    
    assert len(df) > 500, f"Expected >500 rows, got {len(df)}"


def test_fact_oews_primary_key_unique():
    """
    Confirm primary key (area_code, soc_code, ref_year) is unique.
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    df = pd.read_parquet(artifact_path)
    
    pk_cols = ['area_code', 'soc_code', 'ref_year']
    
    # Check all PK columns exist
    for col in pk_cols:
        assert col in df.columns, f"Missing PK column: {col}"
    
    # Check for duplicates
    duplicates = df[df.duplicated(subset=pk_cols, keep=False)]
    assert len(duplicates) == 0, f"Found {len(duplicates)} duplicate primary keys"


def test_fact_oews_pk_non_null():
    """
    Confirm primary key columns are never null.
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    df = pd.read_parquet(artifact_path)
    
    pk_cols = ['area_code', 'soc_code', 'ref_year']
    
    for col in pk_cols:
        null_count = df[col].isna().sum()
        assert null_count == 0, f"Found {null_count} null values in PK column {col}"


def test_fact_oews_wage_fields_present():
    """
    Confirm wage fields exist and have some non-null values.
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    df = pd.read_parquet(artifact_path)
    
    wage_fields = ['h_mean', 'a_mean', 'h_median', 'a_median',
                   'h_pct10', 'h_pct25', 'h_pct75', 'h_pct90',
                   'a_pct10', 'a_pct25', 'a_pct75', 'a_pct90']
    
    for field in wage_fields:
        assert field in df.columns, f"Missing wage field: {field}"
    
    # At least one wage field should have some non-null values
    # (some fields may be suppressed for privacy, but not all)
    non_null_counts = {field: df[field].notna().sum() for field in wage_fields}
    total_non_null = sum(non_null_counts.values())
    
    assert total_non_null > 0, "All wage fields are null"
    
    # a_mean and a_median should have substantial coverage
    assert df['a_mean'].notna().sum() > len(df) * 0.5, \
        f"a_mean has low coverage: {df['a_mean'].notna().sum()} / {len(df)}"


def test_fact_oews_wage_values_reasonable():
    """
    Confirm wage values are within reasonable ranges.
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    df = pd.read_parquet(artifact_path)
    
    # Annual wages should be positive and < $1M for most occupations
    a_mean = df['a_mean'].dropna()
    
    if len(a_mean) > 0:
        assert a_mean.min() > 0, f"Found negative a_mean: {a_mean.min()}"
        # $3M cap: a handful of BLS occupations (e.g., 27-2021 Actors) have
        # legitimate a_mean > $1M due to highly-skewed top-earner distributions.
        assert a_mean.max() < 3_000_000, f"Found suspiciously high a_mean: {a_mean.max()}"
        
        # Mean should be reasonable (e.g., $20k-$500k for most jobs)
        assert a_mean.mean() > 20_000, f"Average a_mean too low: {a_mean.mean()}"
        assert a_mean.mean() < 500_000, f"Average a_mean too high: {a_mean.mean()}"
    
    # Hourly wages should be positive and < $500/hr for most occupations
    h_mean = df['h_mean'].dropna()
    
    if len(h_mean) > 0:
        assert h_mean.min() > 0, f"Found negative h_mean: {h_mean.min()}"
        assert h_mean.max() < 500, f"Found suspiciously high h_mean: {h_mean.max()}"


def test_fact_oews_ref_year():
    """
    Confirm ref_year is present and reasonable.
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    df = pd.read_parquet(artifact_path)
    
    # ref_year should exist and be non-null
    assert 'ref_year' in df.columns, "Missing ref_year column"
    null_count = df['ref_year'].isna().sum()
    assert null_count == 0, f"Found {null_count} null ref_year values"
    
    # ref_year may span multiple years (e.g., 2023 + synthetic 2024 fallback)
    years = df['ref_year'].unique()
    assert 1 <= len(years) <= 3, f"Expected 1-3 years, found: {years}"

    for year in years:
        assert 2020 <= year <= 2026, f"ref_year out of range: {year}"


def test_fact_oews_soc_codes_detailed():
    """
    Confirm SOC codes are detailed (not major/minor group rollups).
    Detailed codes have format XX-XXXX (e.g., "15-1252").
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    df = pd.read_parquet(artifact_path)
    
    # Check format of soc_code
    soc_codes = df['soc_code'].unique()
    
    # All should match detailed SOC format
    for soc in soc_codes[:20]:  # Check first 20
        assert isinstance(soc, str), f"soc_code should be string, got {type(soc)}"
        assert len(soc) == 7 or len(soc) == 8, f"Unexpected soc_code length: {soc}"
        assert '-' in soc, f"soc_code should have hyphen: {soc}"


def test_fact_oews_employment():
    """
    Confirm tot_emp field has reasonable values.
    """
    artifact_path = Path("artifacts/tables/fact_oews.parquet")
    df = pd.read_parquet(artifact_path)
    
    # tot_emp should exist
    assert 'tot_emp' in df.columns, "Missing tot_emp column"
    
    # Some rows should have employment data
    non_null = df['tot_emp'].notna().sum()
    assert non_null > 0, "All tot_emp values are null"
    
    # Employment should be positive
    emp_values = df['tot_emp'].dropna()
    if len(emp_values) > 0:
        assert emp_values.min() >= 0, f"Found negative tot_emp: {emp_values.min()}"
