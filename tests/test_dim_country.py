"""Tests for dim_country dimension table."""

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


@pytest.mark.slow_integration
def test_dim_country_builder_creates_file():
    """Test that running the curate pipeline creates dim_country.parquet."""
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
    
    # Check file exists
    dim_country_path = Path("artifacts/tables/dim_country.parquet")
    assert dim_country_path.exists(), f"dim_country.parquet not created at {dim_country_path}"
    
    # Load and verify content
    df = pd.read_parquet(dim_country_path)
    
    # Should have rows
    assert len(df) > 0, "dim_country.parquet is empty"
    
    # Should have required columns
    required_cols = ['country_name', 'iso2', 'iso3', 'region', 'source_file', 'ingested_at']
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"
    
    # Should have no null iso3 (primary key)
    assert df['iso3'].notna().all(), "Found null values in iso3 (primary key)"
    
    # Should have unique iso3
    assert df['iso3'].is_unique, "Found duplicate iso3 values"
    
    # iso codes should be uppercase
    assert df['iso2'].str.isupper().all(), "iso2 codes not uppercase"
    assert df['iso3'].str.isupper().all(), "iso3 codes not uppercase"
    
    print(f"✓ dim_country validated: {len(df)} countries")


def test_dim_country_schema():
    """Test that dim_country has correct schema and data types."""
    dim_country_path = Path("artifacts/tables/dim_country.parquet")
    
    if not dim_country_path.exists():
        pytest.skip("dim_country.parquet not found - run curate pipeline first")
    
    df = pd.read_parquet(dim_country_path)
    
    # Check data types (string columns can be object or string dtype)
    assert pd.api.types.is_string_dtype(df['country_name']), "country_name should be string"
    assert pd.api.types.is_string_dtype(df['iso2']), "iso2 should be string"
    assert pd.api.types.is_string_dtype(df['iso3']), "iso3 should be string"
    assert pd.api.types.is_string_dtype(df['source_file']), "source_file should be string"
    
    # ingested_at should be datetime
    assert pd.api.types.is_datetime64_any_dtype(df['ingested_at']), "ingested_at should be datetime"
    
    # Check expected countries exist
    expected_countries = ['IND', 'CHN', 'MEX', 'PHL', 'BRA']
    for iso3 in expected_countries:
        assert iso3 in df['iso3'].values, f"Expected country {iso3} not found"
    
    print(f"✓ dim_country schema validated")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
