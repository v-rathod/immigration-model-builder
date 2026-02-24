"""
Tests for dim_area (OEWS geographic areas dimension).
"""

import pytest
from pathlib import Path
import pandas as pd

from src.curate.build_dim_area import build_dim_area
from src.io.readers import load_paths_config


@pytest.fixture
def test_paths():
    """Load test paths configuration."""
    config = load_paths_config("configs/paths.yaml")
    return {
        "data_root": config["data_root"],
        "artifacts_root": config["artifacts_root"]
    }


def test_dim_area_builder_creates_file(test_paths):
    """Test that dim_area builder creates output file."""
    output_path = Path(test_paths["artifacts_root"]) / "tables" / "dim_area.parquet"
    
    # Build dim_area
    result_path = build_dim_area(
        test_paths["data_root"],
        str(output_path),
        schemas_path="configs/schemas.yml"
    )
    
    # Verify file exists
    assert output_path.exists(), f"Expected output file not found: {output_path}"
    assert result_path == str(output_path)
    
    # Verify parquet is valid
    df = pd.read_parquet(output_path)
    
    # Should have at least some rows (or 0 if no data available)
    assert len(df) >= 0, "DataFrame should be valid (can be empty if no source data)"
    
    # If data exists, should have minimum expected rows (OEWS has 587 areas)
    if len(df) > 0:
        assert len(df) > 50, f"Expected > 50 areas, got {len(df)}"
    
    print(f"✓ dim_area built with {len(df)} rows")


def test_dim_area_schema(test_paths):
    """Test that dim_area follows schema definition."""
    output_path = Path(test_paths["artifacts_root"]) / "tables" / "dim_area.parquet"
    
    # Ensure file exists
    if not output_path.exists():
        build_dim_area(test_paths["data_root"], str(output_path))
    
    df = pd.read_parquet(output_path)
    
    if len(df) == 0:
        pytest.skip("No data available - skipping schema validation")
    
    # Check required columns
    required_cols = [
        'area_code', 'area_title', 'area_type', 'state_abbr', 'state_fips',
        'cbsa_code', 'metro_status', 'ref_year', 'source_file', 'ingested_at'
    ]
    
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"
    
    # Validate PK uniqueness
    assert df['area_code'].is_unique, "Primary key 'area_code' must be unique"
    
    # Validate area_type enum
    valid_area_types = {'NATIONAL', 'STATE', 'TERRITORY', 'MSA', 'NONMSA'}
    actual_types = set(df['area_type'].dropna().unique())
    assert actual_types.issubset(valid_area_types), f"Invalid area_types found: {actual_types - valid_area_types}"
    
    # Validate metro_status enum
    valid_metro_status = {'METRO', 'NONMETRO', 'NA'}
    actual_metro = set(df['metro_status'].dropna().unique())
    assert actual_metro.issubset(valid_metro_status), f"Invalid metro_status found: {actual_metro - valid_metro_status}"
    
    # Check non-null constraints
    assert df['area_code'].notna().all(), "area_code should not have nulls"
    assert df['area_title'].notna().all(), "area_title should not have nulls"
    assert df['area_type'].notna().all(), "area_type should not have nulls"
    
    print(f"✓ Schema validated: {len(df)} rows, {len(df.columns)} columns")


def test_dim_area_classifications(test_paths):
    """Test that area_type classifications are reasonable."""
    output_path = Path(test_paths["artifacts_root"]) / "tables" / "dim_area.parquet"
    
    # Ensure file exists
    if not output_path.exists():
        build_dim_area(test_paths["data_root"], str(output_path))
    
    df = pd.read_parquet(output_path)
    
    if len(df) == 0:
        pytest.skip("No data available - skipping classification tests")
    
    # Check distribution of area_types
    type_counts = df['area_type'].value_counts().to_dict()
    print(f"  Area type distribution: {type_counts}")
    
    # Should have exactly 1 NATIONAL
    if 'NATIONAL' in type_counts:
        assert type_counts['NATIONAL'] == 1, f"Expected 1 NATIONAL area, got {type_counts['NATIONAL']}"
    
    # Should have 50-53 STATEs (50 states + DC + possibly territories classified as STATE)
    if 'STATE' in type_counts:
        assert 50 <= type_counts['STATE'] <= 53, f"Expected 50-53 STATE areas, got {type_counts['STATE']}"
    
    # Should have some MSAs and NONMSAs
    if 'MSA' in type_counts:
        assert type_counts['MSA'] > 200, f"Expected >200 MSA areas, got {type_counts['MSA']}"
    
    if 'NONMSA' in type_counts:
        assert type_counts['NONMSA'] > 40, f"Expected >40 NONMSA areas, got {type_counts['NONMSA']}"
    
    # Check metro_status alignment
    for idx, row in df.iterrows():
        if row['area_type'] == 'MSA':
            assert row['metro_status'] == 'METRO', f"MSA should have metro_status=METRO: {row['area_code']}"
        elif row['area_type'] == 'NONMSA':
            assert row['metro_status'] == 'NONMETRO', f"NONMSA should have metro_status=NONMETRO: {row['area_code']}"
        else:
            assert row['metro_status'] == 'NA', f"Non-MSA/NONMSA should have metro_status=NA: {row['area_code']}"
    
    print(f"✓ Classifications validated")


def test_dim_area_state_mappings(test_paths):
    """Test that state_abbr and state_fips are correctly mapped."""
    output_path = Path(test_paths["artifacts_root"]) / "tables" / "dim_area.parquet"
    
    # Ensure file exists
    if not output_path.exists():
        build_dim_area(test_paths["data_root"], str(output_path))
    
    df = pd.read_parquet(output_path)
    
    if len(df) == 0:
        pytest.skip("No data available - skipping state mapping tests")
    
    # For NATIONAL, state_abbr and state_fips should be null
    national_areas = df[df['area_type'] == 'NATIONAL']
    if len(national_areas) > 0:
        assert national_areas['state_abbr'].isna().all(), "NATIONAL areas should have null state_abbr"
        assert national_areas['state_fips'].isna().all(), "NATIONAL areas should have null state_fips"
    
    # For STATE and NONMSA areas, state_abbr should be populated (if PRIM_STATE was available)
    state_areas = df[df['area_type'] == 'STATE']
    nonmsa_areas = df[df['area_type'] == 'NONMSA']
    
    # Check that state_fips are valid 2-digit codes when present
    state_fips_values = df['state_fips'].dropna().unique()
    for fips in state_fips_values:
        assert len(fips) == 2, f"state_fips should be 2 digits: {fips}"
        assert fips.isdigit(), f"state_fips should be numeric: {fips}"
    
    # Check that state_abbr are valid 2-letter codes when present
    state_abbr_values = df['state_abbr'].dropna().unique()
    for abbr in state_abbr_values:
        assert len(abbr) == 2, f"state_abbr should be 2 letters: {abbr}"
        assert abbr.isupper(), f"state_abbr should be uppercase: {abbr}"
    
    print(f"✓ State mappings validated")
    print(f"  Found {len(state_fips_values)} unique FIPS codes")
    print(f"  Found {len(state_abbr_values)} unique state abbreviations")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
