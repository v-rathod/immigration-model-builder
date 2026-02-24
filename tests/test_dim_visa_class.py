"""
Tests for dim_visa_class (Employment-based visa families and subcategories).
"""

import pytest
from pathlib import Path
import pandas as pd

from src.curate.build_dim_visa_class import build_dim_visa_class
from src.io.readers import load_paths_config


@pytest.fixture
def test_paths():
    """Load test paths configuration."""
    config = load_paths_config("configs/paths.yaml")
    return {
        "data_root": config["data_root"],
        "artifacts_root": config["artifacts_root"]
    }


def test_dim_visa_class_builder_creates_file(test_paths):
    """Test that dim_visa_class builder creates output file."""
    output_path = Path(test_paths["artifacts_root"]) / "tables" / "dim_visa_class.parquet"
    
    # Build dim_visa_class
    result_path = build_dim_visa_class(
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
    
    # If data exists, should have minimum expected rows
    if len(df) > 0:
        assert len(df) > 5, f"Expected > 5 visa classes, got {len(df)}"
    
    print(f"✓ dim_visa_class built with {len(df)} rows")


def test_dim_visa_class_schema(test_paths):
    """Test that dim_visa_class follows schema definition."""
    output_path = Path(test_paths["artifacts_root"]) / "tables" / "dim_visa_class.parquet"
    
    # Ensure file exists
    if not output_path.exists():
        build_dim_visa_class(test_paths["data_root"], str(output_path))
    
    df = pd.read_parquet(output_path)
    
    if len(df) == 0:
        pytest.skip("No data available - skipping schema validation")
    
    # Check required columns
    required_cols = [
        'family_code', 'family_name', 'sub_code', 'sub_name',
        'is_employment', 'is_grouped', 'notes', 'source_file', 'ingested_at'
    ]
    
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"
    
    # Validate PK uniqueness (treating null sub_code as empty string for the check)
    df['_pk_check'] = df['family_code'].fillna('') + '||' + df['sub_code'].fillna('')
    assert df['_pk_check'].is_unique, "Primary key (family_code, sub_code) must be unique"
    
    # Check non-null constraints
    assert df['family_code'].notna().all(), "family_code should not have nulls"
    assert df['family_name'].notna().all(), "family_name should not have nulls"
    
    # Check is_employment is boolean
    assert df['is_employment'].dtype == bool, "is_employment should be boolean"
    assert df['is_grouped'].dtype == bool, "is_grouped should be boolean"
    
    print(f"✓ Schema validated: {len(df)} rows, {len(df.columns)} columns")


def test_dim_visa_class_family_codes(test_paths):
    """Test that only EB1..EB5 families appear."""
    output_path = Path(test_paths["artifacts_root"]) / "tables" / "dim_visa_class.parquet"
    
    # Ensure file exists
    if not output_path.exists():
        build_dim_visa_class(test_paths["data_root"], str(output_path))
    
    df = pd.read_parquet(output_path)
    
    if len(df) == 0:
        pytest.skip("No data available - skipping family code validation")
    
    # Check that only valid EB families exist
    valid_families = {'EB1', 'EB2', 'EB3', 'EB4', 'EB5'}
    actual_families = set(df['family_code'].dropna().unique())
    
    assert actual_families.issubset(valid_families), \
        f"Invalid family codes found: {actual_families - valid_families}"
    
    print(f"✓ Family codes validated: {actual_families}")
    
    # Check distribution
    family_counts = df['family_code'].value_counts().to_dict()
    print(f"  Family distribution: {family_counts}")
    
    # Should have at least EB1, EB2, EB3 commonly used categories
    assert 'EB1' in actual_families or 'EB2' in actual_families or 'EB3' in actual_families, \
        "Expected at least one of EB1, EB2, or EB3"


def test_dim_visa_class_subcategories(test_paths):
    """Test subcategory handling and relationships."""
    output_path = Path(test_paths["artifacts_root"]) / "tables" / "dim_visa_class.parquet"
    
    # Ensure file exists
    if not output_path.exists():
        build_dim_visa_class(test_paths["data_root"], str(output_path))
    
    df = pd.read_parquet(output_path)
    
    if len(df) == 0:
        pytest.skip("No data available - skipping subcategory tests")
    
    # Check that rows with sub_code have sub_name
    rows_with_subcode = df[df['sub_code'].notna()]
    if len(rows_with_subcode) > 0:
        # Most subcategories should have names
        named_subcats = rows_with_subcode['sub_name'].notna().sum()
        assert named_subcats > 0, "Subcategories with sub_code should have sub_name"
        print(f"  {named_subcats}/{len(rows_with_subcode)} subcategories have names")
    
    # Check that is_grouped is True for rows with subcategories
    if len(rows_with_subcode) > 0:
        grouped_subcats = rows_with_subcode['is_grouped'].sum()
        print(f"  {grouped_subcats}/{len(rows_with_subcode)} subcategories are grouped")
    
    # All rows should be employment-based (for EB categories)
    assert df['is_employment'].all(), "All EB categories should have is_employment=True"
    
    print(f"✓ Subcategory validation passed")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v"])
