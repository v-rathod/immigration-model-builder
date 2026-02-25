"""Tests for dim_soc table from SOC 2010-to-2018 crosswalk."""

import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest


@pytest.mark.slow_integration
def test_dim_soc_builder_creates_file():
    """Test that running the curate pipeline creates dim_soc parquet file."""
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
    dim_soc_path = Path("artifacts/tables/dim_soc.parquet")
    assert dim_soc_path.exists(), f"dim_soc parquet not created at {dim_soc_path}"
    
    print("✓ dim_soc.parquet file created")


def test_dim_soc_schema():
    """Test that dim_soc has required columns and valid data."""
    dim_soc_path = Path("artifacts/tables/dim_soc.parquet")
    
    if not dim_soc_path.exists():
        pytest.skip("dim_soc.parquet not found - run curate pipeline first")
    
    df = pd.read_parquet(dim_soc_path)
    
    # Check required columns from schema
    required_cols = [
        'soc_code', 'soc_title', 'soc_version', 'soc_major_group',
        'soc_minor_group', 'soc_broad_group', 'from_version', 'from_code',
        'mapping_confidence', 'is_aggregated', 'source_file', 'ingested_at'
    ]
    
    for col in required_cols:
        assert col in df.columns, f"Missing required column: {col}"
    
    # Check primary key (soc_code) is unique and not null
    assert df['soc_code'].notna().all(), "Found null values in soc_code (primary key)"
    assert df['soc_code'].is_unique, "soc_code is not unique (violates primary key constraint)"
    
    # Check data types
    assert pd.api.types.is_string_dtype(df['soc_code']), "soc_code should be string"
    assert pd.api.types.is_string_dtype(df['soc_title']), "soc_title should be string"
    assert pd.api.types.is_string_dtype(df['soc_version']), "soc_version should be string"
    assert pd.api.types.is_bool_dtype(df['is_aggregated']), "is_aggregated should be boolean"
    
    # Check soc_version is always a known taxonomy year
    valid_versions = {'2018', '2010'}
    invalid_version = df[~df['soc_version'].isin(valid_versions)]
    assert len(invalid_version) == 0, (
        f"Found invalid soc_version values: {invalid_version['soc_version'].unique()}"
    )
    
    # Check soc_code format (XX-XXXX)
    import re
    invalid_codes = df[~df['soc_code'].str.match(r'^\d{2}-\d{4}$')]
    assert len(invalid_codes) == 0, (
        f"Found {len(invalid_codes)} invalid SOC codes. "
        f"Examples: {invalid_codes['soc_code'].head().tolist()}"
    )
    
    # Check mapping confidence values
    valid_confidence = {'deterministic', 'one-to-many', 'many-to-one', 'manual-review', 'inferred_from_lca'}
    invalid_confidence = df[~df['mapping_confidence'].isin(valid_confidence)]
    assert len(invalid_confidence) == 0, (
        f"Found invalid mapping_confidence values: {invalid_confidence['mapping_confidence'].unique()}"
    )
    
    # Check hierarchy consistency
    # Major group should match first 2 digits of soc_code
    major_mismatch = df[df['soc_major_group'] != df['soc_code'].str[:2]]
    assert len(major_mismatch) == 0, (
        f"Found {len(major_mismatch)} rows where major_group doesn't match soc_code. "
        f"Examples: {major_mismatch[['soc_code', 'soc_major_group']].head().to_dict('records')}"
    )
    
    # Minor group should match first 5 chars (XX-XX) — only check populated rows
    minor_populated = df[df['soc_minor_group'].notna()]
    minor_mismatch = minor_populated[minor_populated['soc_minor_group'] != minor_populated['soc_code'].str[:5]]
    assert len(minor_mismatch) == 0, (
        f"Found {len(minor_mismatch)} rows where minor_group doesn't match soc_code"
    )
    
    # Provenance fields — source_file may be null for inferred legacy (SOC-2010) codes
    soc2018 = df[df['soc_version'] == '2018']
    assert soc2018['source_file'].notna().all(), "source_file should not be null for SOC-2018 codes"
    assert soc2018['ingested_at'].notna().all(), "ingested_at should not be null for SOC-2018 codes"
    
    print(f"✓ dim_soc validated: {len(df)} rows")
    print(f"  SOC codes: {df['soc_code'].nunique()} unique")
    print(f"  Major groups: {df['soc_major_group'].nunique()} unique")
    print(f"  Deterministic mappings: {sum(df['mapping_confidence'] == 'deterministic')}")
    print(f"  Aggregated SOCs: {sum(df['is_aggregated'] == True)}")


def test_dim_soc_crosswalk_coverage():
    """Test that dim_soc includes both 2010 and native 2018 SOCs where applicable."""
    dim_soc_path = Path("artifacts/tables/dim_soc.parquet")
    
    if not dim_soc_path.exists():
        pytest.skip("dim_soc.parquet not found")
    
    df = pd.read_parquet(dim_soc_path)
    
    # Check that some rows have from_version='2010' (crosswalked)
    crosswalked = df[df['from_version'] == '2010']
    assert len(crosswalked) > 0, "Expected some rows to be crosswalked from SOC 2010"
    
    # Check that rows with from_version also have from_code
    crosswalked_with_code = crosswalked[crosswalked['from_code'].notna()]
    assert len(crosswalked_with_code) == len(crosswalked), (
        "All crosswalked rows should have from_code populated"
    )
    
    # Check mapping confidence distribution
    confidence_counts = df['mapping_confidence'].value_counts().to_dict()
    print(f"  Mapping confidence distribution: {confidence_counts}")
    
    # Should have at least some deterministic mappings
    assert confidence_counts.get('deterministic', 0) > 0, (
        "Expected at least some deterministic mappings"
    )
    
    print(f"✓ Crosswalk coverage validated")
    print(f"  Crosswalked from 2010: {len(crosswalked)} rows")
    print(f"  Confidence levels: {confidence_counts}")


def test_dim_soc_hierarchy_extraction():
    """Test that hierarchy fields are correctly extracted from soc_code."""
    dim_soc_path = Path("artifacts/tables/dim_soc.parquet")
    
    if not dim_soc_path.exists():
        pytest.skip("dim_soc.parquet not found")
    
    df = pd.read_parquet(dim_soc_path)
    
    # Sample a few SOC codes and verify hierarchy
    sample = df.head(5)
    
    for _, row in sample.iterrows():
        soc = row['soc_code']
        major = row['soc_major_group']
        minor = row['soc_minor_group']
        broad = row['soc_broad_group']
        
        # Verify extraction logic
        assert major == soc[:2], f"Major group mismatch for {soc}"
        assert minor == soc[:5], f"Minor group mismatch for {soc}"
        assert broad == soc[:6], f"Broad group mismatch for {soc}"
        
        print(f"  {soc}: major={major}, minor={minor}, broad={broad} ✓")
    
    print(f"✓ Hierarchy extraction validated")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
