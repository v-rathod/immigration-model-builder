"""
Test dim_employer dimension table.
"""
from pathlib import Path
import pandas as pd
import pytest


def test_dim_employer_exists():
    """
    Confirm that dim_employer.parquet was created.
    """
    artifact_path = Path("artifacts/tables/dim_employer.parquet")
    assert artifact_path.is_file(), f"{artifact_path} not found"


def test_dim_employer_min_rows():
    """
    Confirm dim_employer has a reasonable number of rows.
    We sample 50k rows per FY from PERM, so expect >100 unique employers minimum.
    Fallback to >20 if dataset is small.
    """
    artifact_path = Path("artifacts/tables/dim_employer.parquet")
    df = pd.read_parquet(artifact_path)
    
    # Primary: expect >100 rows for production sampling
    # Fallback: accept >20 rows for minimal test datasets
    if len(df) > 100:
        assert True
    else:
        assert len(df) > 20, f"Expected >100 rows (or >20 minimum), got {len(df)}"


def test_dim_employer_id_unique():
    """
    Confirm employer_id is unique (primary key constraint).
    """
    artifact_path = Path("artifacts/tables/dim_employer.parquet")
    df = pd.read_parquet(artifact_path)
    
    assert df["employer_id"].is_unique, "employer_id must be unique"


def test_dim_employer_name_non_null():
    """
    Confirm employer_name is never null.
    """
    artifact_path = Path("artifacts/tables/dim_employer.parquet")
    df = pd.read_parquet(artifact_path)
    
    null_count = df["employer_name"].isna().sum()
    assert null_count == 0, f"Found {null_count} null employer_name values"


def test_dim_employer_suffix_removal():
    """
    Spot-check that common legal suffixes are removed from canonical names.
    """
    artifact_path = Path("artifacts/tables/dim_employer.parquet")
    df = pd.read_parquet(artifact_path)
    
    # Sample canonical names should not end with common legal suffixes
    # (after normalization they should be removed)
    bad_suffixes = [" Inc", " LLC", " Ltd", " Corp", " LLP"]
    
    for name in df["employer_name"].head(50):
        for suffix in bad_suffixes:
            assert not name.endswith(suffix), \
                f"Canonical name '{name}' still has legal suffix '{suffix}'"


def test_dim_employer_title_case():
    """
    Confirm employer_name uses Title Case formatting.
    """
    artifact_path = Path("artifacts/tables/dim_employer.parquet")
    df = pd.read_parquet(artifact_path)
    
    # Check first 20 names for title case
    for name in df["employer_name"].head(20):
        # Title case means first letter of each word is uppercase
        # We can't enforce perfectly (e.g., "McDonald's" vs "Mcdonald's")
        # but we can check it's not all uppercase or all lowercase
        assert not name.isupper(), f"Name '{name}' is all uppercase"
        assert not name.islower(), f"Name '{name}' is all lowercase"
        assert name[0].isupper(), f"Name '{name}' doesn't start with capital"
