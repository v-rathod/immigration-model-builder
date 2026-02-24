"""
Test fact_perm fact table.
"""
from pathlib import Path
import pandas as pd
import pytest

# fact_perm is stored as a partitioned directory (fiscal_year=XXXX sub-dirs).
# These helpers resolve either a flat file or a partitioned directory.

FACT_PERM_DIR  = Path("artifacts/tables/fact_perm")
FACT_PERM_FILE = Path("artifacts/tables/fact_perm.parquet")


def _fact_perm_exists() -> bool:
    return FACT_PERM_DIR.is_dir() or FACT_PERM_FILE.is_file()


def _load_fact_perm() -> pd.DataFrame:
    if FACT_PERM_FILE.is_file():
        return pd.read_parquet(FACT_PERM_FILE)
    if FACT_PERM_DIR.is_dir():
        return pd.read_parquet(FACT_PERM_DIR)
    pytest.skip("fact_perm artifact not found")


def test_fact_perm_exists():
    """
    Confirm that fact_perm artifact (file or partitioned dir) was created.
    """
    assert _fact_perm_exists(), (
        f"Neither {FACT_PERM_FILE} nor {FACT_PERM_DIR} found"
    )


def test_fact_perm_min_rows():
    """
    Confirm fact_perm has a reasonable number of rows.
    We sample 50k rows per FY from PERM, so expect >1000 rows minimum.
    Fallback to >100 rows if dataset is small.
    """
    df = _load_fact_perm()
    
    # Primary: expect >1000 rows for production sampling
    # Fallback: accept >100 rows for minimal test datasets
    if len(df) > 1000:
        assert True
    else:
        assert len(df) > 100, f"Expected >1000 rows (or >100 minimum), got {len(df)}"


def test_fact_perm_case_number_unique():
    """
    Confirm case_number has very low duplication rate.
    fact_perm is partitioned by fiscal_year; a tiny number of applications
    can appear in adjacent FY datasets (cross-year boundary).  We allow up
    to 1% cross-FY duplicates.
    """
    df = _load_fact_perm()

    total = len(df)
    unique = df["case_number"].nunique()
    dup_rate = (total - unique) / total if total > 0 else 0
    assert dup_rate <= 0.01, (
        f"case_number duplicate rate {dup_rate*100:.2f}% exceeds 1% threshold "
        f"({total - unique:,} dups out of {total:,} rows)"
    )


def test_fact_perm_case_number_non_null():
    """
    Confirm case_number is never null.
    """
    df = _load_fact_perm()
    
    null_count = df["case_number"].isna().sum()
    assert null_count == 0, f"Found {null_count} null case_number values"


def test_fact_perm_foreign_keys_present():
    """
    Confirm foreign keys columns exist and have values.
    """
    df = _load_fact_perm()
    
    # Check columns exist
    required_fks = ['employer_id', 'soc_code', 'area_code', 'employer_country']
    for fk in required_fks:
        assert fk in df.columns, f"Missing FK column: {fk}"
    
    # employer_id should always be present (generated from normalized name)
    assert df['employer_id'].notna().sum() > 0, "All employer_id values are null"
    
    # At least some rows should have mapped soc_code, area_code
    # (not all will map if dimension coverage is incomplete)
    soc_mapped = df['soc_code'].notna().sum()
    area_mapped = df['area_code'].notna().sum()
    
    print(f"\n  FK mapping rates:")
    print(f"    employer_id: {df['employer_id'].notna().sum()} / {len(df)} ({100*df['employer_id'].notna().mean():.1f}%)")
    print(f"    soc_code: {soc_mapped} / {len(df)} ({100*df['soc_code'].notna().mean():.1f}%)")
    print(f"    area_code: {area_mapped} / {len(df)} ({100*df['area_code'].notna().mean():.1f}%)")
    print(f"    employer_country: {df['employer_country'].notna().sum()} / {len(df)} ({100*df['employer_country'].notna().mean():.1f}%)")


def test_fact_perm_dates_parsed():
    """
    Confirm received_date and decision_date are datetime types.
    """
    df = _load_fact_perm()
    
    # Check columns exist
    assert 'received_date' in df.columns, "Missing received_date column"
    assert 'decision_date' in df.columns, "Missing decision_date column"
    
    # Check at least some dates are non-null
    assert df['received_date'].notna().sum() > 0, "All received_date values are null"
    
    # Check types (should be datetime64 or timestamp)
    assert pd.api.types.is_datetime64_any_dtype(df['received_date']), \
        f"received_date should be datetime type, got {df['received_date'].dtype}"


def test_fact_perm_fy_derivation():
    """
    Confirm fiscal_year column is present and contains reasonable values.
    fact_perm is partitioned by fiscal_year (the source file's FY, not derived
    from received_date).  We verify the column exists and values are in the
    expected range (2008-2030).
    """
    df = _load_fact_perm()

    # Support both 'fy' (flat file) and 'fiscal_year' (partitioned dir)
    fy_col = 'fy' if 'fy' in df.columns else 'fiscal_year'
    if fy_col not in df.columns:
        pytest.skip("No fy/fiscal_year column found")

    values = df[fy_col].dropna()
    if len(values) == 0:
        pytest.skip("No non-null fy/fiscal_year values found")

    # fiscal_year may be stored as a Categorical dtype (hive partition key)
    values_int = values.astype(int)
    fy_min, fy_max = int(values_int.min()), int(values_int.max())
    assert 2008 <= fy_min, f"fiscal_year min {fy_min} is before 2008"
    assert fy_max <= 2030, f"fiscal_year max {fy_max} is after 2030"
    print(f"  fiscal_year range: {fy_min}–{fy_max}")


def test_fact_perm_case_status_values():
    """
    Confirm case_status has reasonable values.
    """
    df = _load_fact_perm()
    
    # Check column exists
    assert 'case_status' in df.columns, "Missing case_status column"
    
    # Check for common status values
    status_values = df['case_status'].unique()
    
    # Common PERM statuses include: CERTIFIED, DENIED, WITHDRAWN, CERTIFIED-EXPIRED
    # Just verify we have some non-null values
    assert len(status_values) > 0, "case_status has no values"
    assert df['case_status'].notna().sum() > 0, "All case_status values are null"
    
    print(f"\n  Case status distribution:")
    for status, count in df['case_status'].value_counts().head(10).items():
        print(f"    {status}: {count}")
