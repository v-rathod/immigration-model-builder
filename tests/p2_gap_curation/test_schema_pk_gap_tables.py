"""
P2 Gap Curation – Schema & PK uniqueness tests.

Validates that every new gap-curation parquet:
  1. Has all required schema columns.
  2. Has no duplicate primary-key rows.
  3. Has no all-null rows.
"""
import pathlib
import pandas as pd
import pytest

TABLES_DIR = pathlib.Path("artifacts/tables")

# -----------------------------------------------------------------
# (table_name, required_columns, pk_columns)
# Stubs (expected 0 rows) are allowed to have no PK duplicates trivially.
# -----------------------------------------------------------------
TABLE_SPECS = [
    (
        "dim_visa_ceiling",
        ["fiscal_year", "category", "country", "ceiling", "source_file", "ingested_at"],
        ["fiscal_year", "category", "country"],
    ),
    (
        "fact_waiting_list",
        ["report_year", "category", "country", "count_waiting", "source_file", "ingested_at"],
        ["report_year", "category", "country"],
    ),
    (
        "fact_niv_issuance",
        ["fiscal_year", "visa_class", "country", "issued", "source_file", "ingested_at"],
        ["fiscal_year", "visa_class", "country"],
    ),
    (
        "fact_uscis_approvals",
        ["fiscal_year", "form", "category", "approvals", "denials", "source_file", "ingested_at"],
        ["fiscal_year", "form", "category"],
    ),
    (
        "fact_dhs_admissions",
        ["fiscal_year", "class_of_admission", "country", "admissions", "source_file", "ingested_at"],
        ["fiscal_year", "class_of_admission", "country"],
    ),
    (
        "fact_warn_events",
        ["state", "notice_date", "employer_name_raw", "city", "employees_affected", "source_file", "ingested_at"],
        ["state", "notice_date", "employer_name_raw", "city"],
    ),
    (
        "fact_trac_adjudications",
        ["fiscal_year", "form", "measure", "value", "source_file", "ingested_at"],
        ["fiscal_year", "form", "measure"],
    ),
    (
        "fact_acs_wages",
        ["year", "soc_code", "area_code", "median", "source_file", "ingested_at"],
        ["year", "soc_code", "area_code"],
    ),
]

# visa_issuance and visa_applications are added only if they exist
OPTIONAL_TABLE_SPECS = [
    (
        "fact_visa_issuance",
        ["fiscal_year", "category", "country", "issued", "source_file", "ingested_at"],
        ["fiscal_year", "category", "country"],
    ),
    (
        "fact_visa_applications",
        ["fiscal_year", "visa_class", "category", "country", "applications", "source_file", "ingested_at"],
        ["fiscal_year", "visa_class", "category", "country"],
    ),
]


def load(name: str) -> pd.DataFrame:
    p = TABLES_DIR / f"{name}.parquet"
    assert p.exists(), f"Parquet file not found: {p}"
    return pd.read_parquet(p)


@pytest.mark.parametrize("table_name,required_cols,pk_cols", TABLE_SPECS)
def test_schema_columns(table_name, required_cols, pk_cols):
    df = load(table_name)
    missing = [c for c in required_cols if c not in df.columns]
    assert not missing, f"{table_name}: missing columns {missing}"


@pytest.mark.parametrize("table_name,required_cols,pk_cols", TABLE_SPECS)
def test_pk_uniqueness(table_name, required_cols, pk_cols):
    df = load(table_name)
    if df.empty:
        return  # stub — trivially unique
    existing_pk = [c for c in pk_cols if c in df.columns]
    if not existing_pk:
        return
    dupes = df.duplicated(subset=existing_pk, keep=False).sum()
    assert dupes == 0, f"{table_name}: {dupes} duplicate PK rows on {existing_pk}"


@pytest.mark.parametrize("table_name,required_cols,pk_cols", TABLE_SPECS)
def test_no_all_null_rows(table_name, required_cols, pk_cols):
    df = load(table_name)
    if df.empty:
        return
    all_null = df.isnull().all(axis=1).sum()
    assert all_null == 0, f"{table_name}: {all_null} all-null rows found"


@pytest.mark.parametrize("table_name,required_cols,pk_cols", OPTIONAL_TABLE_SPECS)
def test_optional_schema_columns(table_name, required_cols, pk_cols):
    p = TABLES_DIR / f"{table_name}.parquet"
    if not p.exists():
        pytest.skip(f"{table_name}.parquet not yet generated")
    df = pd.read_parquet(p)
    missing = [c for c in required_cols if c not in df.columns]
    assert not missing, f"{table_name}: missing columns {missing}"


@pytest.mark.parametrize("table_name,required_cols,pk_cols", OPTIONAL_TABLE_SPECS)
def test_optional_pk_uniqueness(table_name, required_cols, pk_cols):
    p = TABLES_DIR / f"{table_name}.parquet"
    if not p.exists():
        pytest.skip(f"{table_name}.parquet not yet generated")
    df = pd.read_parquet(p)
    if df.empty:
        return
    existing_pk = [c for c in pk_cols if c in df.columns]
    if not existing_pk:
        return
    dupes = df.duplicated(subset=existing_pk, keep=False).sum()
    assert dupes == 0, f"{table_name}: {dupes} duplicate PK rows on {existing_pk}"
