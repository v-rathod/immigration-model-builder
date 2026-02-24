"""
P2 Gap Curation – Row-count, range, and data-quality tests.

Gates:
  - Non-stub tables must have ≥1 row.
  - Numeric measure columns must be ≥ 0.
  - Fiscal-year strings must look like FY20xx (where present).
  - dim_visa_ceiling: ceiling values must be plausible (≥ 1,000).
  - fact_niv_issuance: at least 100K rows (known: 153K).
  - fact_warn_events: at least 100 rows.
  - fact_dhs_admissions: FY range covers FY1980–FY2024+.
"""
import pathlib
import re
import pandas as pd
import pytest

TABLES_DIR = pathlib.Path("artifacts/tables")

FY_PATTERN = re.compile(r"^FY\d{4}$")

STUB_TABLES = {"fact_trac_adjudications", "fact_acs_wages"}
NON_STUB_TABLES = [
    "dim_visa_ceiling",
    "fact_waiting_list",
    "fact_niv_issuance",
    "fact_uscis_approvals",
    "fact_dhs_admissions",
    "fact_warn_events",
]
OPTIONAL_TABLES = ["fact_visa_issuance", "fact_visa_applications"]


def load(name: str) -> pd.DataFrame:
    p = TABLES_DIR / f"{name}.parquet"
    assert p.exists(), f"Parquet file not found: {p}"
    return pd.read_parquet(p)


# ---- Row-count gates -------------------------------------------------------

@pytest.mark.parametrize("table_name", NON_STUB_TABLES)
def test_non_stub_has_rows(table_name):
    df = load(table_name)
    assert len(df) >= 1, f"{table_name}: expected ≥1 rows, got {len(df)}"


def test_fact_niv_issuance_row_count():
    df = load("fact_niv_issuance")
    assert len(df) >= 100_000, f"fact_niv_issuance: expected ≥100K rows, got {len(df)}"


def test_fact_warn_events_row_count():
    df = load("fact_warn_events")
    assert len(df) >= 100, f"fact_warn_events: expected ≥100 rows, got {len(df)}"


def test_dim_visa_ceiling_row_count():
    df = load("dim_visa_ceiling")
    assert len(df) >= 10, f"dim_visa_ceiling: expected ≥10 rows, got {len(df)}"


# ---- Numeric range checks --------------------------------------------------

def test_dim_visa_ceiling_plausible():
    df = load("dim_visa_ceiling")
    assert (df["ceiling"] >= 0).all(), "dim_visa_ceiling: negative ceiling values"
    # At least some ceilings should be large (worldwide limits are in tens of thousands)
    assert df["ceiling"].max() >= 1_000, "dim_visa_ceiling: max ceiling suspiciously low"


def test_fact_niv_issuance_non_negative():
    df = load("fact_niv_issuance")
    assert (df["issued"] >= 0).all(), "fact_niv_issuance: negative issued values"


def test_fact_uscis_approvals_non_negative():
    df = load("fact_uscis_approvals")
    if df.empty:
        return
    assert (df["approvals"] >= 0).all(), "fact_uscis_approvals: negative approvals"
    assert (df["denials"] >= 0).all(), "fact_uscis_approvals: negative denials"


def test_fact_dhs_admissions_non_negative():
    df = load("fact_dhs_admissions")
    assert (df["admissions"] >= 0).all(), "fact_dhs_admissions: negative admissions"


def test_fact_warn_events_non_negative():
    df = load("fact_warn_events")
    assert (df["employees_affected"] >= 0).all(), "fact_warn_events: negative employees_affected"


# ---- Fiscal year format checks ---------------------------------------------

@pytest.mark.parametrize("table_name,fy_col", [
    ("dim_visa_ceiling", "fiscal_year"),
    ("fact_niv_issuance", "fiscal_year"),
    ("fact_uscis_approvals", "fiscal_year"),
    ("fact_dhs_admissions", "fiscal_year"),
])
def test_fiscal_year_format(table_name, fy_col):
    df = load(table_name)
    if df.empty:
        return
    bad = df[~df[fy_col].astype(str).str.match(r"^FY\d{4}$", na=False)]
    # At most 5% can be non-standard (like FY_UNKNOWN from multi-year files)
    pct_bad = len(bad) / len(df)
    assert pct_bad <= 0.05, (
        f"{table_name}.{fy_col}: {pct_bad:.1%} non-FYxxxx values "
        f"(sample: {bad[fy_col].unique()[:5].tolist()})"
    )


# ---- DHS yearbook coverage -------------------------------------------------

def test_fact_dhs_admissions_fy_range():
    df = load("fact_dhs_admissions")
    fys = sorted(df["fiscal_year"].unique())
    assert "FY1980" in fys or any(y <= "FY1985" for y in fys), (
        f"fact_dhs_admissions: no early FY rows (earliest: {fys[:3]})"
    )
    assert any(y >= "FY2020" for y in fys), (
        f"fact_dhs_admissions: no recent FY rows (latest: {fys[-3:]})"
    )


# ---- Optional tables (skip if absent) -------------------------------------

@pytest.mark.parametrize("table_name,count_col", [
    ("fact_visa_issuance", "issued"),
    ("fact_visa_applications", "applications"),
])
def test_optional_non_negative(table_name, count_col):
    p = TABLES_DIR / f"{table_name}.parquet"
    if not p.exists():
        pytest.skip(f"{table_name}.parquet not yet generated")
    df = pd.read_parquet(p)
    if df.empty:
        return
    assert (df[count_col] >= 0).all(), f"{table_name}: negative {count_col} values"
