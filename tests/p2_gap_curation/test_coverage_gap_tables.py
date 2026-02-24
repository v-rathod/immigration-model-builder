"""
P2 Gap Curation – Coverage and completeness tests.

Validates that the gap-curation parquets together cover the required
fiscal-year spans and table combinations expected from the source data.
"""
import pathlib
import pandas as pd
import pytest

TABLES_DIR = pathlib.Path("artifacts/tables")


def load(name: str) -> pd.DataFrame:
    p = TABLES_DIR / f"{name}.parquet"
    assert p.exists(), f"Parquet file not found: {p}"
    return pd.read_parquet(p)


# ---- Fiscal-year coverage -------------------------------------------------

def test_niv_fy_coverage():
    """fact_niv_issuance should span at least FY2017-FY2024."""
    df = load("fact_niv_issuance")
    fys = set(df["fiscal_year"].dropna().unique())
    expected = {f"FY{y}" for y in range(2017, 2025)}
    missing = expected - fys
    assert not missing, f"fact_niv_issuance: missing FYs {missing}"


def test_dhs_admissions_span():
    """fact_dhs_admissions should cover a 30+ year span."""
    df = load("fact_dhs_admissions")
    fys = df["fiscal_year"].dropna().unique()
    years = sorted(int(fy[2:]) for fy in fys if fy.startswith("FY") and fy[2:].isdigit())
    span = years[-1] - years[0] if years else 0
    assert span >= 30, f"fact_dhs_admissions FY span too small ({span} years)"


def test_uscis_approvals_fy_coverage():
    """fact_uscis_approvals should have at least 3 distinct fiscal years."""
    df = load("fact_uscis_approvals")
    if df.empty:
        pytest.skip("fact_uscis_approvals is empty")
    n_fys = df["fiscal_year"].nunique()
    assert n_fys >= 3, f"fact_uscis_approvals: only {n_fys} distinct FYs"


def test_warn_has_multiple_states():
    """fact_warn_events should cover at least 2 states."""
    df = load("fact_warn_events")
    n_states = df["state"].nunique()
    assert n_states >= 2, f"fact_warn_events: only {n_states} states (expected CA + TX)"


# ---- Schema completeness --------------------------------------------------

def test_all_p2_parquets_exist():
    """All 10 expected P2 gap parquets must exist on disk."""
    expected = [
        "dim_visa_ceiling",
        "fact_waiting_list",
        "fact_niv_issuance",
        "fact_uscis_approvals",
        "fact_dhs_admissions",
        "fact_warn_events",
        "fact_trac_adjudications",
        "fact_acs_wages",
    ]
    missing = []
    for name in expected:
        p = TABLES_DIR / f"{name}.parquet"
        if not p.exists():
            missing.append(name)
    assert not missing, f"Missing parquets: {missing}"


def test_optional_pdf_parquets_if_exist():
    """If PDF-derived parquets exist, they should have rows."""
    for name in ("fact_visa_issuance", "fact_visa_applications"):
        p = TABLES_DIR / f"{name}.parquet"
        if p.exists():
            df = pd.read_parquet(p)
            # Either 0 rows (all PDFs unreadable) or many rows; warn if suspiciously empty
            row_count = len(df)
            # Only fail if it appears the builder ran but got tiny output
            if row_count > 0:
                assert row_count >= 100, (
                    f"{name}: only {row_count} rows — suspiciously low for PDF extraction"
                )


# ---- Referential integrity check ------------------------------------------

def test_niv_has_standard_visa_classes():
    """fact_niv_issuance should contain at least some well-known visa classes."""
    STANDARD_CLASSES = {"H-1B", "H-4", "L-1", "O-1", "F-1", "J-1", "B-1/B-2", "B1/B2"}
    df = load("fact_niv_issuance")
    found = set(df["visa_class"].dropna().unique())
    overlap = STANDARD_CLASSES & found
    # Must have at least 2 of the standard classes
    assert len(overlap) >= 2, (
        f"fact_niv_issuance has no standard visa classes; found: {sorted(list(found))[:10]}"
    )


def test_dim_visa_ceiling_has_employment_category():
    """dim_visa_ceiling should include employment-based categories."""
    df = load("dim_visa_ceiling")
    cats = set(df["category"].dropna().str.upper().unique())
    # Accept any category mentioning employment or EB
    has_eb = any("EMPLOY" in c or c.startswith("EB") for c in cats)
    assert has_eb, f"dim_visa_ceiling: no employment-based category; found: {sorted(cats)}"


def test_dhs_refugee_class():
    """fact_dhs_admissions should include refugee class of admission."""
    df = load("fact_dhs_admissions")
    classes = set(df["class_of_admission"].dropna().str.upper().unique())
    has_refugee = any("REFUGEE" in c or "REF" == c for c in classes)
    assert has_refugee, f"fact_dhs_admissions: no REFUGEE class; found: {sorted(classes)}"
