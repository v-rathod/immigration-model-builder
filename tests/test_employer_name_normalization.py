"""
Integration tests: verify canonical employer name normalization in artifacts.

These tests read actual parquet artifacts and confirm that:
  1. Known duplicate raw names (e.g., "GOOGLE INC" vs "Google Inc.") no longer
     appear as separate entries in feature / fact tables.
  2. All canonical employer names that originate from dim_employer use
     Title Case formatting (not ALL-CAPS raw names).
  3. Key employer-keyed feature tables use canonical names for top employers.

Tests are marked as integration tests (they require built artifacts) and are
skipped if the relevant parquet files do not exist.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

TABLES = Path("artifacts/tables")


def _load(filename: str) -> pd.DataFrame:
    path = TABLES / filename
    if not path.exists():
        pytest.skip(f"Artifact not found: {path}")
    return pd.read_parquet(path)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _google_variants(df: pd.DataFrame, col: str = "employer_name") -> list[str]:
    mask = df[col].str.lower().str.contains("google", na=False)
    return sorted(df.loc[mask, col].unique().tolist())


_KNOWN_RAW_GOOGLE = {
    "google inc", "google inc.", "GOOGLE INC", "GOOGLE INC.", "GOOGLE INC,",
    "google llc", "GOOGLE LLC", "Google Inc.", "GOOGLE LLC,",
}


def _has_raw_google(names: list[str]) -> bool:
    """Return True if any raw Google all-caps variant is in the list."""
    return any(n in _KNOWN_RAW_GOOGLE for n in names)


# ---------------------------------------------------------------------------
# dim_employer — baseline check
# ---------------------------------------------------------------------------

def test_dim_employer_google_canonical():
    """dim_employer must have 'Google' (Title Case) not 'GOOGLE INC'."""
    df = _load("dim_employer.parquet")
    variants = _google_variants(df)
    assert "Google" in variants, f"Expected 'Google' in dim_employer, got: {variants}"
    assert not _has_raw_google(variants), (
        f"Raw capitalized Google variant found in dim_employer: {variants}"
    )


def test_dim_employer_no_all_caps_names():
    """No canonical employer names should be entirely uppercase.

    (All Title Case names will have mixed case for multi-word names.)
    """
    df = _load("dim_employer.parquet")
    # Sample first 200 names — all should not be ALL-CAPS
    sample = df["employer_name"].dropna().head(200)
    all_caps = [n for n in sample if isinstance(n, str) and n.isupper() and len(n) > 3]
    assert not all_caps, (
        f"Found {len(all_caps)} ALL-CAPS employer names in dim_employer: {all_caps[:5]}"
    )


# ---------------------------------------------------------------------------
# employer_salary_yearly — primary consumer for P3 Wage page
# ---------------------------------------------------------------------------

def test_employer_salary_yearly_google_no_raw_variants():
    """employer_salary_yearly must NOT contain raw Google variants like 'GOOGLE INC'."""
    df = _load("employer_salary_yearly.parquet")
    variants = _google_variants(df)
    assert not _has_raw_google(variants), (
        f"Raw Google variants still present in employer_salary_yearly: {variants}"
    )


def test_employer_salary_yearly_google_canonical_present():
    """employer_salary_yearly must contain the canonical 'Google' entry."""
    df = _load("employer_salary_yearly.parquet")
    variants = _google_variants(df)
    assert "Google" in variants, (
        f"Canonical 'Google' not found in employer_salary_yearly. Got: {variants}"
    )


def test_employer_salary_yearly_no_all_caps_top_employers():
    """Top 100 employers by total filings must not be ALL-CAPS."""
    df = _load("employer_salary_yearly.parquet")
    top_employers = (
        df.groupby("employer_name")["total_filings"].sum()
        .nlargest(100)
        .index.tolist()
    )
    all_caps = [n for n in top_employers if isinstance(n, str) and n.isupper() and len(n) > 3]
    assert not all_caps, (
        f"ALL-CAPS names in top-100 employers (employer_salary_yearly): {all_caps[:5]}"
    )


def test_employer_salary_yearly_microsoft_no_raw_variants():
    """Microsoft variants should all collapse to one canonical name."""
    df = _load("employer_salary_yearly.parquet")
    mask = df["employer_name"].str.lower().str.contains(r"\bmicrosoft\b", regex=True, na=False)
    variants = sorted(df.loc[mask, "employer_name"].unique().tolist())
    # There should be a Microsoft variant but its raw upper-case forms should be gone
    raw_ms_variants = [v for v in variants if v.isupper() and "MICROSOFT" in v.upper()]
    assert not raw_ms_variants, (
        f"Raw Microsoft variants still in employer_salary_yearly: {raw_ms_variants}"
    )


# ---------------------------------------------------------------------------
# employer_salary_profiles — detailed grain
# ---------------------------------------------------------------------------

def test_employer_salary_profiles_google_no_raw_variants():
    """employer_salary_profiles must NOT contain raw Google variants."""
    df = _load("employer_salary_profiles.parquet")
    variants = _google_variants(df)
    assert not _has_raw_google(variants), (
        f"Raw Google variants still present in employer_salary_profiles: {variants}"
    )


def test_employer_salary_profiles_google_canonical_present():
    df = _load("employer_salary_profiles.parquet")
    variants = _google_variants(df)
    assert "Google" in variants, (
        f"Canonical 'Google' not found in employer_salary_profiles. Got: {variants}"
    )


# ---------------------------------------------------------------------------
# employer_monthly_metrics — PERM-based; should join dim_employer
# ---------------------------------------------------------------------------

def test_employer_monthly_metrics_google_no_raw_variants():
    """employer_monthly_metrics must not contain 'GOOGLE INC.' etc."""
    df = _load("employer_monthly_metrics.parquet")
    variants = _google_variants(df)
    assert not _has_raw_google(variants), (
        f"Raw Google variants in employer_monthly_metrics: {variants}"
    )


def test_employer_monthly_metrics_google_canonical_present():
    df = _load("employer_monthly_metrics.parquet")
    variants = _google_variants(df)
    assert "Google" in variants, (
        f"Canonical 'Google' not in employer_monthly_metrics. Got: {variants}"
    )


# ---------------------------------------------------------------------------
# Cross-artifact consistency: employer_id linkage
# ---------------------------------------------------------------------------

def test_employer_salary_yearly_has_employer_id():
    """employer_salary_yearly must contain employer_id for P3 lookups."""
    df = _load("employer_salary_yearly.parquet")
    assert "employer_id" in df.columns, "employer_salary_yearly is missing employer_id column"
    null_ids = df["employer_id"].isna().sum()
    assert null_ids == 0, f"{null_ids} rows have null employer_id in employer_salary_yearly"


def test_employer_salary_profiles_has_employer_id():
    df = _load("employer_salary_profiles.parquet")
    assert "employer_id" in df.columns, "employer_salary_profiles is missing employer_id"
    null_ids = df["employer_id"].isna().sum()
    assert null_ids == 0, f"{null_ids} rows have null employer_id in employer_salary_profiles"


# ---------------------------------------------------------------------------
# normalize module: mappings imported correctly
# ---------------------------------------------------------------------------

def test_normalize_module_importable():
    """Verify that src.normalize.mappings can be imported (import sanity check)."""
    from src.normalize.mappings import (  # noqa: F401
        normalize_employer_name,
        normalize_soc_code,
        normalize_country_code,
        normalize_visa_category,
        title_case_employer_name,
    )


def test_normalize_module_google_dedup():
    """Functional round-trip: normalize all raw Google names → same key."""
    from src.normalize.mappings import normalize_employer_name
    variants = [
        "Google Inc",
        "GOOGLE INC",
        "Google Inc.",
        "GOOGLE INC.",
        "GOOGLE INC,",
        "Google LLC",
        "GOOGLE LLC",
    ]
    results = {normalize_employer_name(v) for v in variants}
    assert len(results) == 1, f"Expected 1 key, got {len(results)}: {results}"
