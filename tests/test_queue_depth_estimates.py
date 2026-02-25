"""Tests for queue_depth_estimates.parquet feature table."""

import pandas as pd
import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"
QDE_PATH = TABLES / "queue_depth_estimates.parquet"


def _load():
    if not QDE_PATH.exists():
        pytest.skip("queue_depth_estimates.parquet not found")
    return pd.read_parquet(QDE_PATH)


# ── Schema ────────────────────────────────────────────────────────────────────


class TestSchema:
    def test_file_exists(self):
        assert QDE_PATH.exists(), "queue_depth_estimates.parquet not found"

    def test_required_columns(self):
        df = _load()
        required = [
            "category",
            "country",
            "pd_month",
            "perm_filings_certified",
            "eb_category_ratio",
            "est_category_filings",
            "est_applicants_with_dependents",
            "current_cutoff_date",
            "is_ahead_of_cutoff",
            "annual_visa_allocation",
            "velocity_days_per_month",
            "cumulative_ahead",
            "est_wait_years",
            "est_months_to_current",
            "confidence",
            "generated_at",
        ]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"Missing columns: {missing}"

    def test_row_count_minimum(self):
        df = _load()
        # 3 categories × 5 countries × ~150 distinct PD months = ~2000+ rows
        assert len(df) >= 1000, f"Expected ≥1000 rows, got {len(df)}"


# ── Categories & Countries ────────────────────────────────────────────────────


class TestDimensions:
    def test_categories(self):
        df = _load()
        expected = {"EB1", "EB2", "EB3"}
        actual = set(df["category"].unique())
        assert expected == actual, f"Expected {expected}, got {actual}"

    def test_countries(self):
        df = _load()
        expected = {"IND", "CHN", "MEX", "PHL", "ROW"}
        actual = set(df["country"].unique())
        assert expected == actual, f"Expected {expected}, got {actual}"


# ── Primary Key ───────────────────────────────────────────────────────────────


class TestPrimaryKey:
    def test_pk_unique(self):
        df = _load()
        pk_cols = ["category", "country", "pd_month"]
        dupes = df.duplicated(subset=pk_cols).sum()
        assert dupes == 0, f"PK (category, country, pd_month) has {dupes} duplicates"


# ── Value Ranges ──────────────────────────────────────────────────────────────


class TestValueRanges:
    def test_perm_filings_non_negative(self):
        df = _load()
        assert (df["perm_filings_certified"] >= 0).all()

    def test_eb_category_ratio_bounds(self):
        df = _load()
        assert ((df["eb_category_ratio"] >= 0) & (df["eb_category_ratio"] <= 1)).all()

    def test_est_category_filings_non_negative(self):
        df = _load()
        assert (df["est_category_filings"] >= 0).all()

    def test_est_applicants_non_negative(self):
        df = _load()
        assert (df["est_applicants_with_dependents"] >= 0).all()

    def test_annual_allocation_positive(self):
        df = _load()
        assert (df["annual_visa_allocation"] > 0).all()

    def test_cumulative_ahead_non_negative(self):
        df = _load()
        assert (df["cumulative_ahead"] >= 0).all()

    def test_est_wait_years_non_negative(self):
        df = _load()
        assert (df["est_wait_years"] >= 0).all()

    def test_confidence_values(self):
        df = _load()
        valid = {"low", "medium", "medium-low"}
        actual = set(df["confidence"].unique())
        assert actual.issubset(valid), f"Unexpected confidence values: {actual - valid}"


# ── Business Logic ────────────────────────────────────────────────────────────


class TestBusinessLogic:
    def test_eb2_india_has_queue(self):
        """EB2 India should have a substantial queue ahead."""
        df = _load()
        eb2_ind = df[(df["category"] == "EB2") & (df["country"] == "IND")]
        assert len(eb2_ind) > 0, "No EB2 India rows"
        ahead = eb2_ind[eb2_ind["is_ahead_of_cutoff"]]
        assert len(ahead) > 0, "No rows ahead of cutoff for EB2 India"
        max_cum = ahead["cumulative_ahead"].max()
        assert max_cum > 10000, f"EB2 India cumulative ahead = {max_cum}, expected >10K"

    def test_eb2_india_wait_substantial(self):
        """EB2 India wait should be substantial (>5 years)."""
        df = _load()
        eb2_ind = df[(df["category"] == "EB2") & (df["country"] == "IND")]
        ahead = eb2_ind[eb2_ind["is_ahead_of_cutoff"]]
        max_wait = ahead["est_wait_years"].max()
        assert max_wait > 5, f"EB2 India max wait = {max_wait}, expected >5yr"

    def test_eb2_india_cutoff_date_present(self):
        """EB2 India should have a current cutoff date."""
        df = _load()
        eb2_ind = df[(df["category"] == "EB2") & (df["country"] == "IND")]
        assert eb2_ind["current_cutoff_date"].notna().any()

    def test_cumulative_monotonic_for_ahead_rows(self):
        """Cumulative ahead should be monotonically non-decreasing within each category×country for ahead rows."""
        df = _load()
        for (cat, cty), grp in df.groupby(["category", "country"]):
            ahead = grp[grp["is_ahead_of_cutoff"]].sort_values("pd_month")
            if len(ahead) < 2:
                continue
            vals = ahead["cumulative_ahead"].values
            for i in range(1, len(vals)):
                assert vals[i] >= vals[i - 1], (
                    f"{cat} {cty}: cumulative_ahead not monotonic at pd_month="
                    f"{ahead.iloc[i]['pd_month']} ({vals[i]} < {vals[i-1]})"
                )

    def test_est_filings_le_raw_filings(self):
        """Estimated category filings should be ≤ raw PERM filings (ratio ≤ 1)."""
        df = _load()
        violations = df[df["est_category_filings"] > df["perm_filings_certified"]]
        assert len(violations) == 0, f"{len(violations)} rows have est_category > raw"

    def test_per_country_allocation_for_oversubscribed(self):
        """IND and CHN should have per-country capped allocation (≤9800 per category)."""
        df = _load()
        for cty in ["IND", "CHN"]:
            sub = df[df["country"] == cty]
            max_alloc = sub["annual_visa_allocation"].max()
            assert max_alloc <= 9800, f"{cty} allocation {max_alloc} exceeds per-country cap"
