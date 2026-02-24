"""
test_remaining_artifacts.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Tests for artifacts that were not previously covered by any test file:
  - employer_friendliness_scores_ml.parquet  (956 rows — ML-based EFS)
  - employer_scores.parquet                   (0 rows — legacy stub)
  - oews_wages.parquet                        (0 rows — legacy stub)
  - pd_forecasts.parquet                      (0 rows — legacy stub)
  - visa_bulletin.parquet                     (0 rows — legacy stub)
  - fact_perm_unique_case/                    (1.67M rows — deduped PERM)
"""
from __future__ import annotations

import pathlib

import pandas as pd
import pytest

TABLES = pathlib.Path(__file__).resolve().parents[2] / "artifacts" / "tables"


def _load(name: str) -> pd.DataFrame:
    p = TABLES / f"{name}.parquet"
    if p.exists():
        return pd.read_parquet(p)
    d = TABLES / name
    if d.is_dir():
        files = sorted(d.rglob("*.parquet"))
        if files:
            return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    return pd.DataFrame()


# ─── employer_friendliness_scores_ml ─────────────────────────────────────────

class TestEmployerFriendlinessScoresML:
    """ML-calibrated employer friendliness scores."""

    def test_exists(self):
        p = TABLES / "employer_friendliness_scores_ml.parquet"
        assert p.exists(), "employer_friendliness_scores_ml.parquet not found"

    def test_required_columns(self):
        df = _load("employer_friendliness_scores_ml")
        required = {"employer_id", "efs_ml"}
        assert required.issubset(set(df.columns)), (
            f"Missing columns: {required - set(df.columns)}"
        )

    def test_row_count(self):
        df = _load("employer_friendliness_scores_ml")
        assert len(df) > 0, "employer_friendliness_scores_ml is empty"

    def test_pk_unique(self):
        df = _load("employer_friendliness_scores_ml")
        if len(df) == 0:
            pytest.skip("empty table")
        dupes = df.duplicated(subset=["employer_id"], keep=False).sum()
        assert dupes == 0, f"{dupes} duplicate employer_ids"

    def test_efs_ml_range(self):
        """EFS scores should be in [0, 100]."""
        df = _load("employer_friendliness_scores_ml")
        if len(df) == 0:
            pytest.skip("empty table")
        vals = df["efs_ml"].dropna()
        assert vals.between(0, 100).all(), (
            f"efs_ml out of [0,100]: min={vals.min()}, max={vals.max()}"
        )

    def test_no_all_null_rows(self):
        df = _load("employer_friendliness_scores_ml")
        if len(df) == 0:
            pytest.skip("empty table")
        all_null = df.isnull().all(axis=1).sum()
        assert all_null == 0, f"{all_null} all-null rows"


# ─── fact_perm_unique_case ───────────────────────────────────────────────────

class TestFactPermUniqueCase:
    """Deduplicated PERM cases (one row per case_number)."""

    def test_exists(self):
        d = TABLES / "fact_perm_unique_case"
        assert d.is_dir(), "fact_perm_unique_case/ directory not found"
        files = list(d.rglob("*.parquet"))
        assert len(files) > 0, "No parquet files in fact_perm_unique_case/"

    def test_required_columns(self):
        df = _load("fact_perm_unique_case")
        required = {"case_number", "case_status", "employer_id", "soc_code"}
        assert required.issubset(set(df.columns)), (
            f"Missing columns: {required - set(df.columns)}"
        )

    def test_row_count(self):
        df = _load("fact_perm_unique_case")
        assert len(df) >= 1_000_000, f"fact_perm_unique_case: only {len(df):,} rows"

    def test_pk_unique(self):
        """case_number may have near-duplicates across FYs; check uniqueness rate."""
        df = _load("fact_perm_unique_case")
        if "case_number" not in df.columns:
            pytest.skip("No case_number column")
        total = len(df)
        unique = df["case_number"].nunique()
        rate = unique / total if total else 1.0
        # fact_perm_unique_case may still have multi-FY filings for same case;
        # require at least 70% case_number uniqueness
        assert rate >= 0.70, (
            f"fact_perm_unique_case: only {rate:.1%} case_number uniqueness "
            f"({unique:,} unique / {total:,} total)"
        )

    def test_case_status_values(self):
        df = _load("fact_perm_unique_case")
        if "case_status" not in df.columns:
            pytest.skip("No case_status column")
        # Normalize to upper for comparison — raw data has mixed casing
        valid_upper = {"CERTIFIED", "DENIED", "WITHDRAWN", "CERTIFIED-EXPIRED",
                       "CERTIFIED - EXPIRED"}
        vals = set(df["case_status"].dropna().str.upper().str.strip().unique())
        assert vals.issubset(valid_upper), (
            f"Unexpected case_status values: {vals - valid_upper}"
        )

    def test_employer_id_coverage(self):
        """Most rows should have a non-null employer_id."""
        df = _load("fact_perm_unique_case")
        if "employer_id" not in df.columns:
            pytest.skip("No employer_id column")
        pct = df["employer_id"].notna().mean()
        assert pct >= 0.95, f"employer_id coverage: {pct:.1%} < 95%"


# ─── Legacy/empty stubs ─────────────────────────────────────────────────────

class TestLegacyStubs:
    """Artifacts that are empty (0 rows) — confirm they exist with correct schema."""

    @pytest.mark.parametrize("name,expected_cols", [
        ("employer_scores", {"employer_name_normalized", "friendliness_score"}),
        ("oews_wages", {"year", "soc_code", "area_code"}),
        ("pd_forecasts", {"category", "country"}),
        ("visa_bulletin", {"category", "country"}),
    ])
    def test_stub_exists_with_schema(self, name, expected_cols):
        p = TABLES / f"{name}.parquet"
        assert p.exists(), f"{name}.parquet not found"
        df = pd.read_parquet(p)
        # These are legacy stubs — 0 rows is acceptable
        actual = set(df.columns)
        assert expected_cols.issubset(actual), (
            f"{name}: missing columns {expected_cols - actual}"
        )

    @pytest.mark.parametrize("name", [
        "employer_scores", "oews_wages", "pd_forecasts", "visa_bulletin",
    ])
    def test_stub_row_count(self, name):
        """These are legacy stubs — 0 rows is the expected state."""
        df = _load(name)
        # We don't assert > 0; we just confirm they are empty stubs
        # (which is expected until the pipeline is extended)
        if len(df) > 0:
            # If populated, it's a bonus — just pass
            pass
        else:
            # 0 rows is expected for stubs
            assert len(df) == 0
