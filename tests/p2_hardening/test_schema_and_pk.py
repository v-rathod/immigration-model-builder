"""P2 Hardening: schema presence, PK uniqueness, monotonic percentiles, row count regression.

These tests operate on existing parquet artifacts only (no raw re-ingestion).
Run with:  pytest tests/p2_hardening/ -q
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent.parent
TABLES = ROOT / "artifacts" / "tables"
METRICS = ROOT / "artifacts" / "metrics"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load(name: str) -> pd.DataFrame:
    p_file = TABLES / f"{name}.parquet"
    p_dir = TABLES / name
    if p_file.exists():
        return pd.read_parquet(p_file)
    if p_dir.exists():
        files = sorted(p_dir.rglob("*.parquet"))
        chunks = []
        for pf in files:
            ch = pd.read_parquet(pf)
            for part in pf.parts:
                if "=" in part:
                    col, val = part.split("=", 1)
                    if col not in ch.columns:
                        ch[col] = val
            chunks.append(ch)
        return pd.concat(chunks, ignore_index=True) if chunks else pd.DataFrame()
    pytest.skip(f"{name} not found; skipping test")


def _row_count(name: str) -> int:
    p_file = TABLES / f"{name}.parquet"
    p_dir = TABLES / name
    if p_file.exists():
        return pq.read_metadata(p_file).num_rows
    if p_dir.exists():
        total = 0
        for pf in sorted(p_dir.rglob("*.parquet")):
            total += pq.read_metadata(pf).num_rows
        return total
    return -1


# ── SCHEMA TESTS ──────────────────────────────────────────────────────────────

class TestSchema:
    def test_fact_cutoffs_all_schema(self):
        df = _load("fact_cutoffs_all")
        required = ["bulletin_year", "bulletin_month", "chart", "category", "country", "cutoff_date"]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"fact_cutoffs_all missing columns: {missing}"

    def test_fact_cutoff_trends_schema(self):
        df = _load("fact_cutoff_trends")
        required = ["bulletin_year", "bulletin_month", "category", "country",
                    "queue_position_days", "monthly_advancement_days", "retrogression_flag"]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"fact_cutoff_trends missing columns: {missing}"

    def test_employer_monthly_metrics_schema(self):
        df = _load("employer_monthly_metrics")
        required = ["employer_id", "month", "filings", "approvals", "approval_rate"]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"employer_monthly_metrics missing columns: {missing}"

    def test_salary_benchmarks_schema(self):
        df = _load("salary_benchmarks")
        required = ["soc_code", "p10", "p25", "median", "p75", "p90"]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"salary_benchmarks missing columns: {missing}"

    def test_worksite_geo_metrics_schema(self):
        df = _load("worksite_geo_metrics")
        required = ["state", "filings_count"]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"worksite_geo_metrics missing columns: {missing}"

    def test_employer_friendliness_scores_schema(self):
        df = _load("employer_friendliness_scores")
        required = ["employer_id", "scope", "efs", "efs_tier"]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"employer_friendliness_scores missing columns: {missing}"

    def test_soc_demand_metrics_schema(self):
        df = _load("soc_demand_metrics")
        required = ["soc_code", "filings_count", "approval_rate"]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"soc_demand_metrics missing columns: {missing}"

    def test_backlog_estimates_schema(self):
        df = _load("backlog_estimates")
        required = ["bulletin_year", "bulletin_month", "chart", "category", "country",
                    "advancement_days_12m_avg", "backlog_months_to_clear_est"]
        missing = [c for c in required if c not in df.columns]
        assert not missing, f"backlog_estimates missing columns: {missing}"

    def test_processing_times_trends_exists(self):
        """processing_times_trends may be empty stub, but must exist."""
        p = TABLES / "processing_times_trends.parquet"
        assert p.exists(), "processing_times_trends.parquet not found"

    def test_dim_tables_present(self):
        for dim in ["dim_employer", "dim_soc", "dim_area", "dim_country", "dim_visa_class"]:
            p = TABLES / f"{dim}.parquet"
            assert p.exists(), f"{dim}.parquet not found"


# ── PK UNIQUENESS ─────────────────────────────────────────────────────────────

class TestPKUniqueness:
    def test_fact_cutoffs_all_pk_unique(self):
        df = _load("fact_cutoffs_all")
        pk = ["bulletin_year", "bulletin_month", "chart", "category", "country"]
        pk_avail = [c for c in pk if c in df.columns]
        dups = df.duplicated(subset=pk_avail).sum()
        assert dups == 0, f"fact_cutoffs_all has {dups:,} PK duplicates"

    def test_fact_cutoff_trends_pk_unique(self):
        df = _load("fact_cutoff_trends")
        # PK includes chart (DFF vs FA creates two rows per bulletin_year/month/category/country)
        pk = ["bulletin_year", "bulletin_month", "chart", "category", "country"]
        pk_avail = [c for c in pk if c in df.columns]
        dups = df.duplicated(subset=pk_avail).sum()
        assert dups == 0, f"fact_cutoff_trends has {dups:,} PK duplicates"

    def test_category_movement_metrics_pk_unique(self):
        df = _load("category_movement_metrics")
        # PK includes chart (DFF vs FA creates two rows per bulletin_year/month/category/country)
        pk = ["bulletin_year", "bulletin_month", "chart", "category", "country"]
        pk_avail = [c for c in pk if c in df.columns]
        if pk_avail:
            dups = df.duplicated(subset=pk_avail).sum()
            assert dups == 0, f"category_movement_metrics has {dups:,} PK duplicates"

    def test_dim_soc_pk_unique(self):
        df = _load("dim_soc")
        assert "soc_code" in df.columns
        dups = df.duplicated(subset=["soc_code"]).sum()
        assert dups == 0, f"dim_soc has {dups:,} duplicate soc_code values"

    def test_dim_area_pk_unique(self):
        df = _load("dim_area")
        assert "area_code" in df.columns
        dups = df.duplicated(subset=["area_code"]).sum()
        assert dups == 0, f"dim_area has {dups:,} duplicate area_code values"

    def test_dim_country_pk_unique(self):
        df = _load("dim_country")
        # dim_country uses iso2 as the primary key (no country_code column)
        pk_col = "iso2" if "iso2" in df.columns else "country_name"
        dups = df.duplicated(subset=[pk_col]).sum()
        assert dups == 0, f"dim_country has {dups:,} duplicate {pk_col} values"


# ── MONOTONIC PERCENTILES ─────────────────────────────────────────────────────

class TestMonotonicPercentiles:
    PCT_COLS = ["p10", "p25", "median", "p75", "p90"]

    def test_salary_benchmarks_monotonic_zero_violations(self):
        """After enforce_monotonic(), there must be 0 violations."""
        df = _load("salary_benchmarks")
        violations = 0
        pct_avail = [c for c in self.PCT_COLS if c in df.columns]
        for i in range(len(pct_avail) - 1):
            lo, hi = pct_avail[i], pct_avail[i + 1]
            mask = df[lo].notna() & df[hi].notna()
            bad = (df.loc[mask, lo] > df.loc[mask, hi]).sum()
            violations += bad
        assert violations == 0, f"salary_benchmarks has {violations} monotonic violations (p10≤p25≤…≤p90)"

    def test_salary_benchmarks_percentile_count(self):
        df = _load("salary_benchmarks")
        present = [c for c in self.PCT_COLS if c in df.columns]
        assert len(present) == 5, f"Expected 5 percentile cols, found {len(present)}: {present}"


# ── ROW COUNT REGRESSION (golden samples) ─────────────────────────────────────

class TestGoldenRowCounts:
    """Assert known stable row counts from prior sessions."""

    def test_fact_cutoffs_all_rows(self):
        assert _row_count("fact_cutoffs_all") == 8_315

    def test_fact_cutoff_trends_rows(self):
        assert _row_count("fact_cutoff_trends") == 8_315

    def test_category_movement_metrics_rows(self):
        assert _row_count("category_movement_metrics") == 8_315

    def test_backlog_estimates_rows(self):
        assert _row_count("backlog_estimates") == 8_315

    def test_employer_monthly_metrics_rows(self):
        rows = _row_count("employer_monthly_metrics")
        assert rows >= 74_350, f"employer_monthly_metrics: {rows:,} rows < 74,350 minimum"

    def test_dim_employer_rows(self):
        rows = _row_count("dim_employer")
        assert rows >= 60_000, f"dim_employer: {rows:,} rows < 60,000 minimum (requires fact_perm expansion)"

    def test_dim_soc_rows(self):
        rows = _row_count("dim_soc")
        assert rows >= 1_396, f"dim_soc: {rows:,} rows < 1,396 minimum (canonical SOC 2018 crosswalk)"

    def test_dim_area_rows(self):
        assert _row_count("dim_area") == 587

    def test_dim_country_rows(self):
        assert _row_count("dim_country") == 249

    def test_salary_benchmarks_rows(self):
        rows = _row_count("salary_benchmarks")
        assert rows == 224_047, f"salary_benchmarks: {rows:,} rows ≠ 224,047"

    def test_employer_features_rows(self):
        rows = _row_count("employer_features")
        assert rows >= 70_000, f"employer_features: {rows:,} < 70,000 minimum"

    def test_employer_friendliness_scores_rows(self):
        rows = _row_count("employer_friendliness_scores")
        assert rows >= 43_604, f"employer_friendliness_scores: {rows:,} < 43,604 minimum"


# ── PARTITION STRUCTURE ────────────────────────────────────────────────────────

class TestPartitions:
    def test_fact_perm_has_partitions(self):
        perm_dir = TABLES / "fact_perm"
        assert perm_dir.exists(), "fact_perm/ directory not found"
        files = list(perm_dir.rglob("*.parquet"))
        assert len(files) >= 10, f"fact_perm has only {len(files)} partition files; expected ≥10"

    def test_fact_lca_has_partitions(self):
        lca_dir = TABLES / "fact_lca"
        assert lca_dir.exists(), "fact_lca/ directory not found"
        files = list(lca_dir.rglob("*.parquet"))
        assert len(files) >= 10, f"fact_lca has only {len(files)} partition files; expected ≥10"

    def test_fact_cutoffs_leaf_count(self):
        cutoffs_dir = TABLES / "fact_cutoffs"
        assert cutoffs_dir.exists(), "fact_cutoffs/ not found"
        leaves = list(cutoffs_dir.rglob("*.parquet"))
        # 280 partition files (168 unique bulletin_year/month dirs, some with multiple parts)
        assert len(leaves) >= 168, f"fact_cutoffs has only {len(leaves)} leaves; expected ≥168"
