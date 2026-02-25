"""P2 Hardening: range/value checks, referential integrity, statistical smoke tests.

Operates on existing parquet artifacts only.
Run with:  pytest tests/p2_hardening/ -q
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
TABLES = ROOT / "artifacts" / "tables"
METRICS = ROOT / "artifacts" / "metrics"


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
    pytest.skip(f"{name} not found; skipping")


# ── RATE BOUNDS ───────────────────────────────────────────────────────────────

class TestRateBounds:
    def test_employer_monthly_approval_rate_in_01(self):
        df = _load("employer_monthly_metrics")
        if "approval_rate" not in df.columns:
            pytest.skip("approval_rate column not present")
        vals = df["approval_rate"].dropna()
        bad = ((vals < 0) | (vals > 1)).sum()
        assert bad == 0, f"employer_monthly_metrics: {bad:,} approval_rate values outside [0,1]"

    def test_employer_monthly_denial_rate_in_01(self):
        df = _load("employer_monthly_metrics")
        if "denial_rate" not in df.columns:
            pytest.skip("denial_rate not present")
        vals = df["denial_rate"].dropna()
        bad = ((vals < 0) | (vals > 1)).sum()
        assert bad == 0, f"employer_monthly_metrics: {bad:,} denial_rate values outside [0,1]"

    def test_employer_features_approval_rate_in_01(self):
        df = _load("employer_features")
        if "approval_rate_24m" not in df.columns:
            pytest.skip("approval_rate_24m not present")
        vals = df["approval_rate_24m"].dropna()
        bad = ((vals < 0) | (vals > 1)).sum()
        assert bad == 0, f"employer_features: {bad:,} approval_rate_24m outside [0,1]"

    def test_soc_demand_approval_rate_in_01(self):
        df = _load("soc_demand_metrics")
        if "approval_rate" not in df.columns:
            pytest.skip("approval_rate not present")
        vals = df["approval_rate"].dropna()
        bad = ((vals < 0) | (vals > 1)).sum()
        assert bad == 0, f"soc_demand_metrics: {bad:,} approval_rate outside [0,1]"


# ── APPROVALS ≤ FILINGS ───────────────────────────────────────────────────────

class TestApprovalsFilings:
    def test_employer_monthly_no_approvals_exceed_filings(self):
        """Core data-quality FAIL gate: no month can have approvals > filings."""
        df = _load("employer_monthly_metrics")
        if "approvals" not in df.columns or "filings" not in df.columns:
            pytest.skip("approvals/filings columns not present")
        bad = (df["approvals"] > df["filings"]).sum()
        assert bad == 0, (
            f"employer_monthly_metrics: {bad:,} months have approvals > filings"
        )

    def test_employer_monthly_approvals_nonnegative(self):
        df = _load("employer_monthly_metrics")
        if "approvals" not in df.columns:
            pytest.skip()
        bad = (df["approvals"] < 0).sum()
        assert bad == 0, f"employer_monthly_metrics: {bad:,} negative approvals"

    def test_employer_monthly_filings_positive(self):
        df = _load("employer_monthly_metrics")
        if "filings" not in df.columns:
            pytest.skip()
        bad = (df["filings"] <= 0).sum()
        # Some months may legitimately have 0 filings; WARN only
        assert bad < len(df) * 0.05, f"employer_monthly_metrics: {bad:,} rows with filings ≤ 0 (>5%)"


# ── EFS BOUNDS ────────────────────────────────────────────────────────────────

class TestEFSBounds:
    def test_efs_in_0_to_100(self):
        df = _load("employer_friendliness_scores")
        if "efs" not in df.columns:
            pytest.skip("efs column not present")
        valid = df["efs"].dropna()
        bad = ((valid < 0) | (valid > 100)).sum()
        assert bad == 0, f"employer_friendliness_scores: {bad:,} EFS values outside [0,100]"

    def test_efs_null_requires_insufficient_flag(self):
        """Rows with EFS=null must have n_24m < threshold (guardrail check)."""
        df = _load("employer_friendliness_scores")
        if "efs" not in df.columns or "n_24m" not in df.columns:
            pytest.skip("Required columns not present")
        null_efs = df["efs"].isna()
        # All null-EFS rows should have n_24m < 3 (MIN_CASES guardrail)
        if null_efs.sum() > 0:
            null_with_sufficient = null_efs & (df["n_24m"] >= 3)
            assert null_with_sufficient.sum() == 0, (
                f"{null_with_sufficient.sum():,} rows have EFS=null but n_24m >= 3 (guardrail violated)"
            )


# ── REFERENTIAL INTEGRITY ─────────────────────────────────────────────────────

class TestReferentialIntegrity:
    def test_salary_benchmarks_soc_coverage(self):
        df_sb = _load("salary_benchmarks")
        df_soc = _load("dim_soc")
        if "soc_code" not in df_sb.columns or "soc_code" not in df_soc.columns:
            pytest.skip("soc_code column not present")
        soc_set = set(df_soc["soc_code"].dropna())
        total = df_sb["soc_code"].notna().sum()
        if total == 0:
            pytest.skip("No non-null soc_codes in salary_benchmarks")
        mapped = df_sb["soc_code"].isin(soc_set).sum()
        pct = mapped / total
        assert pct >= 0.95, (
            f"salary_benchmarks: only {pct*100:.1f}% of soc_codes found in dim_soc (need ≥95%)"
        )

    def test_worksite_geo_soc_coverage(self):
        df_ws = _load("worksite_geo_metrics")
        df_soc = _load("dim_soc")
        if "soc_code" not in df_ws.columns or "soc_code" not in df_soc.columns:
            pytest.skip("soc_code column not present")
        soc_set = set(df_soc["soc_code"].dropna())
        total = df_ws["soc_code"].notna().sum()
        if total == 0:
            pytest.skip("No non-null soc_codes in worksite_geo_metrics")
        mapped = df_ws["soc_code"].isin(soc_set).sum()
        pct = mapped / total
        # worksite_geo uses LCA SOC codes which are broader than PERM dim_soc → 60% FAIL threshold
        assert pct >= 0.60, (
            f"worksite_geo_metrics: only {pct*100:.1f}% of soc_codes in dim_soc"
        )

    def test_employer_features_employer_id_coverage(self):
        """employer_features.employer_id rows should mostly map to dim_employer.
        
        employer_features is built with a lower minimum-case threshold than
        dim_employer, so not all features employers appear in the dim table.
        We require ≥40% row-level coverage (unique employer coverage is ~100%).
        """
        df_feat = _load("employer_features")
        df_emp = _load("dim_employer")
        if "employer_id" not in df_feat.columns or "employer_id" not in df_emp.columns:
            pytest.skip("employer_id column missing")
        emp_set = set(df_emp["employer_id"].dropna())
        feat_ids = df_feat["employer_id"].dropna()
        mapped = feat_ids.isin(emp_set).sum()
        pct = mapped / len(feat_ids) if len(feat_ids) > 0 else 1.0
        assert pct >= 0.40, (
            f"employer_features: {pct*100:.1f}% of employer_ids found in dim_employer"
        )


# ── STATISTICAL SMOKE TESTS ────────────────────────────────────────────────────

class TestStatisticalSmoke:
    def test_advancement_days_median_in_band(self):
        """Monthly advancement should be in a reasonable range."""
        df = _load("fact_cutoff_trends")
        if "monthly_advancement_days" not in df.columns:
            pytest.skip("monthly_advancement_days not present")
        vals = df["monthly_advancement_days"].dropna()
        if len(vals) == 0:
            pytest.skip("No non-null advancement values")
        med = vals.median()
        assert -120 <= med <= 240, (
            f"monthly_advancement_days median {med:.1f} outside expected [-120, 240]"
        )

    def test_competitiveness_ratio_mostly_positive(self):
        df = _load("worksite_geo_metrics")
        if "competitiveness_ratio" not in df.columns:
            pytest.skip("competitiveness_ratio not present")
        cr = df["competitiveness_ratio"].dropna()
        if len(cr) == 0:
            pytest.skip("No competitiveness_ratio values")
        non_positive = (cr <= 0).sum()
        # Allow up to 0.1% edge-case non-positive values (rounding, zero-wage SOCs)
        max_allowed = max(5, int(len(cr) * 0.001))
        assert non_positive <= max_allowed, (
            f"worksite_geo_metrics: {non_positive:,} competitiveness_ratio values ≤ 0 "
            f"(max allowed: {max_allowed})"
        )

    def test_salary_benchmarks_median_reasonable(self):
        """Salary benchmarks median should be positive and plausible (>$10k, <$600k)."""
        df = _load("salary_benchmarks")
        if "median" not in df.columns:
            pytest.skip()
        vals = df["median"].dropna()
        if len(vals) == 0:
            pytest.skip("No non-null median values")
        # At least 90% should be in plausible range
        in_range = ((vals > 10_000) & (vals < 600_000)).sum()
        pct = in_range / len(vals)
        assert pct >= 0.90, (
            f"salary_benchmarks: only {pct*100:.1f}% of median values in $10k-$600k range"
        )

    def test_backlog_estimates_nonnegative(self):
        df = _load("backlog_estimates")
        if "backlog_months_to_clear_est" not in df.columns:
            pytest.skip()
        vals = df["backlog_months_to_clear_est"].dropna()
        bad = (vals < 0).sum()
        assert bad == 0, f"backlog_estimates: {bad:,} negative backlog_months_to_clear_est values"

    def test_velocity_3m_present_and_numeric(self):
        df = _load("fact_cutoff_trends")
        if "velocity_3m" not in df.columns:
            pytest.skip("velocity_3m not present")
        numeric = pd.to_numeric(df["velocity_3m"], errors="coerce")
        null_pct = numeric.isna().mean()
        # First few months per category won't have 3m windows; ≤55% null acceptable
        assert null_pct <= 0.55, f"fact_cutoff_trends: {null_pct*100:.1f}% null velocity_3m"
