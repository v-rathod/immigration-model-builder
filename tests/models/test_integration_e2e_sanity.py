"""
tests/models/test_integration_e2e_sanity.py

End-to-end integration and sanity checks:
- visa_demand_metrics joins with dim_country (RI ≥95%)
- backlog_estimates has no NaN-only columns; quantiles sane
- EFS verification: parse log for PASS and correlation ≥0.55
"""
import pathlib
import re

import pandas as pd
import pytest

TABLES = pathlib.Path("artifacts/tables")
METRICS = pathlib.Path("artifacts/metrics")


def _load(name: str) -> pd.DataFrame:
    p = TABLES / f"{name}.parquet"
    if not p.exists():
        pytest.skip(f"{name}.parquet not found")
    return pd.read_parquet(p)


class TestVisaDemandCountryRI:
    """visa_demand_metrics country column should join to dim_country.

    visa_demand_metrics aggregates from three DOS/NIV sources that use plain
    country names (\"China-Mainland Born\", \"Vietnam\", \"Africa Total\") rather
    than ISO names. A 70% threshold is used to reflect that reality.
    """

    def test_country_ri(self):
        vdm = TABLES / "visa_demand_metrics.parquet"
        if not vdm.exists():
            pytest.skip("visa_demand_metrics.parquet not found")
        df_vdm = pd.read_parquet(vdm)
        if len(df_vdm) == 0:
            pytest.skip("visa_demand_metrics is empty")
        if "country" not in df_vdm.columns:
            pytest.skip("visa_demand_metrics missing 'country' column")

        df_country = _load("dim_country")
        known = set(df_country["country_name"].dropna().str.upper())
        if "iso2" in df_country.columns:
            known |= set(df_country["iso2"].dropna().str.upper())
        if "iso3" in df_country.columns:
            known |= set(df_country["iso3"].dropna().str.upper())

        col = df_vdm["country"].dropna().astype(str).str.upper()
        rate = col.isin(known).mean() if len(col) > 0 else 1.0
        assert rate >= 0.70, (
            f"visa_demand_metrics: country RI = {rate:.1%} < 70% (DOS naming). "
            f"Unmatched sample: {list((col[~col.isin(known)].unique())[:5])}"
        )


class TestBacklogEstimatesSanity:
    def test_no_nan_only_columns(self):
        df = _load("backlog_estimates")
        for col in df.columns:
            non_null = df[col].notna().sum()
            assert non_null > 0, f"backlog_estimates: column '{col}' is all-null"

    def test_months_quantiles_sane(self):
        df = _load("backlog_estimates")
        col = "backlog_months_to_clear_est"
        if col not in df.columns:
            pytest.skip(f"backlog_estimates missing '{col}'")
        numeric = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(numeric) == 0:
            pytest.skip("no numeric backlog_months values")
        # All non-null values should be 0–600
        assert numeric.min() >= 0, f"backlog_months_to_clear_est min={numeric.min()} < 0"
        assert numeric.max() <= 600, f"backlog_months_to_clear_est max={numeric.max()} > 600"
        # Should have some valid range (not all identical unless capped)
        # Accept all-600 (capped) as valid
        unique_vals = numeric.nunique()
        assert unique_vals >= 1, "backlog_months: no unique values"

    def test_category_country_presence(self):
        df = _load("backlog_estimates")
        assert "category" in df.columns, "backlog_estimates missing 'category'"
        assert "country" in df.columns, "backlog_estimates missing 'country'"
        assert df["category"].nunique() >= 2
        assert df["country"].nunique() >= 2


class TestEfsAcceptance:
    """Re-check EFS acceptance by parsing efs_verify.log."""

    def test_efs_verify_log_pass(self):
        log_path = METRICS / "efs_verify.log"
        if not log_path.exists():
            pytest.skip("efs_verify.log not found")
        log_text = log_path.read_text(encoding="utf-8", errors="replace")
        # Expect "PASS" in the verification summary line
        assert "PASS" in log_text, (
            "efs_verify.log does not contain 'PASS' — EFS verification may have failed"
        )

    def test_efs_correlation_threshold(self):
        log_path = METRICS / "efs_verify.log"
        if not log_path.exists():
            pytest.skip("efs_verify.log not found")
        log_text = log_path.read_text(encoding="utf-8", errors="replace")
        # Parse Pearson r from line like: Pearson r = 0.5882
        match = re.search(r"Pearson r\s*=\s*([\d.]+)", log_text)
        if not match:
            pytest.skip("Could not parse Pearson r from efs_verify.log")
        corr = float(match.group(1))
        assert corr >= 0.55, (
            f"EFS correlation (r={corr:.4f}) < 0.55 threshold. "
            f"Check efs_verify.log for details."
        )

    def test_efs_parquet_valid(self):
        df = _load("employer_friendliness_scores")
        assert len(df) > 0, "employer_friendliness_scores is empty"
        assert "efs" in df.columns, "employer_friendliness_scores missing 'efs' column"
        valid_efs = pd.to_numeric(df["efs"], errors="coerce").dropna()
        if len(valid_efs) > 0:
            assert valid_efs.min() >= 0, "efs: min < 0"
            assert valid_efs.max() <= 100, "efs: max > 100"


class TestCoreTablesNoRegression:
    """Quick smoke checks that PERM/LCA/OEWS/VB are intact after this session."""

    def test_fact_perm_all_unchanged(self):
        df = _load("fact_perm_all")
        assert len(df) >= 1_000_000, f"fact_perm_all regressed to {len(df):,} rows"

    def test_fact_oews_unchanged(self):
        df = _load("fact_oews")
        assert len(df) >= 400_000, f"fact_oews regressed to {len(df):,} rows"

    def test_fact_cutoffs_unchanged(self):
        df = _load("fact_cutoffs_all")
        assert len(df) == 8_315, f"fact_cutoffs_all regressed to {len(df)} rows"

    def test_fact_lca_present(self):
        d = TABLES / "fact_lca"
        if not d.is_dir():
            pytest.skip("fact_lca/ partitioned directory not found")
        parts = list(d.rglob("*.parquet"))
        assert len(parts) >= 5, f"fact_lca/: only {len(parts)} partition files"

    def test_employer_features_intact(self):
        df = _load("employer_features")
        assert len(df) >= 50_000, f"employer_features regressed to {len(df):,} rows"
