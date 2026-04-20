"""
tests/models/test_pd_forecast_retrograde.py

Validates the Monte Carlo Retrograde-Adjusted (MCRA) forecast model:
- Artifact existence and schema
- Row count and series count consistency
- Retrograde-specific columns present and valid
- Comparison with base model (MCRA ≤ optimistic cutoff dates)
- Model parameters JSON structure
"""
import json
import pathlib

import numpy as np
import pandas as pd
import pytest

TABLES = pathlib.Path("artifacts/tables")
MODELS = pathlib.Path("artifacts/models")


def _load(name: str) -> pd.DataFrame:
    p = TABLES / f"{name}.parquet"
    if not p.exists():
        pytest.skip(f"{name}.parquet not found")
    return pd.read_parquet(p)


# ═══════════════════════════════════════════════════════════════════════════
# Artifact Existence
# ═══════════════════════════════════════════════════════════════════════════

class TestMcraArtifactExists:
    def test_parquet_exists(self):
        assert (TABLES / "pd_forecasts_retrograde.parquet").exists()

    def test_model_json_exists(self):
        assert (MODELS / "pd_forecast_retrograde_model.json").exists()

    def test_original_untouched(self):
        """Original pd_forecasts.parquet must still exist alongside."""
        assert (TABLES / "pd_forecasts.parquet").exists()


# ═══════════════════════════════════════════════════════════════════════════
# Schema & Structure
# ═══════════════════════════════════════════════════════════════════════════

class TestMcraSchema:
    @pytest.fixture(autouse=True)
    def load(self):
        self.df = _load("pd_forecasts_retrograde")

    def test_base_columns_present(self):
        """Must have all columns from the original model."""
        base_cols = {
            "forecast_month", "months_ahead", "chart", "category", "country",
            "projected_cutoff_date", "confidence_low", "confidence_high",
            "velocity_days_per_month", "cumulative_advancement_days",
        }
        assert base_cols.issubset(set(self.df.columns))

    def test_retrograde_columns_present(self):
        """Must have the three new retrograde-specific columns."""
        retro_cols = {"retrograde_prob", "expected_setback_days", "risk_adjusted_velocity"}
        assert retro_cols.issubset(set(self.df.columns))

    def test_row_count_matches_original(self):
        orig = _load("pd_forecasts")
        assert len(self.df) == len(orig), (
            f"MCRA rows ({len(self.df)}) != orig rows ({len(orig)})"
        )

    def test_series_count(self):
        series = self.df.groupby(["chart", "category", "country"]).ngroups
        assert series >= 40, f"Expected ≥40 series, got {series}"

    def test_months_ahead_range(self):
        assert self.df["months_ahead"].min() == 1
        assert self.df["months_ahead"].max() == 24


# ═══════════════════════════════════════════════════════════════════════════
# Retrograde Column Validity
# ═══════════════════════════════════════════════════════════════════════════

class TestRetrogradeColumns:
    @pytest.fixture(autouse=True)
    def load(self):
        self.df = _load("pd_forecasts_retrograde")

    def test_retrograde_prob_range(self):
        """retrograde_prob must be in [0.0, 1.0]."""
        assert self.df["retrograde_prob"].min() >= 0.0
        assert self.df["retrograde_prob"].max() <= 1.0

    def test_expected_setback_non_negative(self):
        assert self.df["expected_setback_days"].min() >= 0.0

    def test_risk_adjusted_velocity_non_negative(self):
        assert self.df["risk_adjusted_velocity"].min() >= 0.0

    def test_no_null_retrograde_cols(self):
        for col in ["retrograde_prob", "expected_setback_days", "risk_adjusted_velocity"]:
            assert self.df[col].notna().all(), f"{col} has null values"


# ═══════════════════════════════════════════════════════════════════════════
# MCRA vs Original Model Comparison
# ═══════════════════════════════════════════════════════════════════════════

class TestMcraVsOriginal:
    @pytest.fixture(autouse=True)
    def load(self):
        self.mcra = _load("pd_forecasts_retrograde")
        self.orig = _load("pd_forecasts")

    def test_mcra_cutoffs_slower_than_optimistic(self):
        """MCRA projected cutoffs should generally be ≤ base model at 24 months.

        The MCRA model incorporates retrograde risk via Monte Carlo simulation,
        so cumulative advancement over 24 months should be equal or less than
        the base model (which applies a deterministic retrogression dampen).
        5% tolerance accounts for MC stochastic variation from random draws.

        Fix M24: MCRA now uses the same 8-year windowed full_history_vel and
        anomaly-weighted rolling means as pd_forecast_v2, eliminating the
        velocity overestimation caused by pre-2016 fiscal-year reset spikes.
        """
        violations = []
        total_series = 0
        for (chart, cat, cty), mcra_g in self.mcra.groupby(["chart", "category", "country"]):
            orig_g = self.orig[
                (self.orig.chart == chart) &
                (self.orig.category == cat) &
                (self.orig.country == cty)
            ]
            if len(orig_g) == 0:
                continue

            mcra_last = mcra_g[mcra_g.months_ahead == 24]
            orig_last = orig_g[orig_g.months_ahead == 24]
            if len(mcra_last) == 0 or len(orig_last) == 0:
                continue

            total_series += 1
            mcra_cum = mcra_last.iloc[0]["cumulative_advancement_days"]
            orig_cum = orig_last.iloc[0]["cumulative_advancement_days"]

            # MCRA should accumulate ≤ orig (with 5% tolerance for MC variance)
            if mcra_cum > orig_cum * 1.05:
                violations.append(
                    f"{chart}/{cat}/{cty}: MCRA cumulative={mcra_cum:.0f} > "
                    f"orig={orig_cum:.0f} * 1.05"
                )

        assert len(violations) == 0, (
            f"{len(violations)}/{total_series} series violated MCRA <= orig:\n"
            + "\n".join(violations)
        )

    def test_same_series_set(self):
        """MCRA and original should have the same set of series."""
        mcra_series = set(
            self.mcra.groupby(["chart", "category", "country"]).groups.keys()
        )
        orig_series = set(
            self.orig.groupby(["chart", "category", "country"]).groups.keys()
        )
        assert mcra_series == orig_series


# ═══════════════════════════════════════════════════════════════════════════
# Model Parameters JSON
# ═══════════════════════════════════════════════════════════════════════════

class TestMcraModelJson:
    @pytest.fixture(autouse=True)
    def load(self):
        path = MODELS / "pd_forecast_retrograde_model.json"
        if not path.exists():
            pytest.skip("Model JSON not found")
        with open(path) as f:
            self.model = json.load(f)

    def test_model_type(self):
        assert self.model["model_type"] == "monte_carlo_retrograde_adjusted"

    def test_version(self):
        assert self.model["version"] == "3.0.0"

    def test_mc_simulations(self):
        assert self.model["mc_simulations"] >= 1000

    def test_series_have_retro_params(self):
        for s in self.model["series"]:
            assert "retro_overall_prob" in s
            assert "retro_overall_severity_days" in s
            assert "retro_regime" in s
            assert s["retro_regime"] in ("low", "moderate", "elevated")

    def test_retro_monthly_prob_keys(self):
        for s in self.model["series"]:
            probs = s["retro_monthly_prob"]
            # Should have entries for all 12 months
            assert len(probs) == 12
            for m in range(1, 13):
                assert str(m) in probs

    def test_eb2_ind_has_retrograde_data(self):
        """EB2/IND must have non-zero retrograde probability."""
        eb2 = [
            s for s in self.model["series"]
            if s["chart"] == "DFF" and s["category"] == "EB2" and s["country"] == "IND"
        ]
        assert len(eb2) == 1
        assert eb2[0]["retro_overall_prob"] > 0.0
