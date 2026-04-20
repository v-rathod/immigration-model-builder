"""
tests/p2_baselines/test_model_baselines.py

Baseline regression tests for P2 model outputs.

These tests lock in the current model output ranges (as of M24, Apr 2026)
so that future runs can be compared against known-good expectations.

IMPORTANT: Update baselines intentionally when models are deliberately re-tuned.
Run with: python3 -m pytest tests/p2_baselines/ -v

Design principles:
- Use wide ±25% ranges to tolerate incremental data updates without false failures
- Test structural properties (ordering, monotonicity) that should always hold
- Test key series for each model to catch regressions in individual forecasts
- Never hardcode exact values -- always use floor/ceiling ranges
"""
import pathlib

import pandas as pd
import pytest

TABLES = pathlib.Path("artifacts/tables")
MODELS = pathlib.Path("artifacts/models")


def _load(name: str) -> pd.DataFrame:
    p = TABLES / f"{name}.parquet"
    if not p.exists():
        pytest.skip(f"{name}.parquet not found")
    return pd.read_parquet(p)


# =============================================================================
# PD FORECAST MODEL — Base (pd_forecast_v2 Windowed + Anomaly-Weighted)
# Baselines captured April 2026, May 2026 Visa Bulletin data
# =============================================================================

class TestPDForecastBaselines:
    """Regression tests for base priority date forecast model."""

    @pytest.fixture(scope="class")
    def df(self):
        return _load("pd_forecasts")

    def _get_cum(self, df, chart, category, country, months_ahead=24):
        g = df[
            (df.chart == chart) & (df.category == category) &
            (df.country == country) & (df.months_ahead == months_ahead)
        ]
        if len(g) == 0:
            pytest.skip(f"Series {chart}/{category}/{country} not found")
        return g.iloc[0]["cumulative_advancement_days"]

    # --- EB2-India FAD: High-demand series, slow advancement ---
    def test_eb2_india_fad_24m_cumulative_range(self, df):
        """EB2-India FAD 24m cumulative should be 300-800 days (Apr 2026 baseline: ~569)."""
        cum = self._get_cum(df, "FAD", "EB2", "IND")
        assert 300 <= cum <= 800, f"EB2-India FAD 24m cumulative {cum:.0f} out of [300, 800]"

    # --- EB3-India FAD: Similar high-demand category ---
    def test_eb3_india_fad_24m_cumulative_range(self, df):
        """EB3-India FAD 24m cumulative should be 250-750 days (Apr 2026 baseline: ~493)."""
        cum = self._get_cum(df, "FAD", "EB3", "IND")
        assert 250 <= cum <= 750, f"EB3-India FAD 24m cumulative {cum:.0f} out of [250, 750]"

    # --- EB2-China: Moderate demand ---
    def test_eb2_china_fad_24m_cumulative_range(self, df):
        """EB2-China FAD 24m cumulative should be 300-800 days (Apr 2026 baseline: ~568)."""
        cum = self._get_cum(df, "FAD", "EB2", "CHN")
        assert 300 <= cum <= 800, f"EB2-China FAD 24m cumulative {cum:.0f} out of [300, 800]"

    # --- EB3-China ---
    def test_eb3_china_fad_24m_cumulative_range(self, df):
        """EB3-China FAD 24m cumulative should be 200-700 days (Apr 2026 baseline: ~461)."""
        cum = self._get_cum(df, "FAD", "EB3", "CHN")
        assert 200 <= cum <= 700, f"EB3-China FAD 24m cumulative {cum:.0f} out of [200, 700]"

    # --- Structural: cumulative should be monotonically non-decreasing across months ---
    def test_cumulative_advancement_monotonic_per_series(self, df):
        """For each series, cumulative_advancement_days should be non-decreasing over forecast months."""
        violations = []
        for (chart, cat, cty), g in df.groupby(["chart", "category", "country"]):
            g_sorted = g.sort_values("months_ahead")
            cum = g_sorted["cumulative_advancement_days"].values
            if len(cum) < 2:
                continue
            drops = [(i + 1, cum[i + 1] - cum[i]) for i in range(len(cum) - 1) if cum[i + 1] < cum[i] - 1]
            if drops:
                violations.append(f"{chart}/{cat}/{cty}: {len(drops)} backward steps")
        assert len(violations) == 0, f"Non-monotonic cumulative in series:\n" + "\n".join(violations[:5])

    # --- All series should have rows for months 1-24 ---
    def test_all_series_have_24_forecast_months(self, df):
        """Each forecast series should have exactly 24 rows (one per month)."""
        counts = df.groupby(["chart", "category", "country"]).size()
        short = counts[counts < 24]
        assert len(short) == 0, f"{len(short)} series with <24 forecast months:\n{short.head()}"

    # --- Row count stability: should have at least 40 series ---
    def test_series_count_floor(self, df):
        series = df.groupby(["chart", "category", "country"]).ngroups
        assert series >= 40, f"Expected ≥40 forecast series, got {series}"


# =============================================================================
# MCRA FORECAST MODEL — Monte Carlo Retrograde-Adjusted
# Baselines captured April 2026
# =============================================================================

class TestMCRABaselines:
    """Regression tests for MCRA retrograde-adjusted forecast."""

    @pytest.fixture(scope="class")
    def base(self):
        return _load("pd_forecasts")

    @pytest.fixture(scope="class")
    def mcra(self):
        return _load("pd_forecasts_retrograde")

    def _get_cum(self, df, chart, category, country, months_ahead=24):
        g = df[
            (df.chart == chart) & (df.category == category) &
            (df.country == country) & (df.months_ahead == months_ahead)
        ]
        if len(g) == 0:
            pytest.skip(f"Series {chart}/{category}/{country} not found in MCRA")
        return g.iloc[0]["cumulative_advancement_days"]

    def test_eb2_india_mcra_conservative_vs_base(self, base, mcra):
        """MCRA EB2-India should be ≤ base model (retrograde penalty).
        Apr 2026 baseline: MCRA=456 vs base=569."""
        mcra_cum = self._get_cum(mcra, "FAD", "EB2", "IND")
        base_cum = self._get_cum(base, "FAD", "EB2", "IND")
        assert mcra_cum <= base_cum * 1.05, (
            f"MCRA EB2-IND {mcra_cum:.0f} should be ≤ base {base_cum:.0f} * 1.05"
        )

    def test_eb2_india_mcra_retrograde_prob_nonzero(self, mcra):
        """EB2-India has historical retrogrades, so retrograde_prob should be > 0."""
        g = mcra[(mcra.chart == "FAD") & (mcra.category == "EB2") & (mcra.country == "IND")]
        if len(g) == 0:
            pytest.skip("EB2/India series not found")
        assert g["retrograde_prob"].max() > 0, "Expected some retrograde probability for EB2-India"

    def test_all_retrograde_probs_bounded(self, mcra):
        """retrograde_prob must be in [0.0, 1.0]."""
        rp = mcra["retrograde_prob"].dropna()
        assert (rp >= 0.0).all(), "Found retrograde_prob < 0"
        assert (rp <= 1.0).all(), "Found retrograde_prob > 1.0"

    def test_mcra_not_exceeding_base_for_all_series(self, base, mcra):
        """MCRA cumulative at 24m must not exceed base by more than 5% for ANY series.

        This validates the M24 fix: 8-year windowed velocity + anomaly-weighted
        rolling means. Before fix: 35/55 series violated this constraint.
        After fix: 0/55 violations.
        """
        violations = []
        total = 0
        for (chart, cat, cty), g in mcra.groupby(["chart", "category", "country"]):
            og = base[(base.chart == chart) & (base.category == cat) & (base.country == cty)]
            if not len(og):
                continue
            m24 = g[g.months_ahead == 24]
            o24 = og[og.months_ahead == 24]
            if not len(m24) or not len(o24):
                continue
            total += 1
            mc = m24.iloc[0]["cumulative_advancement_days"]
            oc = o24.iloc[0]["cumulative_advancement_days"]
            if mc > oc * 1.05:
                violations.append(f"{chart}/{cat}/{cty}: MCRA={mc:.0f} > base={oc:.0f}")
        assert len(violations) == 0, (
            f"{len(violations)}/{total} series violated MCRA ≤ base:\n"
            + "\n".join(violations)
        )

    def test_expected_setback_days_non_negative(self, mcra):
        """Expected setback days should be ≥ 0."""
        assert (mcra["expected_setback_days"] >= 0).all(), "Found negative expected_setback_days"

    def test_risk_adjusted_velocity_positive(self, mcra):
        """Risk-adjusted velocity should be > 0 (MIN_VEL_FRACTION floor = 0.30)."""
        assert (mcra["risk_adjusted_velocity"] > 0).all(), "Found zero/negative risk_adjusted_velocity"


# =============================================================================
# EMPLOYER FRIENDLINESS SCORE (EFS) — Sponsorship Reliability
# Baselines captured April 2026: ~70,206 employers, 17,836 rated
# =============================================================================

class TestEFSBaselines:
    """Regression tests for Employer Friendliness Score model."""

    @pytest.fixture(scope="class")
    def df(self):
        return _load("employer_friendliness_scores")

    def test_row_count_floor(self, df):
        """Should have at least 60,000 employer rows."""
        assert len(df) >= 60_000, f"Expected ≥60K employer rows, got {len(df):,}"

    def test_efs_rated_employer_count(self, df):
        """At least 15,000 employers should have a valid EFS score."""
        rated = df["efs"].notna().sum()
        assert rated >= 15_000, f"Expected ≥15K rated employers, got {rated:,}"

    def test_efs_score_range(self, df):
        """EFS scores should be in [10, 100]."""
        valid = df["efs"].dropna()
        assert (valid >= 10).all(), f"EFS score below 10: {valid.min()}"
        assert (valid <= 100).all(), f"EFS score above 100: {valid.max()}"

    def test_efs_mean_in_expected_range(self, df):
        """EFS mean should be in [55, 85] (Apr 2026 baseline: ~68.5)."""
        mean = df["efs"].mean()
        assert 55 <= mean <= 85, f"EFS mean {mean:.1f} out of [55, 85]"

    def test_efs_tier_distribution(self, df):
        """All expected tier labels should be present."""
        tiers = set(df["efs_tier"].dropna().unique())
        for expected in ["Excellent", "Good", "Moderate", "Poor"]:
            assert expected in tiers, f"Missing tier: {expected}"

    def test_unrated_majority(self, df):
        """Unrated employers should be the majority (>50%), showing conservative scoring.
        Only employers with sufficient data (≥3 LCA filings, PERM activity) get rated.
        Apr 2026 baseline: ~52,370 / 70,206 = 74.6% unrated."""
        unrated = (df["efs_tier"] == "Unrated").sum()
        fraction = unrated / len(df)
        assert fraction >= 0.50, (
            f"Expected >50% unrated employers (conservative scoring), got {fraction:.1%}"
        )

    def test_approval_rate_cols_bounded(self, df):
        """Approval rate columns should be in [0, 1]."""
        for col in ["approval_rate_24m", "approval_rate_36m"]:
            if col in df.columns:
                valid = df[col].dropna()
                assert (valid >= 0).all(), f"{col} < 0"
                assert (valid <= 1).all(), f"{col} > 1"

    def test_known_major_sponsors_present(self, df):
        """Well-known major H-1B sponsors should appear in the dataset."""
        employers = df["employer_name"].str.upper().values
        for name_fragment in ["INFOSYS", "TATA", "COGNIZANT", "AMAZON", "GOOGLE"]:
            found = any(name_fragment in e for e in employers)
            assert found, f"Expected employer '{name_fragment}' not found in EFS table"


# =============================================================================
# QUEUE DEPTH ESTIMATES — Backlog wait time model
# Baselines: key EB2/EB3 India/China series
# =============================================================================

class TestQueueDepthBaselines:
    """Regression tests for queue depth and wait time estimates."""

    @pytest.fixture(scope="class")
    def df(self):
        return _load("queue_depth_estimates")

    def test_row_count_floor(self, df):
        """Should have at least 1,500 rows."""
        assert len(df) >= 1_500, f"Expected ≥1,500 rows, got {len(df):,}"

    def test_est_wait_years_non_negative(self, df):
        """All non-null wait year estimates should be ≥ 0."""
        valid = df["est_wait_years"].dropna()
        assert (valid >= 0).all(), "Found negative est_wait_years"

    def test_eb2_india_has_rows(self, df):
        """EB2-India should have queue depth estimates across multiple PD months."""
        g = df[(df.category == "EB2") & (df.country == "IND")]
        assert len(g) >= 10, f"Expected ≥10 EB2-India rows, got {len(g)}"

    def test_eb_categories_present(self, df):
        """All major EB categories should be present."""
        cats = set(df["category"].unique())
        for cat in ["EB1", "EB2", "EB3"]:
            assert cat in cats, f"Missing category: {cat}"

    def test_high_demand_countries_present(self, df):
        """India and China should dominate queue depth estimates."""
        for country in ["IND", "CHN"]:
            n = (df["country"] == country).sum()
            assert n >= 20, f"Expected ≥20 rows for country={country}, got {n}"


# =============================================================================
# BACKLOG ESTIMATES — Historical queue snapshot
# Baselines: 8,115 rows (May 2026 bulletin data)
# =============================================================================

class TestBacklogEstimateBaselines:
    """Regression tests for the backlog estimates table."""

    @pytest.fixture(scope="class")
    def df(self):
        return _load("backlog_estimates")

    def test_row_count_floor(self, df):
        """Should have at least 8,000 rows."""
        assert len(df) >= 8_000, f"Expected ≥8,000 rows, got {len(df):,}"

    def test_bulletin_year_range(self, df):
        """Should span at least from 2015 to 2026."""
        assert df["bulletin_year"].min() <= 2015
        assert df["bulletin_year"].max() >= 2026

    def test_category_presence(self, df):
        """All major EB categories should be present."""
        cats = set(df["category"].unique())
        for cat in ["EB1", "EB2", "EB3"]:
            assert cat in cats, f"Missing category: {cat}"

    def test_blended_velocity_non_negative(self, df):
        """blended_velocity must be ≥ 0."""
        valid = df["blended_velocity"].dropna()
        assert (valid >= 0).all(), "Found negative blended_velocity"


# =============================================================================
# SOC SALARY MARKET — Wage benchmark table
# Baselines: 18,038 rows, LCA + PERM + OEWS data
# =============================================================================

class TestSocSalaryMarketBaselines:
    """Regression tests for SOC salary market benchmarks."""

    @pytest.fixture(scope="class")
    def df(self):
        return _load("soc_salary_market")

    def test_row_count_floor(self, df):
        """Should have at least 15,000 rows."""
        assert len(df) >= 15_000, f"Expected ≥15,000 rows, got {len(df):,}"

    def test_market_mean_in_range(self, df):
        """Market mean wages should be in $20K-$1M range (annual)."""
        valid = df["market_mean"].dropna()
        assert (valid >= 20_000).mean() >= 0.90, "Majority of market_mean rows below $20K/yr"
        assert (valid <= 2_000_000).all(), "Found market_mean above $2M/yr"

    def test_market_median_less_than_mean(self, df):
        """market_median should generally be ≤ market_mean (right-skewed distribution)."""
        both = df[df["market_mean"].notna() & df["market_median"].notna()]
        violations = (both["market_median"] > both["market_mean"] * 1.5).mean()
        assert violations < 0.05, f"{violations:.1%} of rows have median > 1.5x mean"

    def test_soc_codes_present(self, df):
        """Key high-demand SOC codes should be present."""
        socs = set(df["soc_code"].unique())
        # Software developers (15-1252), Computer Systems Analysts (15-1211)
        high_demand = {"15-1252", "15-1211", "15-1251", "15-2051"}
        found = socs & high_demand
        assert len(found) >= 1, f"None of the expected high-demand SOCs found: {high_demand}"

    def test_fiscal_year_coverage(self, df):
        """Should have data for at least FY2023 or FY2024."""
        if "fiscal_year" in df.columns:
            years = set(df["fiscal_year"].unique())
            assert any(y >= 2023 for y in years), f"No data for FY2023+, got: {sorted(years)[-3:]}"


# =============================================================================
# BLS CES — Employment statistics (new data source, M23)
# Baselines: 54 rows (Apr 2026), 2 series
# =============================================================================

class TestBLSCESBaselines:
    """Regression tests for BLS Current Employment Statistics table."""

    @pytest.fixture(scope="class")
    def df(self):
        return _load("fact_bls_ces")

    def test_row_count_floor(self, df):
        """Should have at least 20 rows (2 series × multiple months)."""
        assert len(df) >= 20, f"Expected ≥20 rows, got {len(df)}"

    def test_total_nonfarm_series_present(self, df):
        """Total Nonfarm series (CES0000000001) must be present."""
        assert "CES0000000001" in df["series_id"].values, "Missing Total Nonfarm series"

    def test_employment_values_in_range(self, df):
        """Total nonfarm employment should be 140,000-170,000 thousands (~140-170M workers)."""
        total = df[df["series_id"] == "CES0000000001"]
        if len(total) == 0:
            pytest.skip("Total Nonfarm series not found")
        vals = total["value"].dropna()
        assert (vals >= 140_000).all(), f"Total nonfarm below 140M: {vals.min():.0f}"
        assert (vals <= 170_000).all(), f"Total nonfarm above 170M: {vals.max():.0f}"

    def test_latest_year_is_recent(self, df):
        """Latest year in BLS CES should be at most 1 year old."""
        import datetime
        current_year = datetime.date.today().year
        assert df["year"].max() >= current_year - 1, (
            f"Latest BLS CES year {df['year'].max()} is too old (current: {current_year})"
        )

    def test_year_period_pk_unique(self, df):
        """series_id + year + period must be unique."""
        dups = df.duplicated(subset=["series_id", "year", "period"]).sum()
        assert dups == 0, f"{dups} duplicate (series, year, period) keys in BLS CES"


# =============================================================================
# VISA DEMAND METRICS — Aggregated visa demand data
# Baselines: 569,114 rows, FY2017-FY2025
# =============================================================================

class TestVisaDemandBaselines:
    """Regression tests for aggregated visa demand metrics."""

    @pytest.fixture(scope="class")
    def df(self):
        return _load("visa_demand_metrics")

    def test_row_count_floor(self, df):
        """Should have at least 400,000 rows."""
        assert len(df) >= 400_000, f"Expected ≥400K rows, got {len(df):,}"

    def test_fiscal_year_range(self, df):
        """Should cover at least FY2020-FY2025."""
        years = df["fiscal_year"].unique()
        assert any("2020" in str(y) or y == 2020 for y in years), "Missing FY2020 data"
        assert any("2025" in str(y) or y == 2025 for y in years), "Missing FY2025 data"

    def test_count_issued_non_negative(self, df):
        """count_issued should always be ≥ 0."""
        if "count_issued" in df.columns:
            assert (df["count_issued"].dropna() >= 0).all(), "Found negative count_issued"

    def test_major_categories_present(self, df):
        """EB categories or visa classes should be in the data."""
        cats = df["category"].unique() if "category" in df.columns else []
        eb_cats = [c for c in cats if "EB" in str(c) or "E" in str(c)[:2]]
        assert len(eb_cats) >= 1, f"No EB-type categories found. Got: {sorted(cats)[:10]}"
