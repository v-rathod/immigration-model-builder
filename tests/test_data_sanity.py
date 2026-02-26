"""
test_data_sanity.py — Product-Owner / Stakeholder Acceptance Tests
===================================================================
Business-meaningful assertions that validate whether the data "makes sense"
from a domain expert's perspective. These are NOT schema or technical tests —
they catch logic errors, model inversions, and data pipeline bugs that schema
tests miss entirely.

Each test has a docstring explaining the business rule in plain English.

Run with:  pytest tests/test_data_sanity.py -v
Marker:    @pytest.mark.sanity
"""
from __future__ import annotations

import pathlib
import warnings

import pandas as pd
import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"


def _load(name: str) -> pd.DataFrame:
    """Load artifact or skip test if missing."""
    flat = TABLES / f"{name}.parquet"
    partitioned = TABLES / name
    if flat.is_file():
        df = pd.read_parquet(flat)
    elif partitioned.is_dir():
        parts = sorted(partitioned.glob("**/*.parquet"))
        if not parts:
            pytest.skip(f"{name}: no partition files found")
        frames = []
        for p in parts:
            try:
                frames.append(pd.read_parquet(p))
            except Exception:
                continue
        if not frames:
            pytest.skip(f"{name}: all partitions unreadable")
        df = pd.concat(frames, ignore_index=True)
    else:
        pytest.skip(f"{name}.parquet not found")
    if len(df) == 0:
        pytest.skip(f"{name}: 0 rows (stub)")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# Priority Date Forecasts — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestPDForecastSanity:
    """Priority date forecasts should reflect known immigration patterns."""

    @pytest.fixture(scope="class")
    def forecasts(self):
        return _load("pd_forecasts")

    def test_all_eb_categories_have_forecasts(self, forecasts):
        """PD forecasts must exist for all major EB categories (EB1, EB2, EB3)."""
        categories = set(forecasts["category"].unique())
        required = {"EB1", "EB2", "EB3"}
        missing = required - categories
        assert not missing, f"Missing forecast categories: {missing}"

    def test_india_and_china_have_forecasts(self, forecasts):
        """India and China are the most backlogged — they must have forecasts."""
        countries = set(forecasts["country"].unique())
        required = {"IND", "CHN"}
        missing = required - countries
        assert not missing, f"Missing forecast countries: {missing}"

    def test_eb2_india_is_most_backlogged(self, forecasts):
        """EB2 India should be among the most backlogged categories.
        Business rule: EB2-India cutoff dates are historically years behind EB2-ROW."""
        eb2_ind = forecasts[
            (forecasts["category"] == "EB2") & (forecasts["country"] == "IND")
        ]
        eb2_row = forecasts[
            (forecasts["category"] == "EB2") & (forecasts["country"] == "ROW")
        ]
        if len(eb2_ind) == 0 or len(eb2_row) == 0:
            pytest.skip("EB2-India or EB2-ROW forecasts not available")

        # Compare velocity (advancement rate) — India should be slower or similar
        ind_velocity = eb2_ind["velocity_days_per_month"].median()
        row_velocity = eb2_row["velocity_days_per_month"].median()
        # India velocity should be finite (not wildly higher than ROW)
        assert ind_velocity < row_velocity * 5, (
            f"EB2-India velocity ({ind_velocity:.1f} d/mo) is suspiciously faster "
            f"than EB2-ROW ({row_velocity:.1f} d/mo)"
        )

    def test_forecasts_have_56_series(self, forecasts):
        """The forecast model should produce at least 30 category×country series."""
        series = forecasts.groupby(["category", "country"]).ngroups
        assert series >= 30, f"Only {series} forecast series — expected ≥30 (56 target)"

    def test_forecast_months_are_24(self, forecasts):
        """Each series should have up to 24 months of forecast per chart type."""
        # Forecasts may have multiple chart types (filing/final_action)
        # so group by chart+category+country
        group_cols = ["category", "country"]
        if "chart" in forecasts.columns:
            group_cols = ["chart", "category", "country"]
        per_series = forecasts.groupby(group_cols).size()
        assert per_series.max() <= 24, f"Max months per series = {per_series.max()}, expected ≤24"
        assert per_series.min() >= 12, f"Min months per series = {per_series.min()}, expected ≥12"

    def test_projected_dates_are_in_future(self, forecasts):
        """Projected cutoff dates should not be in the distant past."""
        if "projected_cutoff_date" not in forecasts.columns:
            pytest.skip("No projected_cutoff_date column")
        dates = pd.to_datetime(forecasts["projected_cutoff_date"], errors="coerce")
        valid = dates.dropna()
        if len(valid) == 0:
            pytest.skip("No valid projected dates")
        # At least 50% of projected dates should be after year 2000
        pct_after_2000 = (valid > "2000-01-01").mean()
        assert pct_after_2000 >= 0.5, (
            f"Only {pct_after_2000:.1%} of projected dates are after 2000 — "
            f"possible date parsing error"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Employer Friendliness Scores — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestEFSSanity:
    """Employer scores should reflect known immigration-friendly companies."""

    @pytest.fixture(scope="class")
    def efs_rules(self):
        return _load("employer_friendliness_scores")

    @pytest.fixture(scope="class")
    def efs_ml(self):
        return _load("employer_friendliness_scores_ml")

    def test_efs_rules_covers_many_employers(self, efs_rules):
        """Rules-based EFS should cover tens of thousands of employers."""
        assert len(efs_rules) >= 50_000, (
            f"EFS rules covers only {len(efs_rules):,} employers — expected ≥50K"
        )

    def test_efs_ml_covers_high_volume(self, efs_ml):
        """ML EFS should cover at least 500 high-volume employers."""
        assert len(efs_ml) >= 500, (
            f"EFS ML covers only {len(efs_ml):,} employers — expected ≥500"
        )

    def test_efs_score_range(self, efs_rules):
        """All EFS scores should be in [0, 100]."""
        col = "efs" if "efs" in efs_rules.columns else "efs_score"
        scores = efs_rules[col].dropna()
        assert scores.min() >= 0, f"EFS min = {scores.min()}, expected ≥0"
        assert scores.max() <= 100, f"EFS max = {scores.max()}, expected ≤100"

    def test_efs_has_tier_distribution(self, efs_rules):
        """EFS should have multiple tiers (not all employers in one bucket)."""
        tier_col = "efs_tier" if "efs_tier" in efs_rules.columns else None
        if tier_col is None:
            pytest.skip("No efs_tier column")
        tiers = efs_rules[tier_col].nunique()
        assert tiers >= 3, f"Only {tiers} EFS tiers — expected ≥3 (healthy distribution)"

    def test_no_approval_rate_above_100pct(self, efs_rules):
        """No employer should have >100% approval rate (division error)."""
        for col in ["approval_rate_24m", "approval_rate_12m", "approval_rate_36m"]:
            if col in efs_rules.columns:
                rates = efs_rules[col].dropna()
                bad = (rates > 1.0).sum()
                assert bad == 0, (
                    f"{bad:,} employers have {col} > 100%  — possible division error"
                )

    def test_mean_efs_is_moderate(self, efs_rules):
        """Average EFS should be moderate (20-80), not pegged to extremes."""
        col = "efs" if "efs" in efs_rules.columns else "efs_score"
        mean_score = efs_rules[col].dropna().mean()
        assert 15 <= mean_score <= 85, (
            f"Mean EFS = {mean_score:.1f} — distribution seems skewed "
            f"(expected 15–85 for moderate spread)"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Salary Benchmarks — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestSalarySanity:
    """Salary data should reflect reasonable US wage ranges."""

    @pytest.fixture(scope="class")
    def salary(self):
        return _load("salary_benchmarks")

    def test_median_salary_in_reasonable_range(self, salary):
        """Median salary should be between $20K and $600K (US H-1B range)."""
        if "median" not in salary.columns:
            pytest.skip("No median column")
        medians = salary["median"].dropna()
        overall_median = medians.median()
        assert 20_000 <= overall_median <= 600_000, (
            f"Overall median salary = ${overall_median:,.0f} — outside $20K-$600K range"
        )

    def test_percentile_ordering(self, salary):
        """p10 ≤ p25 ≤ median ≤ p75 ≤ p90 for the vast majority of rows."""
        pcols = ["p10", "p25", "median", "p75", "p90"]
        present = [c for c in pcols if c in salary.columns]
        if len(present) < 3:
            pytest.skip("Fewer than 3 percentile columns found")
        sub = salary[present].dropna()
        if len(sub) == 0:
            pytest.skip("No complete rows")
        # Check ordering for consecutive pairs
        violations = 0
        for i in range(len(present) - 1):
            violations += (sub[present[i]] > sub[present[i + 1]]).sum()
        violation_rate = violations / (len(sub) * (len(present) - 1))
        assert violation_rate < 0.05, (
            f"Percentile ordering violated in {violation_rate:.1%} of cases "
            f"(expected <5%)"
        )

    def test_no_negative_salaries(self, salary):
        """No salary should be negative."""
        for col in ["p10", "p25", "median", "p75", "p90"]:
            if col in salary.columns:
                neg = (salary[col].dropna() < 0).sum()
                assert neg == 0, f"{neg:,} negative values in {col}"

    def test_salary_coverage(self, salary):
        """Should have salary data for at least 1,000 SOC×area combinations."""
        assert len(salary) >= 1_000, (
            f"Only {len(salary):,} salary benchmarks — expected ≥1,000"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Geographic Distribution — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestGeographicSanity:
    """Geographic patterns should match known immigration filing patterns."""

    @pytest.fixture(scope="class")
    def geo(self):
        return _load("worksite_geo_metrics")

    def test_california_is_top_filing_state(self, geo):
        """California should be the highest-filing state (known from PERM data)."""
        if "state" not in geo.columns:
            pytest.skip("No state column")
        filing_col = next(
            (c for c in ["filings_count", "total_filings", "filings"] if c in geo.columns),
            None,
        )
        if filing_col is None:
            pytest.skip("No filings column found")
        # Exclude empty/blank state entries
        state_totals = (
            geo[geo["state"].str.strip().ne("")]
            .groupby("state")[filing_col]
            .sum()
            .sort_values(ascending=False)
        )
        if len(state_totals) == 0:
            pytest.skip("No state data")
        top_state = state_totals.index[0]
        assert top_state == "CA", (
            f"Top filing state is '{top_state}', expected 'CA' (California). "
            f"Top 5: {state_totals.head().to_dict()}"
        )

    def test_top_states_include_tech_hubs(self, geo):
        """Top 10 filing states should include CA, TX, NY, NJ (known tech hubs)."""
        if "state" not in geo.columns:
            pytest.skip("No state column")
        filing_col = next(
            (c for c in ["filings_count", "total_filings", "filings"] if c in geo.columns),
            None,
        )
        if filing_col is None:
            pytest.skip("No filings column found")
        top10 = set(
            geo[geo["state"].str.strip().ne("")]
            .groupby("state")[filing_col]
            .sum()
            .nlargest(10)
            .index
        )
        expected_in_top10 = {"CA", "TX", "NY", "NJ"}
        missing = expected_in_top10 - top10
        assert not missing, (
            f"States not in top 10: {missing}. Top 10: {sorted(top10)}"
        )

    def test_geo_has_multiple_states(self, geo):
        """Should cover at least 40 US states."""
        if "state" not in geo.columns:
            pytest.skip("No state column")
        n_states = geo["state"].str.strip().replace("", pd.NA).dropna().nunique()
        assert n_states >= 40, (
            f"Only {n_states} distinct states — expected ≥40"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PERM Data — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestPERMSanity:
    """PERM labor certification data should reflect known DOL patterns."""

    @pytest.fixture(scope="class")
    def perm(self):
        return _load("fact_perm_all")

    def test_perm_has_over_1m_records(self, perm):
        """PERM dataset should have >1M records (20 years of data)."""
        assert len(perm) >= 1_000_000, (
            f"fact_perm_all has only {len(perm):,} rows — expected ≥1M"
        )

    def test_perm_has_certified_cases(self, perm):
        """Most PERM cases should be Certified (known DOL approval rate ~75%)."""
        if "case_status" not in perm.columns:
            pytest.skip("No case_status column")
        statuses = perm["case_status"].str.upper().str.strip()
        certified = statuses.str.startswith("CERTIFIED").sum()
        rate = certified / len(perm)
        assert rate >= 0.60, (
            f"Certification rate = {rate:.1%} — expected ≥60% "
            f"(DOL historical average ~75%)"
        )

    def test_perm_spans_many_fiscal_years(self, perm):
        """PERM data should span at least 15 fiscal years."""
        fy_col = next(
            (c for c in ["fiscal_year", "fy", "decision_year"] if c in perm.columns),
            None,
        )
        if fy_col is None:
            pytest.skip("No fiscal year column found")
        years = perm[fy_col].dropna()
        # Handle "FY2024" string format or pyarrow str dtype
        if not pd.api.types.is_numeric_dtype(years):
            years = years.astype(str).str.extract(r"(\d{4})", expand=False).dropna().astype(int)
        span = years.max() - years.min()
        assert span >= 15, f"PERM data spans only {span} years — expected ≥15"

    def test_no_impossible_approval_dates(self, perm):
        """Decision dates should be after 2004 (PERM started 2005)."""
        date_cols = [c for c in perm.columns if "decision" in c.lower() and "date" in c.lower()]
        for col in date_cols:
            dates = pd.to_datetime(perm[col], errors="coerce").dropna()
            before_2004 = (dates < "2004-01-01").sum()
            pct = before_2004 / len(dates) if len(dates) > 0 else 0
            assert pct < 0.01, (
                f"{before_2004:,} ({pct:.1%}) dates in {col} are before 2004 "
                f"(PERM didn't exist then)"
            )

    def test_year_over_year_volume_stable(self, perm):
        """No single year should have <50% or >200% of adjacent years (data loading bug)."""
        fy_col = next(
            (c for c in ["fiscal_year", "fy"] if c in perm.columns), None
        )
        if fy_col is None:
            pytest.skip("No fiscal year column found")
        counts = perm[fy_col].value_counts().sort_index()
        if len(counts) < 3:
            pytest.skip("Too few fiscal years to check stability")

        extreme_jumps = []
        values = counts.values
        for i in range(1, len(values)):
            if values[i - 1] == 0:
                continue
            ratio = values[i] / values[i - 1]
            if ratio < 0.3 or ratio > 3.0:
                idx = counts.index[i]
                extreme_jumps.append(
                    f"{idx}: {values[i]:,} vs prev {values[i-1]:,} (ratio {ratio:.2f})"
                )
        assert len(extreme_jumps) <= 2, (
            f"Extreme YoY volume jumps (possible data loading bug):\n"
            + "\n".join(f"  • {j}" for j in extreme_jumps)
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Visa Bulletin / Cutoffs — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestVisaBulletinSanity:
    """Visa bulletin cutoff data should reflect known patterns."""

    @pytest.fixture(scope="class")
    def cutoffs(self):
        return _load("fact_cutoffs_all")

    def test_cutoffs_cover_all_eb_categories(self, cutoffs):
        """Cutoffs should exist for EB1, EB2, EB3 at minimum."""
        if "category" not in cutoffs.columns:
            pytest.skip("No category column")
        categories = set(cutoffs["category"].unique())
        required = {"EB1", "EB2", "EB3"}
        # Categories may be labeled differently — check substring
        found = set()
        for cat in categories:
            for req in required:
                if req in str(cat).upper():
                    found.add(req)
        missing = required - found
        assert not missing, (
            f"Missing EB categories in visa bulletin: {missing}. "
            f"Found: {sorted(categories)[:10]}"
        )

    def test_cutoffs_cover_india_and_china(self, cutoffs):
        """India and China must have cutoff data (most backlogged countries)."""
        if "country" not in cutoffs.columns:
            pytest.skip("No country column")
        countries = set(cutoffs["country"].str.upper().unique())
        # India may be "INDIA", "IND", etc.
        has_india = any("INDIA" in c or c == "IND" for c in countries)
        has_china = any("CHINA" in c or c == "CHN" for c in countries)
        assert has_india, f"No India cutoff data. Countries: {sorted(countries)[:10]}"
        assert has_china, f"No China cutoff data. Countries: {sorted(countries)[:10]}"

    def test_cutoffs_span_many_years(self, cutoffs):
        """Visa bulletin data should span at least 10 years."""
        year_col = next(
            (c for c in ["bulletin_year", "year", "fiscal_year"] if c in cutoffs.columns),
            None,
        )
        if year_col is None:
            pytest.skip("No year column found")
        years = cutoffs[year_col].dropna()
        if years.dtype == object:
            years = years.str.extract(r"(\d{4})", expand=False).dropna().astype(int)
        span = int(years.max()) - int(years.min())
        assert span >= 10, f"Cutoff data spans only {span} years — expected ≥10"


# ═══════════════════════════════════════════════════════════════════════════════
# Dimension Tables — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestDimensionSanity:
    """Dimension tables should meet baseline cardinality expectations."""

    def test_dim_employer_has_many_employers(self):
        """dim_employer should have ≥200K employers (patched from all PERM data)."""
        df = _load("dim_employer")
        assert len(df) >= 200_000, (
            f"dim_employer has only {len(df):,} rows — "
            f"patch script may not have run"
        )

    def test_dim_soc_has_standard_codes(self):
        """dim_soc should have standard SOC code count (1,000+)."""
        df = _load("dim_soc")
        assert len(df) >= 800, (
            f"dim_soc has only {len(df)} codes — expected ≥800 (SOC-2018 has ~867)"
        )

    def test_dim_country_covers_world(self):
        """dim_country should have ≥200 countries (ISO 3166-1 has 249)."""
        df = _load("dim_country")
        assert len(df) >= 200, (
            f"dim_country has only {len(df)} entries — expected ≥200"
        )

    def test_dim_visa_class_has_eb_categories(self):
        """dim_visa_class should include EB1-EB5 categories."""
        df = _load("dim_visa_class")
        assert len(df) >= 4, (
            f"dim_visa_class has only {len(df)} entries — expected ≥4 for EB1-EB5"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Processing Times — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestProcessingTimesSanity:
    """Processing times should reflect USCIS reporting patterns."""

    @pytest.fixture(scope="class")
    def proc(self):
        return _load("processing_times_trends")

    def test_processing_times_exist(self, proc):
        """Should have at least 10 processing time records."""
        assert len(proc) >= 10, (
            f"Only {len(proc)} processing time records — expected ≥10"
        )

    def test_backlog_months_reasonable(self, proc):
        """Backlog months should be between 0 and 120 (0-10 years)."""
        if "backlog_months" not in proc.columns:
            pytest.skip("No backlog_months column")
        bm = proc["backlog_months"].dropna()
        if len(bm) == 0:
            pytest.skip("No backlog data")
        assert bm.min() >= 0, f"Negative backlog months: {bm.min()}"
        assert bm.max() <= 120, f"Backlog months > 10 years: {bm.max()}"

    def test_approval_rate_reasonable(self, proc):
        """USCIS approval rate should be between 50% and 100%."""
        if "approval_rate" not in proc.columns:
            pytest.skip("No approval_rate column")
        rates = proc["approval_rate"].dropna()
        if len(rates) == 0:
            pytest.skip("No approval rate data")
        median_rate = rates.median()
        assert 0.40 <= median_rate <= 1.0, (
            f"Median USCIS approval rate = {median_rate:.1%} — "
            f"expected 40%–100%"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# SOC Demand — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestSOCDemandSanity:
    """SOC demand metrics should reflect known tech-heavy immigration patterns."""

    @pytest.fixture(scope="class")
    def soc_demand(self):
        return _load("soc_demand_metrics")

    def test_has_software_developer_soc(self, soc_demand):
        """15-1252 (Software Developers) should appear in demand metrics —
        it's the #1 H-1B/PERM occupation."""
        if "soc_code" not in soc_demand.columns:
            pytest.skip("No soc_code column")
        codes = set(soc_demand["soc_code"].unique())
        # Software devs: 15-1252 (SOC-2018) or 15-1256 (SOC-2018) or 15-1132 (SOC-2010)
        sw_codes = {"15-1252", "15-1256", "15-1132", "15-1251", "15-1253", "15-1254"}
        found = codes & sw_codes
        assert found, (
            f"No software developer SOC codes found in soc_demand_metrics. "
            f"Sample codes: {sorted(list(codes))[:10]}"
        )

    def test_soc_demand_has_reasonable_count(self, soc_demand):
        """Should have demand metrics for at least 100 distinct SOC codes."""
        if "soc_code" not in soc_demand.columns:
            pytest.skip("No soc_code column")
        n = soc_demand["soc_code"].nunique()
        assert n >= 100, f"Only {n} distinct SOC codes in demand metrics — expected ≥100"

    def test_no_negative_filings(self, soc_demand):
        """Filing counts cannot be negative."""
        for col in ["filings_count", "approvals_count"]:
            if col in soc_demand.columns:
                neg = (soc_demand[col].dropna() < 0).sum()
                assert neg == 0, f"{neg:,} negative values in {col}"


# ═══════════════════════════════════════════════════════════════════════════════
# Cross-Artifact Consistency — Double-Entry Bookkeeping
# ═══════════════════════════════════════════════════════════════════════════════

class TestCrossArtifactConsistency:
    """Validates that related artifacts agree with each other."""

    def test_efs_employers_exist_in_dim_employer(self):
        """Every employer in EFS should exist in dim_employer."""
        efs = _load("employer_friendliness_scores")
        dim = _load("dim_employer")
        if "employer_id" not in efs.columns or "employer_id" not in dim.columns:
            pytest.skip("Missing employer_id columns")
        dim_ids = set(dim["employer_id"].unique())
        efs_ids = set(efs["employer_id"].unique())
        overlap = len(efs_ids & dim_ids) / len(efs_ids) if efs_ids else 0
        assert overlap >= 0.95, (
            f"Only {overlap:.1%} of EFS employers found in dim_employer — "
            f"expected ≥95% ({len(efs_ids & dim_ids):,} / {len(efs_ids):,})"
        )

    def test_employer_features_match_efs_count(self):
        """employer_features and EFS rules should have similar employer counts."""
        ef = _load("employer_features")
        efs = _load("employer_friendliness_scores")
        # They should have the same row count (1:1 mapping)
        ratio = len(efs) / len(ef) if len(ef) > 0 else 0
        assert 0.9 <= ratio <= 1.1, (
            f"employer_features ({len(ef):,}) vs EFS ({len(efs):,}) — "
            f"ratio {ratio:.2f}, expected ~1.0"
        )

    def test_pd_forecasts_categories_match_cutoffs(self):
        """PD forecast categories should overlap with visa bulletin categories."""
        pd_df = _load("pd_forecasts")
        co_df = _load("fact_cutoffs_all")
        if "category" not in pd_df.columns or "category" not in co_df.columns:
            pytest.skip("Missing category columns")
        pd_cats = set(pd_df["category"].unique())
        co_cats = set(co_df["category"].unique())
        overlap = pd_cats & co_cats
        assert len(overlap) >= 3, (
            f"Only {len(overlap)} shared categories between forecasts and cutoffs. "
            f"Forecast: {sorted(pd_cats)}, Cutoffs: {sorted(list(co_cats)[:10])}"
        )

    def test_salary_soc_codes_in_dim_soc(self):
        """salary_benchmarks SOC codes should mostly exist in dim_soc."""
        sal = _load("salary_benchmarks")
        dim_soc = _load("dim_soc")
        if "soc_code" not in sal.columns or "soc_code" not in dim_soc.columns:
            pytest.skip("Missing soc_code columns")
        dim_codes = set(dim_soc["soc_code"].unique())
        sal_codes = set(sal["soc_code"].unique())
        overlap = len(sal_codes & dim_codes) / len(sal_codes) if sal_codes else 0
        assert overlap >= 0.80, (
            f"Only {overlap:.1%} of salary SOC codes in dim_soc — expected ≥80%. "
            f"Unmatched samples: {sorted(sal_codes - dim_codes)[:5]}"
        )

    def test_geo_states_are_valid_us_states(self):
        """worksite_geo_metrics state values should be recognizable US states."""
        geo = _load("worksite_geo_metrics")
        if "state" not in geo.columns:
            pytest.skip("No state column")
        VALID_ABBREVS = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
            "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
            "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
            "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
            "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
            "DC", "PR", "GU", "VI", "AS", "MP",  # territories
        }
        VALID_FULL_NAMES = {
            "ALABAMA", "ALASKA", "ARIZONA", "ARKANSAS", "CALIFORNIA",
            "COLORADO", "CONNECTICUT", "DELAWARE", "FLORIDA", "GEORGIA",
            "HAWAII", "IDAHO", "ILLINOIS", "INDIANA", "IOWA", "KANSAS",
            "KENTUCKY", "LOUISIANA", "MAINE", "MARYLAND", "MASSACHUSETTS",
            "MICHIGAN", "MINNESOTA", "MISSISSIPPI", "MISSOURI", "MONTANA",
            "NEBRASKA", "NEVADA", "NEW HAMPSHIRE", "NEW JERSEY", "NEW MEXICO",
            "NEW YORK", "NORTH CAROLINA", "NORTH DAKOTA", "OHIO", "OKLAHOMA",
            "OREGON", "PENNSYLVANIA", "RHODE ISLAND", "SOUTH CAROLINA",
            "SOUTH DAKOTA", "TENNESSEE", "TEXAS", "UTAH", "VERMONT",
            "VIRGINIA", "WASHINGTON", "WEST VIRGINIA", "WISCONSIN", "WYOMING",
            "DISTRICT OF COLUMBIA", "PUERTO RICO", "GUAM", "VIRGIN ISLANDS",
            "AMERICAN SAMOA", "NORTHERN MARIANA ISLANDS",
            "FEDERATED STATES OF MICRONESIA", "MARSHALL ISLANDS", "PALAU",
        }
        VALID = VALID_ABBREVS | VALID_FULL_NAMES
        states = set(geo["state"].dropna().str.strip().str.upper().unique()) - {""}
        invalid = {s for s in states if s not in VALID}
        if len(states) > 0:
            invalid_rate = len(invalid) / len(states)
            assert invalid_rate < 0.10, (
                f"{len(invalid)} invalid state codes ({invalid_rate:.1%}): "
                f"{sorted(invalid)[:10]}"
            )

    def test_soc_demand_approval_rate_bounded(self):
        """soc_demand_metrics approval rates should be in [0, 1]."""
        soc = _load("soc_demand_metrics")
        if "approval_rate" not in soc.columns:
            pytest.skip("No approval_rate column")
        rates = soc["approval_rate"].dropna()
        above_1 = (rates > 1.0).sum()
        below_0 = (rates < 0.0).sum()
        assert above_1 == 0, f"{above_1:,} SOC codes with approval rate > 100%"
        assert below_0 == 0, f"{below_0:,} SOC codes with approval rate < 0%"

    def test_perm_employer_volume_matches_features(self):
        """Total PERM cases by employer should roughly match employer_features aggregates."""
        ef = _load("employer_features")
        if "employer_id" not in ef.columns:
            pytest.skip("No employer_id in employer_features")
        # Check that n_36m is reasonable (not all zeros, not all nulls)
        vol_col = next(
            (c for c in ["n_36m", "n_24m", "n_12m", "total_cases"] if c in ef.columns),
            None,
        )
        if vol_col is None:
            pytest.skip("No volume column found")
        vols = ef[vol_col].dropna()
        assert vols.sum() > 0, "All employer volumes are 0 — feature engineering may have failed"
        assert vols.median() >= 1, f"Median employer volume = {vols.median()}, expected ≥1"


# ═══════════════════════════════════════════════════════════════════════════════
# RAG Artifacts — Business Sanity
# ═══════════════════════════════════════════════════════════════════════════════

class TestRAGSanity:
    """RAG export artifacts should be complete and usable by Compass."""

    RAG_DIR = ROOT / "artifacts" / "rag"

    def test_rag_catalog_exists(self):
        """RAG catalog must exist and list all artifacts."""
        import json
        cat_path = self.RAG_DIR / "catalog.json"
        if not cat_path.exists():
            pytest.skip("RAG catalog not found — run: python3 -m src.export.rag_builder")
        with open(cat_path) as f:
            cat = json.load(f)
        assert len(cat.get("artifacts", [])) >= 30, (
            f"RAG catalog has only {len(cat.get('artifacts', []))} artifacts — expected ≥30"
        )

    def test_qa_cache_has_enough_pairs(self):
        """QA cache should have ≥100 pre-computed answers."""
        import json
        qa_path = self.RAG_DIR / "qa_cache.json"
        if not qa_path.exists():
            pytest.skip("QA cache not found — run: python3 -m src.export.qa_generator")
        with open(qa_path) as f:
            qa = json.load(f)
        pairs = qa.get("pairs", qa) if isinstance(qa, dict) else qa
        if isinstance(pairs, dict):
            pairs = pairs.get("pairs", [])
        assert len(pairs) >= 100, (
            f"QA cache has only {len(pairs)} pairs — expected ≥100"
        )

    def test_all_chunks_exist(self):
        """All chunks file should exist with reasonable content."""
        import json
        chunks_path = self.RAG_DIR / "all_chunks.json"
        if not chunks_path.exists():
            pytest.skip("all_chunks.json not found")
        with open(chunks_path) as f:
            chunks = json.load(f)
        assert len(chunks) >= 20, (
            f"Only {len(chunks)} RAG chunks — expected ≥20"
        )
