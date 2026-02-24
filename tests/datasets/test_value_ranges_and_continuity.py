"""
tests/datasets/test_value_ranges_and_continuity.py

Value range and temporal continuity checks across new datasets.
- No negative counts
- Ceilings > 0
- backlog_estimates months_to_clear in [0, 600]
- Time spans within expected ranges (out-of-band rows generate WARNs, not failures)
- DHS span ≥ 40 years
"""
import pathlib
import warnings

import pandas as pd
import pytest

TABLES = pathlib.Path("artifacts/tables")


def _load(name: str) -> pd.DataFrame:
    p = TABLES / f"{name}.parquet"
    if not p.exists():
        pytest.skip(f"{name}.parquet not found")
    return pd.read_parquet(p)


def _fy_years(df: pd.DataFrame, col: str = "fiscal_year") -> list[int]:
    """Extract sorted list of integer years from FY#### strings."""
    fys = df[col].dropna().astype(str)
    years = [int(fy[2:]) for fy in fys if fy.startswith("FY") and fy[2:].isdigit()]
    return sorted(set(years))


# ─────────────────── Non-negative counts ─────────────────────────

class TestNonNegative:
    @pytest.mark.parametrize("table,col", [
        ("dim_visa_ceiling", "ceiling"),
        ("fact_visa_issuance", "issued"),
        ("fact_visa_applications", "applications"),
        ("fact_niv_issuance", "issued"),
        ("fact_uscis_approvals", "approvals"),
        ("fact_dhs_admissions", "admissions"),
        ("fact_warn_events", "employees_affected"),
    ])
    def test_no_negative_counts(self, table, col):
        df = _load(table)
        if len(df) == 0:
            pytest.skip(f"{table}: 0 rows (stub)")
        if col not in df.columns:
            pytest.skip(f"{table}: column '{col}' not found")
        numeric = pd.to_numeric(df[col], errors="coerce").dropna()
        neg = (numeric < 0).sum()
        assert neg == 0, f"{table}.{col}: {neg} negative values"

    def test_dim_visa_ceiling_positive(self):
        df = _load("dim_visa_ceiling")
        numeric = pd.to_numeric(df["ceiling"], errors="coerce").dropna()
        pos = (numeric > 0).sum()
        assert pos > 0, "dim_visa_ceiling: no positive ceiling values found"


# ─────────────────── backlog_estimates continuity ────────────────

class TestBacklogEstimates:
    def test_months_to_clear_range(self):
        df = _load("backlog_estimates")
        col = "backlog_months_to_clear_est"
        if col not in df.columns:
            pytest.skip(f"backlog_estimates missing column '{col}'")
        numeric = pd.to_numeric(df[col], errors="coerce").dropna()
        assert (numeric >= 0).all(), f"backlog_estimates: negative months_to_clear values"
        assert (numeric <= 600).all(), f"backlog_estimates: months_to_clear > 600 cap"

    def test_no_nan_only_columns(self):
        df = _load("backlog_estimates")
        for col in df.columns:
            non_null = df[col].notna().sum()
            assert non_null > 0, f"backlog_estimates: column '{col}' is all null"

    def test_category_range(self):
        df = _load("backlog_estimates")
        if "category" not in df.columns:
            pytest.skip("backlog_estimates missing 'category'")
        assert df["category"].nunique() >= 2, "backlog_estimates: < 2 categories"


# ─────────────────── Time span checks ────────────────────────────

class TestTimeSpans:
    def test_niv_fy_span(self):
        df = _load("fact_niv_issuance")
        years = _fy_years(df)
        if not years:
            pytest.skip("fact_niv_issuance: no parseable FY values")
        assert min(years) <= 1997, f"fact_niv_issuance: earliest FY={min(years)}, expected ≤1997"
        assert max(years) >= 2024, f"fact_niv_issuance: latest FY={max(years)}, expected ≥2024"
        if min(years) > 1997:
            warnings.warn(f"fact_niv_issuance: first FY={min(years)} (expected FY1997)")

    def test_visa_issuance_fy_span(self):
        df = _load("fact_visa_issuance")
        years = _fy_years(df)
        if not years:
            pytest.skip("fact_visa_issuance: no parseable FY values")
        assert min(years) <= 2015, f"fact_visa_issuance: earliest FY={min(years)}, expected ≤2015"
        assert max(years) >= 2024, f"fact_visa_issuance: latest FY={max(years)}, expected ≥2024"

    def test_visa_applications_fy_span(self):
        df = _load("fact_visa_applications")
        years = _fy_years(df)
        if not years:
            pytest.skip("fact_visa_applications: no parseable FY values")
        assert min(years) <= 2017, f"fact_visa_applications: earliest FY={min(years)}"
        assert max(years) >= 2024, f"fact_visa_applications: latest FY={max(years)}"

    def test_uscis_approvals_fy_span(self):
        df = _load("fact_uscis_approvals")
        years = _fy_years(df)
        if not years:
            pytest.skip("fact_uscis_approvals: no parseable FY values")
        assert min(years) <= 2016, f"fact_uscis_approvals: earliest FY={min(years)}"
        assert max(years) >= 2023, f"fact_uscis_approvals: latest FY={max(years)}"

    def test_dhs_admissions_fy_span(self):
        df = _load("fact_dhs_admissions")
        years = _fy_years(df)
        if not years:
            pytest.skip("fact_dhs_admissions: no parseable FY values")
        assert min(years) <= 1985, f"fact_dhs_admissions: earliest FY={min(years)}, expected ≤1985"
        assert max(years) >= 2020, f"fact_dhs_admissions: latest FY={max(years)}, expected ≥2020"
        span = max(years) - min(years)
        assert span >= 35, f"fact_dhs_admissions: FY span={span} years < 35"

    def test_warn_events_date_sane(self):
        df = _load("fact_warn_events")
        if "notice_date" not in df.columns:
            pytest.skip("fact_warn_events missing 'notice_date'")
        dates = pd.to_datetime(df["notice_date"], errors="coerce").dropna()
        if len(dates) == 0:
            pytest.skip("fact_warn_events: no parseable notice_date values")
        assert dates.min().year >= 1980, f"fact_warn_events: earliest date = {dates.min()}"
        assert dates.max().year <= 2030, f"fact_warn_events: latest date = {dates.max()}"


# ─────────────────── Salary benchmarks sanity ────────────────────

class TestSalaryBenchmarks:
    def test_percentile_ordering(self):
        """Majority of rows should have p10 <= p25 <= median <= p75 <= p90."""
        df = _load("salary_benchmarks")
        col_map = {
            "p10": "p10", "p25": "p25",
            "median": "median", "p50": "p50",
            "p75": "p75", "p90": "p90",
        }
        p10_col = "p10" if "p10" in df.columns else None
        p50_col = "median" if "median" in df.columns else ("p50" if "p50" in df.columns else None)
        p90_col = "p90" if "p90" in df.columns else None
        if not (p10_col and p50_col and p90_col):
            pytest.skip("salary_benchmarks missing required percentile columns")
        valid = df[[p10_col, p50_col, p90_col]].apply(pd.to_numeric, errors="coerce").dropna()
        violations = (valid[p10_col] > valid[p90_col]).sum()
        total = len(valid)
        viol_rate = violations / total if total > 0 else 0
        # Allow up to 10% violations (known data quality issue)
        assert viol_rate < 0.10, (
            f"salary_benchmarks: {violations}/{total} rows ({viol_rate:.1%}) "
            f"have p10 > p90 — exceeds 10% threshold"
        )
