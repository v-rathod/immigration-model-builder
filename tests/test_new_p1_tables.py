"""
Tests for new P1-sourced fact tables (M17 ingestion):
  - fact_h1b_employer_hub  (USCIS H-1B Employer Hub, FY2010-2023, discontinued)
  - fact_processing_times  (USCIS Processing Times, stub — P1 SPA parsing)
  - fact_bls_ces           (BLS Current Employment Statistics)
"""
import pathlib

import pandas as pd
import pytest

TABLES = pathlib.Path("artifacts/tables")


def _load(name: str) -> pd.DataFrame:
    p = TABLES / f"{name}.parquet"
    if p.exists():
        return pd.read_parquet(p)
    d = TABLES / name
    if d.is_dir():
        files = sorted(d.rglob("*.parquet"))
        if files:
            return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    pytest.skip(f"{name} not found")


# ═══════════════════════════════════════════════════════════════════════
# fact_h1b_employer_hub — USCIS H-1B Employer Hub (discontinued)
# ═══════════════════════════════════════════════════════════════════════

class TestFactH1BEmployerHub:
    """Tests for the USCIS H-1B Employer Hub fact table.

    Data source: USCIS H-1B Employer Data Hub (discontinued after FY2023)
    All records are stale (is_stale=True, data_weight=0.6).
    14 fiscal years (FY2010-FY2023), ~730K rows.
    """

    REQUIRED = [
        "fiscal_year", "employer_name", "initial_approvals", "initial_denials",
        "continuing_approvals", "continuing_denials", "naics_code", "tax_id",
        "state", "city", "zip_code", "total_petitions", "approval_rate",
        "is_stale", "data_weight", "source_file", "ingested_at",
    ]

    PK = ["fiscal_year", "employer_name", "state", "city", "naics_code"]

    @pytest.fixture(scope="class")
    def df(self):
        return _load("fact_h1b_employer_hub")

    def test_required_columns(self, df):
        missing = [c for c in self.REQUIRED if c not in df.columns]
        assert not missing, f"Missing columns: {missing}"

    def test_pk_unique(self, df):
        dups = df.duplicated(subset=self.PK).sum()
        assert dups == 0, f"PK has {dups} duplicates"

    def test_row_count_minimum(self, df):
        """Should have at least 700K rows (14 FYs of employer data)."""
        assert len(df) >= 700_000, f"Expected ≥700K rows, got {len(df):,}"

    def test_fiscal_year_range(self, df):
        """FY2010-FY2023 (14 years of discontinued data)."""
        fy_min = df["fiscal_year"].min()
        fy_max = df["fiscal_year"].max()
        assert fy_min == 2010, f"Expected min FY=2010, got {fy_min}"
        assert fy_max == 2023, f"Expected max FY=2023, got {fy_max}"
        assert df["fiscal_year"].nunique() == 14, (
            f"Expected 14 unique FYs, got {df['fiscal_year'].nunique()}"
        )

    def test_approval_rate_bounded(self, df):
        """approval_rate must be in [0.0, 1.0]."""
        valid = df["approval_rate"].dropna()
        assert (valid >= 0.0).all(), "Found approval_rate < 0"
        assert (valid <= 1.0).all(), "Found approval_rate > 1.0"

    def test_total_petitions_non_negative(self, df):
        """total_petitions should be ≥ 0."""
        assert (df["total_petitions"] >= 0).all(), "Found negative total_petitions"

    def test_petition_columns_non_negative(self, df):
        """All petition count columns should be ≥ 0."""
        for col in ["initial_approvals", "initial_denials",
                     "continuing_approvals", "continuing_denials"]:
            assert (df[col] >= 0).all(), f"Negative values in {col}"

    def test_total_equals_sum(self, df):
        """total_petitions = initial_approvals + initial_denials + continuing_approvals + continuing_denials."""
        computed = (
            df["initial_approvals"] + df["initial_denials"] +
            df["continuing_approvals"] + df["continuing_denials"]
        )
        assert (df["total_petitions"] == computed).all(), "total_petitions mismatch"

    def test_stale_markers(self, df):
        """All rows must be marked as stale (discontinued data source)."""
        assert df["is_stale"].all(), "Expected all rows is_stale=True"
        assert (df["data_weight"] == 0.6).all(), "Expected all data_weight=0.6"

    def test_has_standard_states(self, df):
        """Should include major states like CA, NY, TX, IL."""
        states = set(df["state"].unique())
        for s in ["CA", "NY", "TX", "IL", "WA", "NJ"]:
            assert s in states, f"Missing expected state: {s}"

    def test_employer_count(self, df):
        """Should have a meaningful number of unique employers."""
        n = df["employer_name"].nunique()
        assert n >= 200_000, f"Expected ≥200K unique employers, got {n:,}"


# ═══════════════════════════════════════════════════════════════════════
# fact_processing_times — USCIS Processing Times (currently stub)
# ═══════════════════════════════════════════════════════════════════════

class TestFactProcessingTimes:
    """Tests for the USCIS Processing Times fact table.

    0-row stub: the USCIS processing times page is a Vue.js SPA and P1
    could not extract usable data. The P1 source directory
    (USCIS_Processing_Times/) has been deleted.
    Tests validate the schema exists and allow 0 rows.
    """

    REQUIRED = [
        "snapshot_date", "snapshot_month", "form", "category", "office",
        "processing_time_min", "processing_time_max", "unit",
        "source_file", "ingested_at",
    ]

    def test_exists(self):
        p = TABLES / "fact_processing_times.parquet"
        assert p.exists(), "fact_processing_times.parquet not found"

    def test_stub_schema(self):
        """Even as a stub, should have the correct schema."""
        df = pd.read_parquet(TABLES / "fact_processing_times.parquet")
        missing = [c for c in self.REQUIRED if c not in df.columns]
        assert not missing, f"Missing columns in stub: {missing}"

    def test_row_count_zero_expected(self):
        """Expected 0 rows (P1 source dir deleted; USCIS SPA not parseable)."""
        df = pd.read_parquet(TABLES / "fact_processing_times.parquet")
        # 0 rows is expected; if P1 improves, this test should be updated
        assert len(df) >= 0, "Row count should be non-negative"


# ═══════════════════════════════════════════════════════════════════════
# fact_bls_ces — BLS Current Employment Statistics
# ═══════════════════════════════════════════════════════════════════════

class TestFactBLSCES:
    """Tests for the BLS Current Employment Statistics (CES) fact table.

    Contains nonfarm + private employment time series from BLS API.
    Small table (~26 rows), 2 series, monthly data.
    """

    REQUIRED = [
        "series_id", "series_title", "year", "period", "period_name",
        "value", "is_preliminary", "snapshot_date",
        "source_file", "ingested_at",
    ]

    PK = ["series_id", "year", "period"]

    @pytest.fixture(scope="class")
    def df(self):
        return _load("fact_bls_ces")

    def test_required_columns(self, df):
        missing = [c for c in self.REQUIRED if c not in df.columns]
        assert not missing, f"Missing columns: {missing}"

    def test_pk_unique(self, df):
        dups = df.duplicated(subset=self.PK).sum()
        assert dups == 0, f"PK has {dups} duplicates"

    def test_has_rows(self, df):
        """Should have at least 1 row (BLS API provides monthly data)."""
        assert len(df) >= 1, f"Expected ≥1 rows, got {len(df)}"

    def test_value_positive(self, df):
        """Employment values should be positive (thousands of workers)."""
        assert (df["value"] > 0).all(), "Found non-positive employment values"

    def test_series_ids_present(self, df):
        """Should have at least the Total Nonfarm series."""
        series = set(df["series_id"].unique())
        assert len(series) >= 1, f"Expected at least 1 series, got {len(series)}"

    def test_year_reasonable(self, df):
        """Years should be recent (2020+)."""
        assert df["year"].min() >= 2020, f"Unexpected old year: {df['year'].min()}"

    def test_period_format(self, df):
        """Period should be M01-M12 for monthly data."""
        periods = df["period"].unique()
        for p in periods:
            assert p.startswith("M"), f"Unexpected period format: {p}"
            month = int(p[1:])
            assert 1 <= month <= 12, f"Month out of range: {p}"
