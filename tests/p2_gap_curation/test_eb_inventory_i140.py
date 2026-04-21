"""
Tests for fact_eb_inventory and fact_i140_demand tables.

Validates schema, primary keys, value ranges, cross-table referential integrity,
and data quality for the new USCIS I-485 inventory and I-140 demand tables.
"""
import pathlib
import pandas as pd
import pytest

TABLES_DIR = pathlib.Path("artifacts/tables")


def load(name: str) -> pd.DataFrame:
    p = TABLES_DIR / f"{name}.parquet"
    if not p.exists():
        pytest.skip(f"{name}.parquet not found")
    return pd.read_parquet(p)


# ======================================================================
# fact_eb_inventory
# ======================================================================

class TestFactEbInventory:
    """Tests for the I-485 pending inventory table."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.df = load("fact_eb_inventory")

    def test_not_empty(self):
        assert len(self.df) > 10_000, f"Expected >10K rows, got {len(self.df)}"

    def test_required_columns(self):
        required = {"snapshot_date", "country", "category", "visa_status",
                     "pd_month", "pd_year", "pending_count"}
        missing = required - set(self.df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_countries(self):
        expected = {"ROW", "CHN", "IND", "MEX", "PHL"}
        actual = set(self.df["country"].unique())
        # At least the 5 main countries must be present
        assert expected.issubset(actual), f"Missing countries: {expected - actual}"

    def test_categories(self):
        expected = {"EB1", "EB2", "EB3"}
        actual = set(self.df["category"].unique())
        assert expected.issubset(actual), f"Missing categories: {expected - actual}"

    def test_visa_status_values(self):
        valid = {"Available", "Awaiting Availability"}
        actual = set(self.df["visa_status"].unique())
        assert actual.issubset(valid), f"Unexpected visa_status values: {actual - valid}"

    def test_pd_month_range(self):
        assert self.df["pd_month"].dropna().between(1, 12).all()

    def test_pending_count_non_negative(self):
        assert (self.df["pending_count"].dropna() >= 0).all()

    def test_multiple_snapshots(self):
        n_snapshots = self.df["snapshot_date"].nunique()
        assert n_snapshots >= 10, f"Expected >=10 snapshot dates, got {n_snapshots}"

    def test_india_eb2_has_substantial_backlog(self):
        """India EB2 should have the largest backlog - sanity check."""
        latest = self.df["snapshot_date"].max()
        india_eb2 = self.df[
            (self.df["snapshot_date"] == latest)
            & (self.df["country"] == "IND")
            & (self.df["category"] == "EB2")
        ]
        total = india_eb2["pending_count"].sum()
        assert total > 1_000, f"India EB2 total pending {total} suspiciously low"

    def test_pk_uniqueness(self):
        """Each (snapshot_date, country, category, visa_status, pd_month, pd_year)
        should be unique."""
        pk_cols = ["snapshot_date", "country", "category", "visa_status",
                   "pd_month", "pd_year"]
        dupes = self.df.duplicated(subset=pk_cols, keep=False).sum()
        assert dupes == 0, f"{dupes} duplicate PK rows"


# ======================================================================
# fact_i140_demand
# ======================================================================

class TestFactI140Demand:
    """Tests for the I-140 petition demand table."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.df = load("fact_i140_demand")

    def test_not_empty(self):
        assert len(self.df) > 100, f"Expected >100 rows, got {len(self.df)}"

    def test_required_columns(self):
        required = {"report_period", "country", "category", "fiscal_year",
                     "received", "approved", "denied", "pending"}
        missing = required - set(self.df.columns)
        assert not missing, f"Missing columns: {missing}"

    def test_countries_include_key_nations(self):
        actual = set(self.df["country"].unique())
        assert "IND" in actual, "India missing"
        assert "CHN" in actual, "China missing"
        assert "ALL" in actual, "All Countries aggregate missing"

    def test_categories(self):
        expected = {"EB1", "EB2", "EB3", "TOTAL"}
        actual = set(self.df["category"].unique())
        assert expected.issubset(actual), f"Missing categories: {expected - actual}"

    def test_fiscal_year_range(self):
        fy = self.df["fiscal_year"].dropna()
        assert fy.min() >= 2012
        assert fy.max() <= 2030

    def test_counts_non_negative(self):
        for col in ("received", "approved", "denied", "pending"):
            assert (self.df[col].dropna() >= 0).all(), f"{col} has negative values"

    def test_approved_lte_received(self):
        """Approved should never exceed received for any row."""
        valid = self.df["approved"] <= self.df["received"]
        violations = (~valid).sum()
        assert violations == 0, f"{violations} rows with approved > received"

    def test_india_fy2025_totals(self):
        """Sanity check: India total approved I-140 across all FYs should be >500K."""
        latest = sorted(self.df["report_period"].unique())[-1]
        india_total = self.df[
            (self.df["report_period"] == latest)
            & (self.df["country"] == "IND")
            & (self.df["category"] == "TOTAL")
        ]
        total_approved = india_total["approved"].sum()
        assert total_approved > 500_000, \
            f"India total approved {total_approved:,} suspiciously low (expected >500K)"

    def test_multiple_report_periods(self):
        n_periods = self.df["report_period"].nunique()
        assert n_periods >= 5, f"Expected >=5 report periods, got {n_periods}"


# ======================================================================
# Cross-table: latent demand calculation
# ======================================================================

class TestLatentDemand:
    """Cross-table test: I-140 approved vs I-485 pending (latent demand)."""

    @pytest.fixture(autouse=True)
    def _load(self):
        self.inv = load("fact_eb_inventory")
        self.i140 = load("fact_i140_demand")

    def test_india_eb2_latent_demand_positive(self):
        """For India EB2, cumulative approved I-140s should exceed pending I-485s.
        The difference is the 'latent demand' - people with approved I-140 who
        haven't filed I-485 yet."""
        # Latest I-140 data for India
        latest_period = sorted(self.i140["report_period"].unique())[-1]
        india_eb2_i140 = self.i140[
            (self.i140["report_period"] == latest_period)
            & (self.i140["country"] == "IND")
            & (self.i140["category"] == "EB2")
        ]
        total_approved_i140 = india_eb2_i140["approved"].sum()

        # Latest I-485 inventory for India EB2
        latest_snap = self.inv["snapshot_date"].max()
        india_eb2_inv = self.inv[
            (self.inv["snapshot_date"] == latest_snap)
            & (self.inv["country"] == "IND")
            & (self.inv["category"] == "EB2")
        ]
        total_pending_i485 = india_eb2_inv["pending_count"].sum()

        latent = total_approved_i140 - total_pending_i485
        assert latent > 0, \
            f"Latent demand should be positive: I-140 approved={total_approved_i140:,}, I-485 pending={total_pending_i485:,}"
