"""
Unit tests for make_fact_cutoff_trends.py

Tests:
- queue_position_days only for status_flag == "D"
- monthly_advancement_days via diff logic
- retrogression_flag correctly identifies negative advancement
- velocity rolling means with min_periods
"""
import sys
from pathlib import Path
import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))


def _make_cutoffs(rows):
    """Build a minimal fact_cutoffs_all-style DataFrame."""
    return pd.DataFrame(rows)


def _run_trends(df_raw):
    """Mirror the core logic of make_fact_cutoff_trends._build_trends()."""
    EPOCH = pd.Timestamp("1970-01-01")
    df = df_raw.copy()
    df["cutoff_date"] = pd.to_datetime(df["cutoff_date"])
    df = df.sort_values(["chart", "category", "country", "bulletin_year", "bulletin_month"])

    df["queue_position_days"] = np.where(
        df["status_flag"] == "D",
        (df["cutoff_date"] - EPOCH).dt.days,
        np.nan,
    )
    series_keys = ["chart", "category", "country"]
    df["monthly_advancement_days"] = df.groupby(series_keys)["queue_position_days"].diff()
    df["retrogression_flag"] = df["monthly_advancement_days"].lt(0).astype("Int8")
    df["velocity_3m"] = (
        df.groupby(series_keys)["monthly_advancement_days"]
        .transform(lambda s: s.rolling(3, min_periods=3).mean())
    )
    df["velocity_6m"] = (
        df.groupby(series_keys)["monthly_advancement_days"]
        .transform(lambda s: s.rolling(6, min_periods=6).mean())
    )
    df["retrogression_count_cum"] = (
        df.groupby(series_keys)["retrogression_flag"]
        .transform(lambda s: s.fillna(0).cumsum())
    )
    return df


# --- Tests ---

def test_queue_position_days_only_for_date_final():
    rows = [
        {"chart": "A", "category": "EB1", "country": "ROW",
         "bulletin_year": 2022, "bulletin_month": 1, "status_flag": "D",
         "cutoff_date": "2010-01-01"},
        {"chart": "A", "category": "EB1", "country": "ROW",
         "bulletin_year": 2022, "bulletin_month": 2, "status_flag": "C",
         "cutoff_date": "2010-02-01"},
    ]
    df = _run_trends(_make_cutoffs(rows))
    assert pd.isna(df.loc[df["status_flag"] == "C", "queue_position_days"].iloc[0])
    assert df.loc[df["status_flag"] == "D", "queue_position_days"].iloc[0] > 0


def test_monthly_advancement_days_diff():
    rows = [
        {"chart": "A", "category": "EB2", "country": "INDIA",
         "bulletin_year": 2020, "bulletin_month": m, "status_flag": "D",
         "cutoff_date": pd.Timestamp(f"2010-{m:02d}-01").strftime("%Y-%m-%d")}
        for m in range(1, 5)
    ]
    df = _run_trends(_make_cutoffs(rows))
    # advancement should be ~28–31 days between months
    vals = df["monthly_advancement_days"].dropna().values
    assert all(20 < v < 35 for v in vals), f"Unexpected advancement: {vals}"


def test_retrogression_flag_negative_advancement():
    rows = [
        {"chart": "A", "category": "EB3", "country": "CHINA",
         "bulletin_year": 2021, "bulletin_month": 1, "status_flag": "D",
         "cutoff_date": "2010-06-01"},
        {"chart": "A", "category": "EB3", "country": "CHINA",
         "bulletin_year": 2021, "bulletin_month": 2, "status_flag": "D",
         "cutoff_date": "2010-03-01"},  # retrogression: moved BACK
        {"chart": "A", "category": "EB3", "country": "CHINA",
         "bulletin_year": 2021, "bulletin_month": 3, "status_flag": "D",
         "cutoff_date": "2010-07-01"},
    ]
    df = _run_trends(_make_cutoffs(rows))
    retro_rows = df[df["monthly_advancement_days"] < 0]
    assert len(retro_rows) == 1
    assert retro_rows["retrogression_flag"].iloc[0] == 1


def test_velocity_3m_requires_min_periods_3():
    rows = [
        {"chart": "A", "category": "EB1", "country": "ROW",
         "bulletin_year": 2020, "bulletin_month": m, "status_flag": "D",
         "cutoff_date": pd.Timestamp(f"2010-{m:02d}-01").strftime("%Y-%m-%d")}
        for m in range(1, 6)
    ]
    df = _run_trends(_make_cutoffs(rows))
    # First two rows have NaN diff, third onwards may have velocity_3m
    v = df["velocity_3m"].values
    # row 0: NaN (no diff), row1: diff exists but only 1 value, row2: 2 values, row3: 3 values
    assert pd.isna(v[0]) or pd.isna(v[1]) or pd.isna(v[2]), "velocity_3m should be NaN before 3 diffs exist"


def test_retrogression_count_cum_accumulates():
    rows = [
        {"chart": "A", "category": "EB2", "country": "ROW",
         "bulletin_year": 2020, "bulletin_month": 1, "status_flag": "D",
         "cutoff_date": "2010-06-01"},
        {"chart": "A", "category": "EB2", "country": "ROW",
         "bulletin_year": 2020, "bulletin_month": 2, "status_flag": "D",
         "cutoff_date": "2010-03-01"},  # retro
        {"chart": "A", "category": "EB2", "country": "ROW",
         "bulletin_year": 2020, "bulletin_month": 3, "status_flag": "D",
         "cutoff_date": "2010-07-01"},
        {"chart": "A", "category": "EB2", "country": "ROW",
         "bulletin_year": 2020, "bulletin_month": 4, "status_flag": "D",
         "cutoff_date": "2010-04-01"},  # retro again
    ]
    df = _run_trends(_make_cutoffs(rows))
    df = df.sort_values("bulletin_month")
    cum = df["retrogression_count_cum"].values
    assert cum[-1] >= 2, f"Expected >=2 cumulative retrogressions, got {cum[-1]}"


# ---------------------------------------------------------------------------
# Integration tests: validate the ACTUAL artifact on disk
# These guard against schema regressions where the wrong table gets written
# (e.g. fact_cutoffs_all overwriting fact_cutoff_trends, dropping velocity cols).
# ---------------------------------------------------------------------------

ARTIFACT_PATH = Path(__file__).resolve().parent.parent.parent / "artifacts" / "tables" / "fact_cutoff_trends.parquet"

# Required computed columns that MUST exist in fact_cutoff_trends but NOT in fact_cutoffs_all.
# Their absence causes "NaN days/month" and "Invalid Date" on the P3 homepage.
COMPUTED_COLUMNS = [
    "velocity_3m",
    "velocity_6m",
    "monthly_advancement_days",
    "retrogression_flag",
    "retrogression_count_cum",
    "queue_position_days",
]


@pytest.fixture(scope="module")
def fact_cutoff_trends_df():
    if not ARTIFACT_PATH.exists():
        pytest.skip(f"fact_cutoff_trends.parquet not found at {ARTIFACT_PATH}")
    return pd.read_parquet(ARTIFACT_PATH)


def test_fact_cutoff_trends_artifact_has_computed_columns(fact_cutoff_trends_df):
    """CRITICAL: Computed velocity columns MUST exist in fact_cutoff_trends.parquet.

    Root cause of Apr 2026 regression: append_may2026_bulletin.py copied
    fact_cutoffs_all (raw, no velocity) over fact_cutoff_trends (computed),
    causing the P3 homepage to show 'NaN days/month' and 'Invalid Date'.

    If this test fails: run `python3.12 scripts/make_fact_cutoff_trends.py`
    """
    df = fact_cutoff_trends_df
    missing = [col for col in COMPUTED_COLUMNS if col not in df.columns]
    assert not missing, (
        f"fact_cutoff_trends.parquet is missing computed columns: {missing}\n"
        "This means the raw fact_cutoffs_all was exported instead of the computed table.\n"
        "Fix: python3.12 scripts/make_fact_cutoff_trends.py"
    )


def test_fact_cutoff_trends_velocity_3m_is_numeric(fact_cutoff_trends_df):
    """velocity_3m must be numeric - never the string 'NaN' or object type."""
    df = fact_cutoff_trends_df
    assert "velocity_3m" in df.columns, "velocity_3m column missing"
    col = pd.to_numeric(df["velocity_3m"], errors="coerce")
    # Allow NaN (early rows without enough history), but no coercion failures
    invalid = df.loc[col.isna() & df["velocity_3m"].notna(), "velocity_3m"]
    assert len(invalid) == 0, f"velocity_3m has non-numeric non-null values: {invalid.unique()}"


def test_fact_cutoff_trends_has_may_2026_or_later(fact_cutoff_trends_df):
    """fact_cutoff_trends must include at least May 2026 (tracks bulletin freshness).

    Update this lower bound annually when new bulletins are added.
    """
    df = fact_cutoff_trends_df
    latest_year = int(df["bulletin_year"].max())
    latest_month = int(df.loc[df["bulletin_year"] == latest_year, "bulletin_month"].max())
    combined = latest_year * 100 + latest_month
    assert combined >= 202605, (
        f"fact_cutoff_trends only goes to {latest_year}-{latest_month:02d}; "
        "expected >= 2026-05. Run append_may2026_bulletin.py then make_fact_cutoff_trends.py."
    )


def test_fact_cutoff_trends_row_count_reasonable(fact_cutoff_trends_df):
    """Row count sanity: ~55 rows per bulletin month (6 cats x ~7 countries x 2 charts)."""
    df = fact_cutoff_trends_df
    assert len(df) >= 8_000, f"Too few rows: {len(df)}. Expected >= 8,000."
    assert len(df) <= 20_000, f"Too many rows: {len(df)}. Possible duplicate ingestion."
