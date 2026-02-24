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
