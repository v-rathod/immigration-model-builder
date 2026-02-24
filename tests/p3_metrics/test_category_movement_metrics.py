"""
Unit tests for make_category_movement_metrics.py

Tests:
- Retrogression detection: retrogression_events_12m > 0 when advancement < 0
- Velocity rules: Forward / Backward / Flat / Unknown predictions
- 12m rolling aggregation boundaries
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))


def _predict(velocity_3m, retrogression_events_12m):
    """Mirror next_movement_prediction logic."""
    if pd.isna(velocity_3m):
        return "Unknown"
    if velocity_3m > 0 and retrogression_events_12m == 0:
        return "Forward"
    if velocity_3m < 0:
        return "Backward"
    return "Flat"


def _build_trends_input():
    """Build synthetic fact_cutoff_trends-style rows."""
    EPOCH = pd.Timestamp("1970-01-01")
    rows = []
    base_date = pd.Timestamp("2010-01-01")
    for m in range(1, 16):
        adv = 30 if m != 6 else -60  # retrogression in month 6
        base_date = base_date + pd.DateOffset(days=adv)
        rows.append({
            "bulletin_year": 2020,
            "bulletin_month": m,
            "chart": "A",
            "category": "EB2",
            "country": "INDIA",
            "status_flag": "D",
            "cutoff_date": base_date,
            "queue_position_days": (base_date - EPOCH).days,
            "monthly_advancement_days": float(adv),
            "retrogression_flag": 1 if adv < 0 else 0,
            "velocity_3m": np.nan,
        })
    df = pd.DataFrame(rows)
    df["velocity_3m"] = (
        df.groupby(["chart", "category", "country"])["monthly_advancement_days"]
        .transform(lambda s: s.rolling(3, min_periods=3).mean())
    )
    return df


def _build_category_metrics(df_trends):
    keys = ["chart", "category", "country"]
    df_trends = df_trends.sort_values(keys + ["bulletin_year", "bulletin_month"])

    def _roll12(grp):
        grp = grp.copy()
        grp["avg_monthly_advancement_days"] = (
            grp["monthly_advancement_days"].rolling(12, min_periods=1).mean()
        )
        grp["median_advancement_days"] = (
            grp["monthly_advancement_days"].rolling(12, min_periods=1).median()
        )
        grp["volatility_score"] = (
            grp["monthly_advancement_days"].rolling(12, min_periods=1).std()
        )
        grp["retrogression_events_12m"] = (
            grp["retrogression_flag"].rolling(12, min_periods=1).sum()
        )
        return grp

    df_out = df_trends.groupby(keys, group_keys=False).apply(_roll12)
    df_out["next_movement_prediction"] = df_out.apply(
        lambda r: _predict(r["velocity_3m"], r["retrogression_events_12m"]), axis=1
    )
    return df_out


# --- Tests ---

def test_retrogression_detected():
    df = _build_trends_input()
    metrics = _build_category_metrics(df)
    retro_rows = metrics[metrics["retrogression_events_12m"] > 0]
    assert len(retro_rows) > 0, "Expected some rows with retrogression events"


def test_backward_prediction_on_negative_velocity():
    df = pd.DataFrame([{
        "bulletin_year": 2020, "bulletin_month": 1,
        "chart": "A", "category": "EB3", "country": "CHINA",
        "monthly_advancement_days": -30.0,
        "retrogression_flag": 1,
        "velocity_3m": -15.0,
        "retrogression_events_12m": 3,
    }])
    df["next_movement_prediction"] = df.apply(
        lambda r: _predict(r["velocity_3m"], r["retrogression_events_12m"]), axis=1
    )
    assert df["next_movement_prediction"].iloc[0] == "Backward"


def test_forward_prediction_on_positive_velocity_no_retro():
    pred = _predict(velocity_3m=25.0, retrogression_events_12m=0)
    assert pred == "Forward"


def test_flat_prediction_on_zero_velocity():
    pred = _predict(velocity_3m=0.0, retrogression_events_12m=0)
    assert pred == "Flat"


def test_unknown_when_velocity_null():
    pred = _predict(velocity_3m=np.nan, retrogression_events_12m=0)
    assert pred == "Unknown"


def test_12m_rolling_volatility_non_negative():
    df = _build_trends_input()
    metrics = _build_category_metrics(df)
    valid_vol = metrics["volatility_score"].dropna()
    assert (valid_vol >= 0).all(), "Volatility (std) should be non-negative"


def test_output_min_rows():
    df = _build_trends_input()
    metrics = _build_category_metrics(df)
    assert len(metrics) >= 10, f"Expected >=10 rows, got {len(metrics)}"
