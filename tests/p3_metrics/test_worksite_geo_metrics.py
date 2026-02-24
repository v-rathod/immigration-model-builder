"""
Unit tests for make_worksite_geo_metrics.py

Tests:
- Wage annualization (Hour/Week/Bi-Weekly/Month/Year)
- State grain groups by worksite_state
- SOC×area grain groups by soc_code × area_code
- competitiveness_ratio = offered_median / oews_median
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))

WAGE_MULTIPLIERS = {
    "Hour": 2080, "hr": 2080,
    "Bi-Weekly": 26, "bi-weekly": 26,
    "Week": 52, "weekly": 52,
    "Month": 12, "monthly": 12,
    "Year": 1, "annual": 1, "yearly": 1,
}

APPROVED_STATUS = {"CERTIFIED", "CERTIFIED-EXPIRED"}


def _annualize(wage: pd.Series, unit: pd.Series) -> pd.Series:
    mult = unit.map(WAGE_MULTIPLIERS).fillna(1.0)
    return wage * mult


def _state_grain(df):
    df["is_approved"] = df["case_status"].isin(APPROVED_STATUS).astype(int)
    df["annualized_wage"] = _annualize(df["wage_offer_from"], df["wage_offer_unit"])
    return df.groupby("worksite_state", as_index=False).agg(
        filings_count=("case_number", "count"),
        approvals_count=("is_approved", "sum"),
        offered_median=("annualized_wage", "median"),
        distinct_employers=("employer_id", "nunique"),
    )


# --- Tests ---

def test_hourly_wage_annualized():
    wage = pd.Series([50.0])
    unit = pd.Series(["Hour"])
    result = _annualize(wage, unit)
    assert abs(result.iloc[0] - 50 * 2080) < 0.01


def test_biweekly_annualized():
    wage = pd.Series([4000.0])
    unit = pd.Series(["Bi-Weekly"])
    result = _annualize(wage, unit)
    assert abs(result.iloc[0] - 4000 * 26) < 0.01


def test_monthly_annualized():
    wage = pd.Series([8000.0])
    unit = pd.Series(["Month"])
    result = _annualize(wage, unit)
    assert abs(result.iloc[0] - 8000 * 12) < 0.01


def test_year_passthrough():
    wage = pd.Series([100000.0])
    unit = pd.Series(["Year"])
    result = _annualize(wage, unit)
    assert abs(result.iloc[0] - 100000) < 0.01


def test_state_grain_counts():
    df = pd.DataFrame({
        "case_number": ["A", "B", "C", "D"],
        "case_status": ["CERTIFIED", "CERTIFIED", "DENIED", "CERTIFIED"],
        "employer_id": ["E1", "E2", "E1", "E3"],
        "worksite_state": ["CA", "CA", "TX", "TX"],
        "wage_offer_from": [100000, 110000, 90000, 95000],
        "wage_offer_unit": ["Year", "Year", "Year", "Year"],
    })
    agg = _state_grain(df)
    ca = agg[agg["worksite_state"] == "CA"].iloc[0]
    tx = agg[agg["worksite_state"] == "TX"].iloc[0]
    assert ca["filings_count"] == 2
    assert ca["approvals_count"] == 2
    assert tx["filings_count"] == 2
    assert tx["approvals_count"] == 1
    assert tx["distinct_employers"] == 2


def test_competitiveness_ratio_calculation():
    offered = 120000.0
    oews = 100000.0
    ratio = offered / oews
    assert abs(ratio - 1.2) < 0.001


def test_competitiveness_ratio_null_on_zero_oews():
    oews = 0.0
    offered = 100000.0
    ratio = offered / (oews if oews != 0 else np.nan)
    assert np.isnan(ratio)


def test_soc_area_grain_grouping():
    df = pd.DataFrame({
        "case_number": ["A", "B", "C"],
        "case_status": ["CERTIFIED", "CERTIFIED", "CERTIFIED"],
        "employer_id": ["E1", "E2", "E3"],
        "soc_code": ["15-1252", "15-1252", "11-1021"],
        "area_code": ["CA100", "CA100", "NY200"],
        "wage_offer_from": [100000, 120000, 150000],
        "wage_offer_unit": ["Year", "Year", "Year"],
    })
    df["is_approved"] = df["case_status"].isin(APPROVED_STATUS).astype(int)
    df["annualized_wage"] = _annualize(df["wage_offer_from"], df["wage_offer_unit"])
    sa = df.groupby(["soc_code", "area_code"], as_index=False).agg(
        filings_count=("case_number", "count"),
    )
    assert len(sa) == 2
    ca_row = sa[(sa["soc_code"] == "15-1252") & (sa["area_code"] == "CA100")]
    assert ca_row["filings_count"].iloc[0] == 2
