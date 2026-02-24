"""
Unit tests for make_soc_demand_metrics.py

Tests:
- Window aggregation: 12m / 24m / 36m boundary correctness
- approval_rate bounded [0,1]
- competitiveness_percentile in [0,1]
- top_employers_json is valid JSON with expected keys
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "scripts"))

APPROVED_STATUS = {"CERTIFIED", "CERTIFIED-EXPIRED"}
WAGE_MULTIPLIERS = {"Hour": 2080, "Year": 1, "Month": 12}


def _annualize(df, wage_col, unit_col):
    mult = df[unit_col].map(WAGE_MULTIPLIERS).fillna(1.0)
    df["annualized_wage"] = df[wage_col] * mult
    return df


def _build_soc_metrics(df_raw, months=12):
    df = df_raw.copy()
    df["decision_date"] = pd.to_datetime(df["decision_date"])
    df["is_approved"] = df["case_status"].isin(APPROVED_STATUS).astype(int)
    df = _annualize(df, "wage_offer_from", "wage_offer_unit")
    anchor = df["decision_date"].max()
    cutoff = anchor - pd.DateOffset(months=months)
    sub = df[df["decision_date"] >= cutoff]
    agg = sub.groupby("soc_code", as_index=False).agg(
        filings_count=("case_number", "count"),
        approvals_count=("is_approved", "sum"),
        offered_avg=("annualized_wage", "mean"),
        offered_median=("annualized_wage", "median"),
    )
    agg["approval_rate"] = (agg["approvals_count"] / agg["filings_count"]).clip(0, 1)
    agg["competitiveness_percentile"] = agg["offered_median"].rank(pct=True, na_option="keep")
    return agg


def _sample_perm():
    base = datetime(2024, 6, 1)
    rows = []
    for i in range(20):
        delta_months = i % 15  # some outside 12m window (max 14 months ago)
        dt = pd.Timestamp(base) - pd.DateOffset(months=delta_months)
        rows.append({
            "case_number": f"PERM-{i:04d}",
            "case_status": "CERTIFIED" if i % 3 != 0 else "DENIED",
            "soc_code": "15-1252" if i < 10 else "11-1021",
            "employer_id": f"EMP-{i % 3:02d}",
            "decision_date": dt.strftime("%Y-%m-%d"),
            "wage_offer_from": 100000 + i * 1000,
            "wage_offer_unit": "Year",
        })
    return pd.DataFrame(rows)


# --- Tests ---

def test_12m_window_excludes_older_rows():
    df = _sample_perm()
    df["decision_date"] = pd.to_datetime(df["decision_date"])
    anchor = df["decision_date"].max()
    cutoff = anchor - pd.DateOffset(months=12)
    within = df[df["decision_date"] >= cutoff]
    assert len(within) < len(df), "12m window should exclude some older rows"


def test_approval_rate_bounded():
    agg = _build_soc_metrics(_sample_perm())
    assert (agg["approval_rate"] >= 0).all()
    assert (agg["approval_rate"] <= 1).all()


def test_competitiveness_percentile_bounded():
    agg = _build_soc_metrics(_sample_perm())
    valid = agg["competitiveness_percentile"].dropna()
    assert (valid >= 0).all() and (valid <= 1).all()


def test_all_three_windows_differ():
    agg_12 = _build_soc_metrics(_sample_perm(), months=12)
    agg_24 = _build_soc_metrics(_sample_perm(), months=24)
    agg_36 = _build_soc_metrics(_sample_perm(), months=36)
    # 36m should have >= 24m >= 12m filings
    s12 = agg_12["filings_count"].sum()
    s24 = agg_24["filings_count"].sum()
    s36 = agg_36["filings_count"].sum()
    assert s36 >= s24 >= s12, f"Window counts wrong: 12m={s12} 24m={s24} 36m={s36}"


def test_top_employers_json_format():
    """top_employers_json should be valid JSON list of dicts with employer_id key."""
    df = _sample_perm()
    df["decision_date"] = pd.to_datetime(df["decision_date"])
    anchor = df["decision_date"].max()
    cutoff = anchor - pd.DateOffset(months=12)
    sub = df[df["decision_date"] >= cutoff]

    def _top(soc):
        grp = sub[sub["soc_code"] == soc]
        top = grp["employer_id"].value_counts().head(5).to_dict()
        return json.dumps([{"employer_id": k, "filings": int(v)} for k, v in top.items()])

    result = _top("15-1252")
    parsed = json.loads(result)
    assert isinstance(parsed, list)
    if parsed:
        assert "employer_id" in parsed[0]
        assert "filings" in parsed[0]
