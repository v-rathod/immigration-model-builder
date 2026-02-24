"""
Unit tests for make_salary_benchmarks.py — Patch A

Asserts:
- After enforce_monotonic, ALL rows with all five non-null values satisfy
  p10 <= p25 <= median <= p75 <= p90.  Violations == 0 (FAIL if > 0).
- enforce_monotonic corrects scrambled rows and leaves correct rows unchanged.
- Hourly-to-annual fallback works.
- National rows present with area_code = None.
- Integration: built parquet has 0 violations.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

from make_salary_benchmarks import enforce_monotonic, PCT_COLS  # noqa: E402


# ── helpers ──────────────────────────────────────────────────────────────────

def _sample_oews():
    return [
        {"soc_code": "15-1252", "area_code": "CA",
         "a_pct10": 60000, "a_pct25": 80000, "a_median": 100000, "a_pct75": 120000, "a_pct90": 140000,
         "h_pct10": None, "h_pct25": None, "h_median": None, "h_pct75": None, "h_pct90": None},
        {"soc_code": "15-1252", "area_code": "TX",
         "a_pct10": 55000, "a_pct25": 75000, "a_median": 90000, "a_pct75": 110000, "a_pct90": 130000,
         "h_pct10": None, "h_pct25": None, "h_median": None, "h_pct75": None, "h_pct90": None},
        {"soc_code": "11-1021", "area_code": "NY",
         "a_pct10": 70000, "a_pct25": 90000, "a_median": 120000, "a_pct75": 150000, "a_pct90": 180000,
         "h_pct10": None, "h_pct25": None, "h_median": None, "h_pct75": None, "h_pct90": None},
    ]


def _build_benchmarks_inline(df_oews: pd.DataFrame) -> pd.DataFrame:
    """Mirror build pipeline without I/O."""
    df = df_oews.copy()

    def _ann(a_col_name, h_col_name):
        a = pd.to_numeric(df.get(a_col_name, pd.Series(np.nan, index=df.index)), errors="coerce")
        h = pd.to_numeric(df.get(h_col_name, pd.Series(np.nan, index=df.index)), errors="coerce")
        result = a.copy()
        mask = result.isna() & h.notna()
        result.loc[mask] = h.loc[mask] * 2080
        return result

    df["p10"] = _ann("a_pct10", "h_pct10")
    df["p25"] = _ann("a_pct25", "h_pct25")
    df["median"] = _ann("a_median", "h_median")
    df["p75"] = _ann("a_pct75", "h_pct75")
    df["p90"] = _ann("a_pct90", "h_pct90")

    df_area = df[["soc_code", "area_code"] + PCT_COLS].groupby(
        ["soc_code", "area_code"], as_index=False
    ).median(numeric_only=True)
    df_nat = df[["soc_code"] + PCT_COLS].groupby("soc_code", as_index=False).median(numeric_only=True)
    df_nat["area_code"] = None
    df_out = pd.concat([df_area, df_nat[["soc_code", "area_code"] + PCT_COLS]], ignore_index=True)

    log_lines: list = []
    df_out, _ = enforce_monotonic(df_out, log_lines)
    return df_out


# ── unit tests ────────────────────────────────────────────────────────────────

def test_zero_violations_after_enforce():
    """enforce_monotonic must produce 0 ordering violations."""
    # Include a deliberately scrambled row
    rows = _sample_oews() + [
        {"soc_code": "17-2051", "area_code": "WA",
         "a_pct10": 200000, "a_pct25": 80000, "a_median": 100000,  # p10 > p25 violation
         "a_pct75": 120000, "a_pct90": 140000,
         "h_pct10": None, "h_pct25": None, "h_median": None, "h_pct75": None, "h_pct90": None},
    ]
    df_out = _build_benchmarks_inline(pd.DataFrame(rows))
    full = df_out[df_out[PCT_COLS].notna().all(axis=1)]
    violations = 0
    for lo, hi in [("p10", "p25"), ("p25", "median"), ("median", "p75"), ("p75", "p90")]:
        violations += int((full[lo] > full[hi]).sum())
    assert violations == 0, f"Expected 0 violations, got {violations}"


def test_enforce_corrects_scrambled_row():
    rows = pd.DataFrame([{"p10": 500.0, "p25": 100.0, "median": 200.0, "p75": 300.0, "p90": 400.0}])
    log: list = []
    result, n = enforce_monotonic(rows, log)
    assert n == 1
    assert result.iloc[0][PCT_COLS].tolist() == sorted(result.iloc[0][PCT_COLS].tolist())


def test_enforce_leaves_correct_row_unchanged():
    rows = pd.DataFrame([{"p10": 100.0, "p25": 200.0, "median": 300.0, "p75": 400.0, "p90": 500.0}])
    log: list = []
    _, n = enforce_monotonic(rows, log)
    assert n == 0


def test_enforce_skips_null_rows():
    rows = pd.DataFrame([{"p10": np.nan, "p25": 200.0, "median": 300.0, "p75": 400.0, "p90": 500.0}])
    log: list = []
    result, n = enforce_monotonic(rows, log)
    assert n == 0
    assert np.isnan(result.iloc[0]["p10"])


def test_national_rows_have_null_area_code():
    df_out = _build_benchmarks_inline(pd.DataFrame(_sample_oews()))
    nat = df_out[df_out["area_code"].isna()]
    assert len(nat) > 0
    assert "15-1252" in nat["soc_code"].values


def test_hourly_annualized_when_annual_missing():
    rows = [
        {"soc_code": "17-2051", "area_code": "WA",
         "a_pct10": None, "a_pct25": None, "a_median": None, "a_pct75": None, "a_pct90": None,
         "h_pct10": 25.0, "h_pct25": 35.0, "h_median": 50.0, "h_pct75": 65.0, "h_pct90": 80.0},
    ]
    df_out = _build_benchmarks_inline(pd.DataFrame(rows))
    row = df_out[df_out["area_code"] == "WA"].iloc[0]
    assert abs(row["median"] - 50.0 * 2080) < 1
    assert abs(row["p10"] - 25.0 * 2080) < 1


def test_no_duplicate_soc_area_pairs():
    rows = _sample_oews() + [
        {"soc_code": "15-1252", "area_code": "CA",
         "a_pct10": 62000, "a_pct25": 82000, "a_median": 102000, "a_pct75": 122000, "a_pct90": 142000,
         "h_pct10": None, "h_pct25": None, "h_median": None, "h_pct75": None, "h_pct90": None},
    ]
    df_out = _build_benchmarks_inline(pd.DataFrame(rows))
    area_rows = df_out[df_out["area_code"].notna()]
    dupes = area_rows.duplicated(subset=["soc_code", "area_code"]).sum()
    assert dupes == 0


# ── integration test ──────────────────────────────────────────────────────────

def test_actual_parquet_zero_violations():
    """Read built parquet; assert 0 violations (FAIL if any)."""
    pq = ROOT / "artifacts" / "tables" / "salary_benchmarks.parquet"
    if not pq.exists():
        pytest.skip("salary_benchmarks.parquet not built yet")
    df = pd.read_parquet(pq)
    full = df[df[PCT_COLS].notna().all(axis=1)]
    violations = 0
    for lo, hi in [("p10", "p25"), ("p25", "median"), ("median", "p75"), ("p75", "p90")]:
        violations += int((full[lo] > full[hi]).sum())
    assert violations == 0, f"salary_benchmarks.parquet has {violations} ordering violations"
