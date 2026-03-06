"""
Tests for soc_salary_market.parquet — produced by make_employer_salary_profiles.py.

Regression guards against the median-of-medians bias that was fixed in the
_build_soc_market_summary() function.  Before the fix, the function computed a
*weighted mean of per-employer quantiles* instead of true flat percentiles over
raw records.  The bias was ~4-10% for high-volume SOCs.

These tests assert:
1. Monotonicity: market_p10 ≤ market_p25 ≤ market_median ≤ market_p75 ≤ market_p90
2. Known-value regression: 15-1252 H-1B FY2025 market_median should be in the
   corrected range ($125 K–$145 K), NOT the biased range (>$141 K).
3. Structural integrity: required columns, positive salaries, non-empty output.
4. n_employers is non-negative.
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent.parent
ARTIFACTS_ROOT = ROOT / "artifacts" / "tables"

REQUIRED_COLS = {
    "soc_code", "visa_type", "fiscal_year",
    "total_filings", "n_employers",
    "market_mean", "market_median",
    "market_p10", "market_p25", "market_p75", "market_p90",
}
PCT_ORDERED = [("market_p10", "market_p25"),
               ("market_p25", "market_median"),
               ("market_median", "market_p75"),
               ("market_p75", "market_p90")]


@pytest.fixture(scope="module")
def ssm() -> pd.DataFrame:
    path = ARTIFACTS_ROOT / "soc_salary_market.parquet"
    if not path.exists():
        pytest.skip(f"soc_salary_market.parquet not found at {path}")
    return pd.read_parquet(path)


# ── structural tests ─────────────────────────────────────────────────────────

def test_required_columns_present(ssm):
    missing = REQUIRED_COLS - set(ssm.columns)
    assert not missing, f"Missing columns in soc_salary_market: {missing}"


def test_non_empty(ssm):
    assert len(ssm) >= 10_000, f"Expected ≥10,000 rows, got {len(ssm):,}"


def test_no_negative_market_median(ssm):
    neg = (ssm["market_median"] < 0).sum()
    assert neg == 0, f"{neg} rows have negative market_median"


def test_n_employers_non_negative(ssm):
    neg = (ssm["n_employers"] < 0).sum()
    assert neg == 0, f"{neg} rows have negative n_employers"


def test_total_filings_positive(ssm):
    zero_or_neg = (ssm["total_filings"] <= 0).sum()
    assert zero_or_neg == 0, f"{zero_or_neg} rows have total_filings ≤ 0"


# ── monotonicity tests ───────────────────────────────────────────────────────

def test_percentile_monotonicity_zero_violations(ssm):
    """p10 ≤ p25 ≤ median ≤ p75 ≤ p90 must hold for all complete rows."""
    complete = ssm.dropna(subset=[col for pair in PCT_ORDERED for col in pair])
    total_violations = 0
    details = []
    for lo, hi in PCT_ORDERED:
        count = int((complete[lo] > complete[hi]).sum())
        if count > 0:
            total_violations += count
            details.append(f"  {lo} > {hi}: {count} rows")
    assert total_violations == 0, (
        f"Monotonicity violations in soc_salary_market ({total_violations} total):\n"
        + "\n".join(details)
    )


# ── regression: true flat median (anti-bias guard) ───────────────────────────

def test_15_1252_h1b_fy2025_market_median_corrected_range(ssm):
    """15-1252 (Software Developers) H-1B FY2025 market_median must be in the
    corrected range $125,000–$145,000.  Before the fix it was ~$141,648 (biased
    upward by ~4.8%).  The true flat median over raw LCA records is ~$135,100–
    $136,000 (depending on exact filter/window).
    """
    row = ssm[
        (ssm["soc_code"] == "15-1252") &
        (ssm["fiscal_year"] == 2025) &
        (ssm["visa_type"] == "H-1B")
    ]
    if len(row) == 0:
        pytest.skip("15-1252 H-1B FY2025 row not found — may not be in dataset")
    med = float(row.iloc[0]["market_median"])
    assert 125_000 <= med <= 145_000, (
        f"15-1252 H-1B FY2025 market_median={med:,.0f} is outside expected corrected "
        f"range [$125K, $145K].  A value above $141,648 may indicate the "
        f"median-of-medians bias has been re-introduced."
    )


def test_15_1252_h1b_fy2025_not_biased(ssm):
    """After the fix, 15-1252 H-1B FY2025 market_median must NOT exceed $141,500.
    The biased value was $141,648; the corrected value is ~$136,000.
    """
    row = ssm[
        (ssm["soc_code"] == "15-1252") &
        (ssm["fiscal_year"] == 2025) &
        (ssm["visa_type"] == "H-1B")
    ]
    if len(row) == 0:
        pytest.skip("15-1252 H-1B FY2025 row not found")
    med = float(row.iloc[0]["market_median"])
    assert med <= 141_500, (
        f"15-1252 H-1B FY2025 market_median={med:,.0f} exceeds $141,500 "
        f"— the median-of-medians bias may have been re-introduced."
    )


def test_h1b_high_volume_socs_reasonable_medians(ssm):
    """Top 10 H-1B SOCs by FY2025 filing volume should have market_median
    between $60,000 and $300,000 — a sanity range for professional visas.
    """
    fy25_h1b = ssm[(ssm["fiscal_year"] == 2025) & (ssm["visa_type"] == "H-1B")]
    if len(fy25_h1b) == 0:
        pytest.skip("No FY2025 H-1B rows in soc_salary_market")
    top10 = fy25_h1b.nlargest(10, "total_filings")
    out_of_range = top10[
        (top10["market_median"] < 60_000) | (top10["market_median"] > 300_000)
    ]
    assert len(out_of_range) == 0, (
        f"Top-10 SOCs with out-of-range market_median:\n"
        f"{out_of_range[['soc_code', 'total_filings', 'market_median']].to_string()}"
    )


# ── integration: monotonicity on the actual built parquet ────────────────────

def test_actual_parquet_zero_monotonicity_violations():
    """Integration: load the built parquet and check for 0 violations.
    Independent of the fixture to match the style of test_salary_benchmarks.py.
    """
    pq = ARTIFACTS_ROOT / "soc_salary_market.parquet"
    if not pq.exists():
        pytest.skip(f"Parquet not found: {pq}")
    df = pd.read_parquet(pq)
    complete = df.dropna(subset=[col for pair in PCT_ORDERED for col in pair])
    violations = sum(
        int((complete[lo] > complete[hi]).sum()) for lo, hi in PCT_ORDERED
    )
    assert violations == 0, f"{violations} monotonicity violations in {pq.name}"
