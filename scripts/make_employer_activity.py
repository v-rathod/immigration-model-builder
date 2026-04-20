#!/usr/bin/env python3
"""
Build employer_activity.parquet — employer filing activity classification.

Reads employer_salary_yearly.parquet (the H-1B + PERM yearly summary produced
by make_employer_salary_profiles.py) and classifies each employer into one of
three activity tiers based on their most recent filing year:

  active     — filed H-1B or PERM within the last 3 years
  legacy     — last filing 4–8 years ago (valid historical data, visually demoted)
  historical — no filing in 9+ years

Output grain: one row per employer_id.
Columns: employer_id, employer_name, latest_h1b_year, latest_perm_year,
         latest_any_year, activity_status

Cross-used by:
  - P3 _regen_search.py (enriches the search index with activity_status)
  - P3 smart-sort.ts (applies sort penalty for legacy/historical employers)

Usage: python scripts/make_employer_activity.py
"""
from __future__ import annotations

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
OUT_PATH = TABLES / "employer_activity.parquet"
LOG_PATH = METRICS / "employer_activity.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# ── Activity classification thresholds ──────────────────────────────────────
# These are defined as offsets from the current year so the script stays
# correct when re-run in future years without code changes.
CURRENT_YEAR = datetime.now().year          # e.g. 2026
ACTIVE_CUTOFF = CURRENT_YEAR - 3           # >= 2023 → active
LEGACY_CUTOFF = CURRENT_YEAR - 8           # >= 2018 → legacy (4–8 years ago)
# Anything older than LEGACY_CUTOFF → historical


def classify_activity(latest_year: int) -> str:
    """Classify employer activity based on their most recent filing year."""
    if latest_year >= ACTIVE_CUTOFF:
        return "active"
    elif latest_year >= LEGACY_CUTOFF:
        return "legacy"
    else:
        return "historical"


def main() -> None:
    log_lines: list[str] = []

    def _log(msg: str) -> None:
        log.info(msg)
        log_lines.append(msg)

    _log("=" * 70)
    _log("EMPLOYER ACTIVITY CLASSIFICATION")
    _log("=" * 70)
    _log(f"Current year:       {CURRENT_YEAR}")
    _log(f"Active cutoff:      >= {ACTIVE_CUTOFF}")
    _log(f"Legacy cutoff:      >= {LEGACY_CUTOFF}")
    _log(f"Historical:         <  {LEGACY_CUTOFF}")

    # ── Load employer_salary_yearly ─────────────────────────────────────────
    esy_path = TABLES / "employer_salary_yearly.parquet"
    if not esy_path.exists():
        _log(f"\nERROR: {esy_path} not found. Run make_employer_salary_profiles.py first.")
        sys.exit(1)

    esy = pd.read_parquet(esy_path)
    _log(f"\nLoaded employer_salary_yearly: {len(esy):,} rows")
    _log(f"  Visa types: {sorted(esy['visa_type'].unique().tolist())}")
    _log(f"  Fiscal year range: {esy['fiscal_year'].min()} – {esy['fiscal_year'].max()}")

    # ── Compute latest year per employer per visa type ──────────────────────
    h1b = esy[esy["visa_type"] == "H-1B"]
    perm = esy[esy["visa_type"] == "PERM"]

    latest_h1b = (
        h1b.groupby("employer_id")["fiscal_year"]
        .max()
        .reset_index()
        .rename(columns={"fiscal_year": "latest_h1b_year"})
    )

    latest_perm = (
        perm.groupby("employer_id")["fiscal_year"]
        .max()
        .reset_index()
        .rename(columns={"fiscal_year": "latest_perm_year"})
    )

    # Get canonical employer_name (use the most recent name for each employer_id)
    names = (
        esy.sort_values("fiscal_year", ascending=False)
        .drop_duplicates("employer_id", keep="first")[["employer_id", "employer_name"]]
    )

    # ── Merge H-1B + PERM latest years ─────────────────────────────────────
    activity = names.merge(latest_h1b, on="employer_id", how="left")
    activity = activity.merge(latest_perm, on="employer_id", how="left")

    # Fill NaN years with 0 (employer had no filings of that type)
    activity["latest_h1b_year"] = activity["latest_h1b_year"].fillna(0).astype(int)
    activity["latest_perm_year"] = activity["latest_perm_year"].fillna(0).astype(int)

    # Combined latest year = max of H-1B and PERM
    activity["latest_any_year"] = activity[["latest_h1b_year", "latest_perm_year"]].max(axis=1)

    # ── Classify ────────────────────────────────────────────────────────────
    activity["activity_status"] = activity["latest_any_year"].apply(classify_activity)

    # ── Summary stats ───────────────────────────────────────────────────────
    status_counts = activity["activity_status"].value_counts()
    _log(f"\nClassification results ({len(activity):,} unique employers):")
    for status in ["active", "legacy", "historical"]:
        count = status_counts.get(status, 0)
        pct = 100 * count / len(activity) if len(activity) > 0 else 0
        _log(f"  {status:12s}: {count:>7,}  ({pct:5.1f}%)")

    # ── Spot-check known employers ──────────────────────────────────────────
    spot_checks = [
        "Capgemini America", "Capgemini Financial Services Usa", "Capgemini US",
        "Google", "Microsoft", "Syntel", "Satyam Computer Services", "Yahoo!",
    ]
    _log("\nSpot-checks:")
    for name in spot_checks:
        row = activity[activity["employer_name"] == name]
        if not row.empty:
            r = row.iloc[0]
            _log(f"  {name:50s} h1b={int(r['latest_h1b_year'])} perm={int(r['latest_perm_year'])} "
                 f"any={int(r['latest_any_year'])} → {r['activity_status']}")
        else:
            _log(f"  {name:50s} (not found)")

    # ── Write output ────────────────────────────────────────────────────────
    output_cols = ["employer_id", "employer_name", "latest_h1b_year", "latest_perm_year",
                   "latest_any_year", "activity_status"]
    activity[output_cols].to_parquet(OUT_PATH, index=False, engine="pyarrow")
    _log(f"\nWritten: {OUT_PATH.name} ({len(activity):,} rows)")

    # ── Write log ───────────────────────────────────────────────────────────
    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    _log(f"Log: {LOG_PATH.name}")


if __name__ == "__main__":
    main()
