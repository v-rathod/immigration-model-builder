#!/usr/bin/env python3
"""
STEP 3 — Build category_movement_metrics.parquet
EB category comparison: movement, volatility, predicted next movement.

Input:  artifacts/tables/fact_cutoff_trends.parquet (or fact_cutoffs_all fallback)
Output: artifacts/tables/category_movement_metrics.parquet
Log:    artifacts/metrics/category_movement_metrics.log
"""
import os, sys, logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
OUT_PATH = TABLES / "category_movement_metrics.parquet"
LOG_PATH = METRICS / "category_movement_metrics.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def load_trends() -> pd.DataFrame:
    trends_p = TABLES / "fact_cutoff_trends.parquet"
    if trends_p.exists():
        log.info(f"Loading fact_cutoff_trends: {trends_p}")
        return pd.read_parquet(trends_p)
    # Fallback: build inline from fact_cutoffs_all
    log.info("fact_cutoff_trends not found; loading from fact_cutoffs_all...")
    from make_fact_cutoff_trends import load_fact_cutoffs, build_trends
    return build_trends(load_fact_cutoffs())


def _rolling_trailing(df: pd.DataFrame, series_keys: list, col: str, w: int, func: str) -> pd.Series:
    def _apply(g):
        return getattr(g.rolling(w, min_periods=1), func)()
    return df.groupby(series_keys, observed=True)[col].transform(_apply)


def build_category_metrics(df: pd.DataFrame, log_lines: list) -> pd.DataFrame:
    df = df.copy()
    df["_sort"] = df["bulletin_year"].astype(int) * 100 + df["bulletin_month"].astype(int)
    df.sort_values(["chart", "category", "country", "_sort"], inplace=True)

    series_keys = ["chart", "category", "country"]

    adv_col = "monthly_advancement_days"
    retro_col = "retrogression_flag"

    # Trailing 12m windows from trends
    df["avg_monthly_advancement_days"] = _rolling_trailing(df, series_keys, adv_col, 12, "mean")
    df["volatility_score"] = _rolling_trailing(df, series_keys, adv_col, 12, "std")
    df["retrogression_events_12m"] = _rolling_trailing(df, series_keys, retro_col, 12, "sum")

    # Median is computed separately using expanding because pandas rolling doesn't have direct median
    def _rolling_median(g):
        return g.rolling(12, min_periods=1).median()

    df["median_advancement_days"] = (
        df.groupby(series_keys, observed=True)[adv_col]
        .transform(_rolling_median)
    )

    # next_movement_prediction
    vel3 = "velocity_3m" if "velocity_3m" in df.columns else adv_col
    def _predict(row):
        v = row.get(vel3, None)
        r = row.get("retrogression_events_12m", None)
        if pd.isna(v):
            return "Unknown"
        if v > 0 and (pd.isna(r) or r == 0):
            return "Forward"
        if v < 0:
            return "Backward"
        return "Flat"

    df["next_movement_prediction"] = df.apply(_predict, axis=1)

    out_cols = [
        "bulletin_year", "bulletin_month", "chart", "category", "country",
        "avg_monthly_advancement_days", "median_advancement_days",
        "volatility_score", "retrogression_events_12m", "next_movement_prediction",
    ]
    existing = [c for c in out_cols if c in df.columns]
    result = df[existing].drop_duplicates(["bulletin_year", "bulletin_month", "chart", "category", "country"]).reset_index(drop=True)
    log_lines.append(f"Output rows: {len(result):,}")
    return result


def qa_checks(df: pd.DataFrame, log_lines: list) -> bool:
    ok = True
    pk = ["bulletin_year", "bulletin_month", "chart", "category", "country"]
    dupes = df.duplicated(subset=[c for c in pk if c in df.columns]).sum()
    if dupes:
        log_lines.append(f"WARN: {dupes} duplicate PK rows in category_movement_metrics")
    if len(df) < 100:
        log_lines.append(f"FAIL: category_movement_metrics only {len(df)} rows")
        ok = False
    return ok


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_lines = [f"=== category_movement_metrics build {datetime.now(timezone.utc).isoformat()} ==="]

    df_trends = load_trends()
    log_lines.append(f"Input rows: {len(df_trends):,}")

    df_out = build_category_metrics(df_trends, log_lines)
    log.info(f"Output rows: {len(df_out):,}")

    ok = qa_checks(df_out, log_lines)
    if not ok:
        sys.exit(1)

    if not args.dry_run:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp = OUT_PATH.parent / f".tmp_{ts}_category_movement_metrics.parquet"
        df_out.to_parquet(tmp, index=False)
        if OUT_PATH.exists():
            OUT_PATH.unlink()
        tmp.rename(OUT_PATH)
        log_lines.append(f"Written: {OUT_PATH}")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"category_movement_metrics: {len(df_out):,} rows")


if __name__ == "__main__":
    main()
