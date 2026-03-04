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

    # Median — only over non-zero advancement months (zero = frozen cutoff, not useful)
    def _rolling_nonzero_median(g):
        result = []
        vals = g.values
        for i in range(len(vals)):
            start = max(0, i - 11)
            window = vals[start:i + 1]
            nonzero = [v for v in window if not pd.isna(v) and v != 0.0]
            if nonzero:
                result.append(float(np.median(nonzero)))
            else:
                result.append(np.nan)
        return pd.Series(result, index=g.index)

    df["median_advancement_days"] = (
        df.groupby(series_keys, observed=True)[adv_col]
        .transform(_rolling_nonzero_median)
    )

    # ── Blended velocity (matches pd_forecast.py formula) ──────────────
    # For each series at each bulletin month, compute:
    #   - full_history_net_vel: (last_cutoff - first_cutoff) / total_months
    #   - rolling_12m_mean: avg of last 12 months (NaN→0)
    #   - rolling_24m_mean: avg of last 24 months (NaN→0)
    #   - blended: 50% full_history + 25% capped_r24 + 25% capped_r12
    # This matches across artifacts (pd_forecasts, queue_depth_estimates).
    def _compute_blended(group):
        group = group.sort_values("_sort").copy()
        adv = group[adv_col].fillna(0.0).values.astype(float)

        # Expanding full-history net velocity using queue_position_days
        qpos = group["queue_position_days"].values if "queue_position_days" in group.columns else None
        blended = np.full(len(group), np.nan)
        net_vel_arr = np.full(len(group), np.nan)

        # Find first valid qpos
        if qpos is not None:
            first_valid_idx = None
            first_qpos = None
            for i in range(len(qpos)):
                if not pd.isna(qpos[i]):
                    if first_valid_idx is None:
                        first_valid_idx = i
                        first_qpos = qpos[i]
                    # Full-history net vel up to this point
                    months = i - first_valid_idx + 1
                    if months > 0:
                        net_vel = (qpos[i] - first_qpos) / months
                        net_vel = max(net_vel, 0.0)
                    else:
                        net_vel = 0.0
                    net_vel_arr[i] = net_vel

                    # Rolling 12m and 24m means (NaN filled to 0)
                    r12 = float(np.mean(adv[max(0, i - 11):i + 1]))
                    r24 = float(np.mean(adv[max(0, i - 23):i + 1]))

                    # Cap rolling means
                    vel_cap = max(net_vel * 1.25, net_vel + 5.0)
                    c12 = min(max(r12, 0.0), vel_cap)
                    c24 = min(max(r24, 0.0), vel_cap)

                    # Blended: 50% full-hist + 25% r24 + 25% r12
                    b = 0.50 * net_vel + 0.25 * c24 + 0.25 * c12
                    blended[i] = max(b, 0.0)

        group["blended_velocity"] = blended
        group["net_velocity"] = net_vel_arr
        return group

    # pandas 3.x drops group keys from apply; iterate manually
    parts = []
    for _keys, group in df.groupby(series_keys, observed=True):
        parts.append(_compute_blended(group))
    df = pd.concat(parts, ignore_index=True)

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
        "blended_velocity", "net_velocity",
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
