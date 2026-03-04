#!/usr/bin/env python3
"""
STEP 8 — Build backlog_estimates.parquet
Heuristic backlog calculation per (category, country, bulletin_month).

Inputs: fact_cutoff_trends.parquet (or fact_cutoffs_all), fact_perm
Output: artifacts/tables/backlog_estimates.parquet
Log:    artifacts/metrics/backlog_estimates.log
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
EXCL = ("_backup", "_quarantine", ".tmp_")
OUT_PATH = TABLES / "backlog_estimates.parquet"
LOG_PATH = METRICS / "backlog_estimates.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

APPROVED_STATUS = {"CERTIFIED", "CERTIFIED-EXPIRED"}


def _excl(p: Path) -> bool:
    return any(x in str(p) for x in EXCL)


def _read_partitioned_cols(dir_path: Path, cols: list) -> pd.DataFrame:
    files = sorted(f for f in dir_path.rglob("*.parquet") if not _excl(f))
    dfs = []
    for pf in files:
        try:
            avail = pd.read_parquet(pf, columns=None).columns.tolist()
            df = pd.read_parquet(pf, columns=[c for c in cols if c in avail])
            for part in pf.parts:
                if "=" in part:
                    k, v = part.split("=", 1)
                    if k not in df.columns and k in cols:
                        try:
                            df[k] = int(v)
                        except ValueError:
                            df[k] = v
            dfs.append(df)
        except Exception as e:
            log.warning(f"Skipping {pf}: {e}")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def build_backlog_estimates(log_lines: list) -> pd.DataFrame:
    # Load cutoff trends
    trends_path = TABLES / "fact_cutoff_trends.parquet"
    if trends_path.exists():
        df_trends = pd.read_parquet(trends_path)
        log_lines.append(f"Loaded fact_cutoff_trends: {len(df_trends):,} rows")
    else:
        log_lines.append("WARN: fact_cutoff_trends.parquet not found — attempting fallback from fact_cutoffs_all")
        all_path = TABLES / "fact_cutoffs_all.parquet"
        if not all_path.exists():
            log_lines.append("FAIL: no cutoff source available")
            return pd.DataFrame()
        df_base = pd.read_parquet(all_path)
        df_base["cutoff_date"] = pd.to_datetime(df_base["cutoff_date"], errors="coerce")
        EPOCH = pd.Timestamp("1970-01-01")
        df_base = df_base.sort_values(["chart", "category", "country", "bulletin_year", "bulletin_month"])
        df_base["queue_position_days"] = np.where(
            df_base["status_flag"] == "D",
            (df_base["cutoff_date"] - EPOCH).dt.days,
            np.nan,
        )
        series_keys = ["chart", "category", "country"]
        df_base["monthly_advancement_days"] = (
            df_base.groupby(series_keys)["queue_position_days"].diff()
        )
        df_base["retrogression_flag"] = (
            df_base["monthly_advancement_days"].lt(0).astype("Int8")
        )
        df_trends = df_base
        log_lines.append(f"Inline fallback built: {len(df_trends):,} rows")

    # Load fact_perm for inflow proxy (last 12m filings globally)
    perm_dir = TABLES / "fact_perm"
    df_perm = _read_partitioned_cols(perm_dir, ["case_number", "case_status", "decision_date"])
    if not df_perm.empty:
        df_perm["decision_date"] = pd.to_datetime(df_perm["decision_date"], errors="coerce")
        anchor = df_perm["decision_date"].max()
        cutoff_12m = anchor - pd.DateOffset(months=12)
        df_perm_12m = df_perm[df_perm["decision_date"] >= cutoff_12m]
        total_filings_12m = len(df_perm_12m)
        log_lines.append(f"PERM 12m filings (global proxy): {total_filings_12m:,}")
    else:
        total_filings_12m = None
        log_lines.append("WARN: fact_perm not available; inflow_estimate_12m will be null")

    # Compute trailing 12m advancement per series
    needed_cols = ["bulletin_year", "bulletin_month", "chart", "category", "country",
                   "queue_position_days", "monthly_advancement_days", "retrogression_flag"]
    available = [c for c in needed_cols if c in df_trends.columns]
    df = df_trends[available].copy()

    df = df.sort_values(["chart", "category", "country", "bulletin_year", "bulletin_month"])

    # Rolling 12m mean advancement per series.
    # Use transform to preserve all columns (groupby.apply drops group keys in pandas 2.x)
    group_keys = [c for c in ["chart", "category", "country"] if c in df.columns]
    if group_keys:
        df["advancement_days_12m_avg"] = df.groupby(group_keys)["monthly_advancement_days"].transform(
            lambda s: s.rolling(12, min_periods=3).mean()
        )
    else:
        df["advancement_days_12m_avg"] = (
            df["monthly_advancement_days"].rolling(12, min_periods=3).mean()
        )

    # ── Blended velocity (matches pd_forecast.py formula) ──────────────
    # 50% full-history net + 25% capped rolling-24m + 25% capped rolling-12m
    # This smooths out burst advancement spikes while reflecting long-term trends.
    def _compute_blended_backlog(group):
        group = group.sort_values(["bulletin_year", "bulletin_month"]).copy()
        adv = group["monthly_advancement_days"].fillna(0.0).values.astype(float)
        qpos = group["queue_position_days"].values if "queue_position_days" in group.columns else None
        blended = np.full(len(group), np.nan)

        if qpos is not None:
            first_valid_idx = None
            first_qpos = None
            for i in range(len(qpos)):
                if not pd.isna(qpos[i]):
                    if first_valid_idx is None:
                        first_valid_idx = i
                        first_qpos = qpos[i]
                    months = i - first_valid_idx + 1
                    net_vel = max((qpos[i] - first_qpos) / months, 0.0) if months > 0 else 0.0
                    r12 = float(np.mean(adv[max(0, i - 11):i + 1]))
                    r24 = float(np.mean(adv[max(0, i - 23):i + 1]))
                    vel_cap = max(net_vel * 1.25, net_vel + 5.0)
                    c12 = min(max(r12, 0.0), vel_cap)
                    c24 = min(max(r24, 0.0), vel_cap)
                    blended[i] = max(0.50 * net_vel + 0.25 * c24 + 0.25 * c12, 0.0)

        group["blended_velocity"] = blended
        return group

    if group_keys:
        parts = []
        for _keys, grp in df.groupby(group_keys, observed=True):
            parts.append(_compute_blended_backlog(grp))
        df = pd.concat(parts, ignore_index=True)
    else:
        df = _compute_blended_backlog(df)

    # Backlog estimate: use blended_velocity for clearing estimate
    # (falls back to advancement_days_12m_avg if blended is NaN)
    effective_vel = df["blended_velocity"].fillna(df["advancement_days_12m_avg"])
    adv_days_monthly = effective_vel / 30.0
    df["backlog_months_to_clear_est"] = np.where(
        adv_days_monthly.gt(0),
        df["queue_position_days"] / adv_days_monthly,
        np.nan,
    )
    # Cap at 600 months (50 years) — sentinel 999 masked all real outliers
    df["backlog_months_to_clear_est"] = df["backlog_months_to_clear_est"].clip(lower=0, upper=600)

    # Inflow estimate (global PERM proxy, same for all rows)
    df["inflow_estimate_12m"] = total_filings_12m

    # Final columns
    out_cols = [
        "bulletin_year", "bulletin_month", "chart", "category", "country",
        "inflow_estimate_12m", "advancement_days_12m_avg", "blended_velocity",
        "backlog_months_to_clear_est"
    ]
    df_out = df[[c for c in out_cols if c in df.columns]].reset_index(drop=True)

    # QA
    neg = df_out["backlog_months_to_clear_est"].lt(0).sum()
    if neg:
        log_lines.append(f"WARN: {neg} rows with negative backlog estimate (retrogressions)")

    log_lines.append(f"Output rows: {len(df_out):,}")
    return df_out


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_lines = [f"=== backlog_estimates build {datetime.now(timezone.utc).isoformat()} ==="]
    df_out = build_backlog_estimates(log_lines)

    if not args.dry_run:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp = OUT_PATH.parent / f".tmp_{ts}_backlog_estimates.parquet"
        df_out.to_parquet(tmp, index=False)
        if OUT_PATH.exists():
            OUT_PATH.unlink()
        tmp.rename(OUT_PATH)
        log.info(f"Written: {OUT_PATH}")
        log_lines.append(f"Written: {OUT_PATH}")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"backlog_estimates: {len(df_out):,} rows")


if __name__ == "__main__":
    main()
