#!/usr/bin/env python3
"""
STEP 1 — Build fact_cutoff_trends.parquet
Trend-friendly VB series with advancement/retrogression analytics.

Input:  artifacts/tables/fact_cutoffs_all.parquet  (or two-level partition fallback)
Output: artifacts/tables/fact_cutoff_trends.parquet
Log:    artifacts/metrics/fact_cutoff_trends.log
"""
import os, sys, json, logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# ── Globals ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
EXCL = ("_backup", "_quarantine", ".tmp_", "/tmp_")
OUT_PATH = TABLES / "fact_cutoff_trends.parquet"
LOG_PATH = METRICS / "fact_cutoff_trends.log"

EPOCH = pd.Timestamp("1970-01-01")
PK_COLS = ["bulletin_year", "bulletin_month", "category", "country", "chart"]

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def _excl(p: Path) -> bool:
    return any(x in str(p) for x in EXCL)


def load_fact_cutoffs() -> pd.DataFrame:
    pres = TABLES / "fact_cutoffs_all.parquet"
    if pres.exists():
        log.info(f"Loading presentation table: {pres}")
        return pd.read_parquet(pres)
    # fallback: union partitions
    log.info("Presentation table not found — unioning partitions...")
    fc_dir = TABLES / "fact_cutoffs"
    leaves = [f for f in fc_dir.rglob("*.parquet") if not _excl(f)]
    dfs = []
    for leaf in sorted(leaves):
        df = pd.read_parquet(leaf)
        for part in leaf.parts:
            if "=" in part:
                k, v = part.split("=", 1)
                if k not in df.columns:
                    df[k] = int(v)
        dfs.append(df)
    result = pd.concat(dfs, ignore_index=True)
    log.info(f"Union: {len(result):,} rows from {len(leaves)} leaves")
    return result


def build_trends(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["cutoff_date"] = pd.to_datetime(df["cutoff_date"], errors="coerce")
    df["bulletin_year"] = df["bulletin_year"].astype(int)
    df["bulletin_month"] = df["bulletin_month"].astype(int)

    # sort_key for time ordering
    df["_sort"] = df["bulletin_year"] * 100 + df["bulletin_month"]

    # queue_position_days: only for status "D" (specific date cutoffs)
    df["queue_position_days"] = np.where(
        df["status_flag"].isin(["D"]) & df["cutoff_date"].notna(),
        (df["cutoff_date"] - EPOCH).dt.days,
        np.nan,
    )

    # chart column — use if present, else fill from category
    if "chart" not in df.columns:
        df["chart"] = "FAD"

    series_keys = ["chart", "category", "country"]
    df.sort_values(series_keys + ["_sort"], inplace=True)

    # LAG: monthly_advancement_days
    df["monthly_advancement_days"] = (
        df.groupby(series_keys)["queue_position_days"]
        .diff()
    )

    # velocity windows
    def rolling_mean(g: pd.Series, w: int) -> pd.Series:
        return g.rolling(window=w, min_periods=w).mean()

    df["velocity_3m"] = (
        df.groupby(series_keys)["monthly_advancement_days"]
        .transform(lambda g: rolling_mean(g, 3))
    )
    df["velocity_6m"] = (
        df.groupby(series_keys)["monthly_advancement_days"]
        .transform(lambda g: rolling_mean(g, 6))
    )

    # retrogression (negative advancement = moving backward)
    df["retrogression_flag"] = (
        df["monthly_advancement_days"].notna() & (df["monthly_advancement_days"] < 0)
    ).astype("Int8")

    # cumulative retrogression count per series
    df["retrogression_count_cum"] = (
        df.groupby(series_keys)["retrogression_flag"]
        .transform("cumsum")
    )

    df.drop(columns=["_sort"], inplace=True)

    # ── PK5 uniqueness assertion (no chart-collapse — preserve both DFF and FAD) ──
    # fact_cutoffs_all.parquet is PK5-unique on (yr, mo, cat, country, chart).
    # Retaining both chart types gives 8,315 output rows (target).
    PK5 = ["bulletin_year", "bulletin_month", "category", "country", "chart"]
    pk5_present = [c for c in PK5 if c in df.columns]
    assert df.duplicated(subset=pk5_present).sum() == 0, "PK5 dups remain in fact_cutoff_trends"

    keep_cols = [
        "bulletin_year", "bulletin_month", "chart", "category", "country",
        "status_flag", "cutoff_date", "queue_position_days",
        "monthly_advancement_days", "velocity_3m", "velocity_6m",
        "retrogression_flag", "retrogression_count_cum", "source_file",
    ]
    existing = [c for c in keep_cols if c in df.columns]
    return df[existing].reset_index(drop=True)


def qa_checks(df: pd.DataFrame, logs: list[str]) -> bool:
    ok = True
    # PK uniqueness – 5-col PK (chart dimension preserved)
    pk5 = ["bulletin_year", "bulletin_month", "category", "country", "chart"]
    pk_present = [c for c in pk5 if c in df.columns]
    dupes = df.duplicated(subset=pk_present).sum()
    if dupes > 0:
        logs.append(f"FAIL: {dupes} duplicate 5-col PK rows in fact_cutoff_trends")
        ok = False
    else:
        logs.append(f"OK: 0 duplicate 5-col PK rows ({len(pk_present)}-col key used)")
    # row count: fact_cutoffs_all has 8,315 PK5-unique rows; trends must match
    if len(df) < 8000 or len(df) > 8500:
        logs.append(f"FAIL: fact_cutoff_trends rows={len(df)} outside [8000,8500]")
        ok = False
    # advancement range
    adv = df["monthly_advancement_days"].dropna()
    if len(adv):
        med = adv.median()
        if not (-120 <= med <= 240):
            logs.append(f"WARN: median monthly_advancement_days={med:.1f} outside [-120,240]")
    return ok


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_lines = [f"=== fact_cutoff_trends build {datetime.now(timezone.utc).isoformat()} ==="]

    df_raw = load_fact_cutoffs()
    log_lines.append(f"Input rows: {len(df_raw):,}")
    log.info(f"Input rows: {len(df_raw):,}")

    df_out = build_trends(df_raw)
    log_lines.append(f"Output rows: {len(df_out):,}")
    log.info(f"Output rows: {len(df_out):,}")

    ok = qa_checks(df_out, log_lines)

    if not ok:
        for l in log_lines:
            if "FAIL" in l:
                print(l, file=sys.stderr)
        sys.exit(1)

    if not args.dry_run:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp = OUT_PATH.parent / f".tmp_{ts}_fact_cutoff_trends.parquet"
        df_out.to_parquet(tmp, index=False)
        if OUT_PATH.exists():
            OUT_PATH.unlink()
        tmp.rename(OUT_PATH)
        log_lines.append(f"Written: {OUT_PATH}")
        log.info(f"Written: {OUT_PATH}")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"fact_cutoff_trends: {len(df_out):,} rows")


if __name__ == "__main__":
    main()
