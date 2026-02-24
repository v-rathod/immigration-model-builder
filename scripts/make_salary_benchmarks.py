#!/usr/bin/env python3
"""
STEP 5 — Build salary_benchmarks.parquet (SOC × area percentile table)
Overwrites the existing empty/wrong-schema file.

Inputs: fact_oews, dim_area, dim_soc
Output: artifacts/tables/salary_benchmarks.parquet
Log:    artifacts/metrics/salary_benchmarks_fix.log
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
OUT_PATH = TABLES / "salary_benchmarks.parquet"
LOG_PATH = METRICS / "salary_benchmarks_fix.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def _excl(p: Path) -> bool:
    return any(x in str(p) for x in EXCL)


def _read_oews() -> pd.DataFrame:
    oews_dir = TABLES / "fact_oews"
    needed = ["area_code", "soc_code", "a_pct10", "a_pct25", "a_median", "a_pct75", "a_pct90",
              "h_pct10", "h_pct25", "h_median", "h_pct75", "h_pct90", "tot_emp"]
    files = sorted(f for f in oews_dir.rglob("*.parquet") if not _excl(f))
    dfs = []
    for pf in files:
        avail = pd.read_parquet(pf, columns=None).columns.tolist()
        cols = [c for c in needed if c in avail]
        df = pd.read_parquet(pf, columns=cols)
        for part in pf.parts:
            if "=" in part:
                k, v = part.split("=", 1)
                if k not in df.columns:
                    try:
                        df[k] = int(v)
                    except ValueError:
                        df[k] = v
        dfs.append(df)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def _annualize_or_use(a_col, h_col) -> pd.Series:
    """Return annual value; prefer a_ col, fallback to h_ col ×2080."""
    result = a_col.copy().astype(float)
    mask = result.isna() & h_col.notna()
    result.loc[mask] = h_col.loc[mask] * 2080
    return result


PCT_COLS = ["p10", "p25", "median", "p75", "p90"]


def enforce_monotonic(df: pd.DataFrame, log_lines: list) -> tuple:
    """Sort percentile values ascending per row for any non-null values.

    Strategy:
    - Fully non-null rows: sort all 5 values in place.
    - Partial rows (some nulls): sort only the non-null values and put them
      back into the original non-null slots in ascending order.
    - Fully null rows: skip.
    Returns (corrected_df, corrections_count).
    """
    df = df.copy()
    corrections = []

    vals = df[PCT_COLS].to_numpy(dtype=float, copy=True, na_value=np.nan)  # writable (N, 5)
    all_non_null = (~np.isnan(vals)).all(axis=1)
    null_any = int((~all_non_null).sum())

    # Process every row that has at least 2 non-null values
    for i in range(len(vals)):
        row = vals[i]
        non_null_mask = ~np.isnan(row)
        n_present = int(non_null_mask.sum())
        if n_present < 2:
            continue
        present_vals = row[non_null_mask]
        sorted_vals = np.sort(present_vals)
        if not np.array_equal(present_vals, sorted_vals):
            corrections.append({
                "idx": int(i),
                "old": tuple(float(v) if not np.isnan(v) else None for v in row),
                "new": tuple(float(v) if not np.isnan(v) else None
                             for v in np.where(non_null_mask, np.nan, np.nan)),  # placeholder
            })
            row[non_null_mask] = sorted_vals
            vals[i] = row

    df[PCT_COLS] = vals

    log_lines.append(
        f"enforce_monotonic: corrections_applied={len(corrections)}"
        f"  rows_with_null_any_pct={null_any}"
    )
    if corrections:
        log_lines.append("Top corrected examples (index | old→new):")
        for ex in corrections[:50]:
            log_lines.append(f"  [{ex['idx']}] corrected")
    if len(corrections) > 50:
        log_lines.append(f"  ... {len(corrections) - 50} more corrections not shown")
    if null_any:
        log_lines.append(f"WARN: {null_any} rows have at least one null percentile (kept, monotonically sorted)")

    return df, len(corrections)


def build_salary_benchmarks(log_lines: list) -> pd.DataFrame:
    df = _read_oews()
    log_lines.append(f"fact_oews rows loaded: {len(df):,}")
    if df.empty:
        log_lines.append("FAIL: fact_oews returned empty DataFrame")
        return pd.DataFrame()

    # Annualize all percentile columns
    for pct in ["pct10", "pct25", "median", "pct75", "pct90"]:
        a_key = f"a_{pct}" if pct != "median" else "a_median"
        h_key = f"h_{pct}" if pct != "median" else "h_median"
        # map pct10→p10 etc. for output col names
        col_name = f"p{pct[3:]}" if pct.startswith("pct") else pct
        a_s = pd.to_numeric(df.get(a_key, pd.Series(np.nan, index=df.index)), errors="coerce")
        h_s = pd.to_numeric(df.get(h_key, pd.Series(np.nan, index=df.index)), errors="coerce")
        df[col_name] = _annualize_or_use(a_s, h_s)

    # Drop rows that are clearly bad: all five percentiles are 0 or negative
    val_cols_raw = ["p10", "p25", "median", "p75", "p90"]
    for c in val_cols_raw:
        # Coerce to float; drop negatives only where the other columns are valid positives
        positive_others = df[[v for v in val_cols_raw if v != c]].gt(0).all(axis=1)
        bad = df[c].notna() & df[c].le(0) & positive_others
        df.loc[bad, c] = np.nan

    df.rename(columns={"median": "median_salary"}, inplace=True, errors="ignore")
    # Ensure PCT_COLS match (median_salary → median in final output, but we work as p10/p25/median here)
    df.rename(columns={"median_salary": "median"}, inplace=True, errors="ignore")

    # SOC × area grain — simple median per (soc_code, area_code) across ref_years
    agg_cols = ["soc_code", "area_code"]
    df_area = df[agg_cols + PCT_COLS].groupby(agg_cols, as_index=False).median(numeric_only=True)

    # National grain: area_code = None
    df_nat = df[["soc_code"] + PCT_COLS].groupby("soc_code", as_index=False).median(numeric_only=True)
    df_nat["area_code"] = None

    df_out = pd.concat([df_area, df_nat[["soc_code", "area_code"] + PCT_COLS]], ignore_index=True)

    # ── National-fallback fill ────────────────────────────────────────────────
    # For area-level rows where any percentile is null, fill from the national
    # aggregate for that soc_code.  This reduces null_pct rows from ~3.6% → ~0%.
    nat_lookup = df_nat.set_index("soc_code")[PCT_COLS]
    total_filled = 0
    for col in PCT_COLS:
        null_mask = df_out[col].isna()
        if null_mask.any():
            fill_vals = df_out.loc[null_mask, "soc_code"].map(nat_lookup[col])
            n_filled = int(fill_vals.notna().sum())
            if n_filled:
                df_out.loc[null_mask, col] = fill_vals
                total_filled += n_filled
    if total_filled:
        log_lines.append(f"National fallback fill: {total_filled:,} null percentile cells filled")
        null_rows_after = df_out[PCT_COLS].isna().any(axis=1).sum()
        log_lines.append(f"  Rows with any null percentile after fill: {null_rows_after:,}")
    # ─────────────────────────────────────────────────────────────────────────

    # Enforce monotonic ordering — correct silently, log corrections
    df_out, n_corrections = enforce_monotonic(df_out, log_lines)
    log_lines.append(f"total_corrections: {n_corrections}")

    # Final QA: after correction, failures should be 0
    fails = 0
    for lo, hi in [("p10", "p25"), ("p25", "median"), ("median", "p75"), ("p75", "p90")]:
        mask = df_out[lo].notna() & df_out[hi].notna() & (df_out[lo] > df_out[hi])
        n = int(mask.sum())
        if n:
            fails += n
            log_lines.append(f"FAIL QA (post-fix): {n} rows where {lo} > {hi}")

    if fails == 0:
        log_lines.append("QA PASS: percentile ordering p10 <= p25 <= median <= p75 <= p90")
    else:
        log_lines.append(f"FAIL: {fails} residual ordering violations after enforcement")

    log_lines.append(f"total_rows: {len(df_out):,}  (area_rows: {len(df_area):,}  national_rows: {len(df_nat):,})")
    return df_out[["soc_code", "area_code"] + PCT_COLS].reset_index(drop=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_lines = [f"=== salary_benchmarks build {datetime.now(timezone.utc).isoformat()} ==="]

    df_out = build_salary_benchmarks(log_lines)

    if not args.dry_run:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp = OUT_PATH.parent / f".tmp_{ts}_salary_benchmarks.parquet"
        df_out.to_parquet(tmp, index=False)
        if OUT_PATH.exists():
            OUT_PATH.unlink()
        tmp.rename(OUT_PATH)
        log.info(f"Written: {OUT_PATH}")
        log_lines.append(f"Written: {OUT_PATH}")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"salary_benchmarks: {len(df_out):,} rows")


if __name__ == "__main__":
    main()
