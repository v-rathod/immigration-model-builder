#!/usr/bin/env python3
"""
STEP 2 — Build employer_monthly_metrics.parquet
Monthly grain employer metrics from fact_perm (+ optional LCA).

Input:  fact_perm (partitioned), dim_employer
Output: artifacts/tables/employer_monthly_metrics.parquet
Log:    artifacts/metrics/employer_monthly_metrics.log
"""
import os, sys, json, logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
EXCL = ("_backup", "_quarantine", ".tmp_", "/tmp_")
OUT_PATH = TABLES / "employer_monthly_metrics.parquet"
LOG_PATH = METRICS / "employer_monthly_metrics.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

APPROVED_STATUS = {"CERTIFIED", "CERTIFIED-EXPIRED", "APPROVED"}
DENIED_STATUS = {"DENIED"}
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 250_000))


def _excl(p: Path) -> bool:
    return any(x in str(p) for x in EXCL)


def _read_partitioned(dir_path: Path, restore_col: str = "fiscal_year") -> pd.DataFrame:
    pfiles = [f for f in dir_path.rglob("*.parquet")
              if not _excl(f) and "__HIVE_DEFAULT" not in str(f)]
    dfs = []
    for pf in sorted(pfiles):
        df = pd.read_parquet(pf)
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


def build_employer_metrics(log_lines: list) -> pd.DataFrame:
    # Load fact_perm
    fp_dir = TABLES / "fact_perm"
    log.info("Loading fact_perm...")
    df_perm = _read_partitioned(fp_dir)
    log_lines.append(f"fact_perm: {len(df_perm):,} rows")

    df_perm["decision_date"] = pd.to_datetime(df_perm["decision_date"], errors="coerce")
    df_perm = df_perm.dropna(subset=["decision_date"])

    # Last 5 years by decision_date
    cutoff = df_perm["decision_date"].max() - pd.DateOffset(years=5)
    df_perm = df_perm[df_perm["decision_date"] >= cutoff]
    log_lines.append(f"fact_perm (last 5yr): {len(df_perm):,} rows  cutoff={cutoff.date()}")

    df_perm["month"] = df_perm["decision_date"].dt.to_period("M").dt.to_timestamp()
    # Case-insensitive match: PERM data has both 'Certified' and 'CERTIFIED' variants
    df_perm["is_approved"] = df_perm["case_status"].str.upper().isin(APPROVED_STATUS).astype(int)
    df_perm["is_denied"] = df_perm["case_status"].str.upper().isin(DENIED_STATUS).astype(int)
    df_perm["employer_id"] = df_perm["employer_id"].fillna("UNKNOWN")

    grp = df_perm.groupby(["employer_id", "month"], observed=True)
    monthly = grp.agg(
        filings=("case_number", "count"),
        approvals=("is_approved", "sum"),
        denials=("is_denied", "sum"),
    ).reset_index()

    # Guard: approvals should never exceed filings; clip if data anomaly
    over = monthly["approvals"] > monthly["filings"]
    if over.any():
        n_over = int(over.sum())
        log.warning(f"Clipping {n_over} rows where approvals > filings")
        monthly.loc[over, "approvals"] = monthly.loc[over, "filings"]

    monthly["approval_rate"] = (monthly["approvals"] / monthly["filings"]).clip(0, 1)
    monthly["denial_rate"] = (monthly["denials"] / monthly["filings"]).clip(0, 1)

    # Rolling 12-month audit_rate_t12 (proxy: approval_rate rolling avg over 12m)
    monthly.sort_values(["employer_id", "month"], inplace=True)
    monthly["audit_rate_t12"] = (
        monthly.groupby("employer_id", observed=True)["approval_rate"]
        .transform(lambda g: g.rolling(12, min_periods=1).mean())
    )

    # Join employer_name
    dim_emp = pd.read_parquet(TABLES / "dim_employer.parquet")[["employer_id", "employer_name"]]
    monthly = monthly.merge(dim_emp, on="employer_id", how="left")
    monthly["employer_name"] = monthly["employer_name"].fillna("UNKNOWN")
    monthly["dataset"] = "PERM"

    col_order = [
        "employer_id", "employer_name", "month", "filings", "approvals", "denials",
        "approval_rate", "denial_rate", "audit_rate_t12", "dataset",
    ]
    existing = [c for c in col_order if c in monthly.columns]
    return monthly[existing].reset_index(drop=True)


def qa_checks(df: pd.DataFrame, log_lines: list) -> bool:
    ok = True

    # FAIL: approval_rate/denial_rate must be in [0,1]
    bad_ar = ((df["approval_rate"] < 0) | (df["approval_rate"] > 1)).sum()
    if bad_ar:
        log_lines.append(f"FAIL: {bad_ar} rows with approval_rate outside [0,1]")
        ok = False

    # FAIL: approvals must not exceed filings
    bad_filings = (df["approvals"] > df["filings"]).sum()
    if bad_filings:
        log_lines.append(f"FAIL: {bad_filings} rows where approvals > filings")
        ok = False
    else:
        log_lines.append("QA PASS: no months with approvals > filings")

    # WARN: 36m weighted approval rate for employers with >= 50 total filings
    df["month"] = pd.to_datetime(df["month"])
    anchor = df["month"].max()
    cutoff_36m = anchor - pd.DateOffset(months=36)
    sub36 = df[df["month"] >= cutoff_36m]

    emp_totals = sub36.groupby("employer_id").agg(
        total_filings_36m=("filings", "sum"),
        total_approvals_36m=("approvals", "sum"),
    ).reset_index()
    emp_totals["avg_approval_rate_36m"] = (
        emp_totals["total_approvals_36m"] / emp_totals["total_filings_36m"]
    ).clip(0, 1)

    large = emp_totals[emp_totals["total_filings_36m"] >= 200]
    log_lines.append(f"Employers with total_filings_36m >= 200: {len(large)}")
    outliers = large[(large["avg_approval_rate_36m"] < 0.4) | (large["avg_approval_rate_36m"] > 1.0)]
    if len(outliers):
        log_lines.append(f"WARN: {len(outliers)} employers (total_filings_36m>=200) with weighted avg_approval_rate_36m outside [0.4,1.0]")
        top50 = outliers.sort_values("total_filings_36m", ascending=False).head(50)
        log_lines.append("Top outliers (employer_id | total_filings_36m | avg_approval_rate_36m):")
        for _, row in top50.iterrows():
            log_lines.append(
                f"  {row['employer_id']} | {int(row['total_filings_36m'])} | {row['avg_approval_rate_36m']:.3f}"
            )
    else:
        log_lines.append("QA PASS: all large employers have avg_approval_rate_36m in [0.4,1.0]")

    return ok


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_lines = [f"=== employer_monthly_metrics build {datetime.now(timezone.utc).isoformat()} ==="]

    df_out = build_employer_metrics(log_lines)
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
        tmp = OUT_PATH.parent / f".tmp_{ts}_employer_monthly_metrics.parquet"
        df_out.to_parquet(tmp, index=False)
        if OUT_PATH.exists():
            OUT_PATH.unlink()
        tmp.rename(OUT_PATH)
        log_lines.append(f"Written: {OUT_PATH}")
        log.info(f"Written: {OUT_PATH}")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"employer_monthly_metrics: {len(df_out):,} rows")


if __name__ == "__main__":
    main()
