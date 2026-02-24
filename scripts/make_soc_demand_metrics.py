#!/usr/bin/env python3
"""
STEP 6 — Build soc_demand_metrics.parquet
SOC demand windows: 12m / 24m / 36m from max decision_date.

Inputs: fact_perm, fact_lca, dim_soc
Output: artifacts/tables/soc_demand_metrics.parquet
Log:    artifacts/metrics/soc_demand_metrics.log
"""
import json
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
OUT_PATH = TABLES / "soc_demand_metrics.parquet"
LOG_PATH = METRICS / "soc_demand_metrics.log"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

APPROVED_STATUS = {"CERTIFIED", "CERTIFIED-EXPIRED"}
WINDOWS_MONTHS = [12, 24, 36]

WAGE_MULTIPLIERS = {
    "Hour": 2080, "hr": 2080,
    "Bi-Weekly": 26, "bi-weekly": 26,
    "Week": 52, "weekly": 52,
    "Month": 12, "monthly": 12,
    "Year": 1, "annual": 1, "yearly": 1,
}


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


def annualize(wage: pd.Series, unit: pd.Series) -> pd.Series:
    mult = unit.map(WAGE_MULTIPLIERS).fillna(1.0)
    return wage * mult


def _top_employers_json(sub: pd.DataFrame, emp_col: str, n: int = 5) -> str:
    if emp_col not in sub.columns:
        return "[]"
    top = sub[emp_col].value_counts().head(n).to_dict()
    return json.dumps([{"employer_id": k, "filings": int(v)} for k, v in top.items()])


def build_soc_demand(log_lines: list) -> pd.DataFrame:
    records = []

    for dataset, fp_dir, wage_col, unit_col in [
        ("PERM", TABLES / "fact_perm", "wage_offer_from", "wage_offer_unit"),
        ("LCA",  TABLES / "fact_lca",  "wage_rate_from",  "wage_unit"),
    ]:
        if not fp_dir.exists():
            log_lines.append(f"WARN: {fp_dir} missing — skipping {dataset}")
            continue

        needed = ["case_number", "case_status", "employer_id", "soc_code",
                  "decision_date", wage_col, unit_col]
        log.info(f"Loading {dataset}...")
        df = _read_partitioned_cols(fp_dir, needed)
        if df.empty:
            log_lines.append(f"WARN: {dataset} returned empty")
            continue

        df["decision_date"] = pd.to_datetime(df["decision_date"], errors="coerce")
        df = df.dropna(subset=["decision_date"])
        df["is_approved"] = df["case_status"].str.upper().isin(APPROVED_STATUS).astype(int)

        df[wage_col] = pd.to_numeric(df.get(wage_col, pd.Series(np.nan, index=df.index)), errors="coerce")
        unit_s = df.get(unit_col, pd.Series(["Year"] * len(df), index=df.index))
        df["annualized_wage"] = annualize(df[wage_col], unit_s)

        anchor = df["decision_date"].max()
        log_lines.append(f"{dataset}: {len(df):,} rows, anchor={anchor.date()}")

        for months in WINDOWS_MONTHS:
            cutoff = anchor - pd.DateOffset(months=months)
            sub = df[df["decision_date"] >= cutoff]
            if sub.empty:
                continue
            agg = sub.groupby("soc_code", as_index=False).agg(
                filings_count=("case_number", "count"),
                approvals_count=("is_approved", "sum"),
                offered_avg=("annualized_wage", "mean"),
                offered_median=("annualized_wage", "median"),
            )
            agg["approval_rate"] = (agg["approvals_count"] / agg["filings_count"]).clip(0, 1)
            agg["window"] = f"{months}m"
            agg["dataset"] = dataset
            agg["top_employers_json"] = agg["soc_code"].map(
                lambda soc, _sub=sub: _top_employers_json(_sub[_sub["soc_code"] == soc], "employer_id")
            )
            records.append(agg)

    if not records:
        log_lines.append("FAIL: no SOC demand data")
        return pd.DataFrame()

    df_out = pd.concat(records, ignore_index=True)

    # Competitiveness percentile: rank offered_median among same window×dataset
    df_out["competitiveness_percentile"] = df_out.groupby(
        ["window", "dataset"]
    )["offered_median"].rank(pct=True, na_option="keep")

    cols = ["soc_code", "window", "dataset", "filings_count", "approvals_count",
            "approval_rate", "offered_avg", "offered_median",
            "competitiveness_percentile", "top_employers_json"]
    df_out = df_out[[c for c in cols if c in df_out.columns]].reset_index(drop=True)
    log_lines.append(f"Output rows: {len(df_out):,}")
    return df_out


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_lines = [f"=== soc_demand_metrics build {datetime.now(timezone.utc).isoformat()} ==="]
    df_out = build_soc_demand(log_lines)

    if not args.dry_run:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp = OUT_PATH.parent / f".tmp_{ts}_soc_demand_metrics.parquet"
        df_out.to_parquet(tmp, index=False)
        if OUT_PATH.exists():
            OUT_PATH.unlink()
        tmp.rename(OUT_PATH)
        log.info(f"Written: {OUT_PATH}")
        log_lines.append(f"Written: {OUT_PATH}")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"soc_demand_metrics: {len(df_out):,} rows")


if __name__ == "__main__":
    main()
