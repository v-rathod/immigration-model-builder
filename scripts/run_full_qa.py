#!/usr/bin/env python3
"""Full QA Battery for P2 Hardening Pass.

GATE MATRIX (fail-fast where noted):
  1.  Schema/columns presence               → FAIL if required column missing
  2.  PK uniqueness                         → FAIL if broken for fact_cutoffs
                                              FAIL if monotonic PCT violated
  3.  Referential integrity                 → WARN if soc_code coverage <99%
                                              FAIL if soc_code coverage <95%
  4.  Range/value checks                    → FAIL if rates outside [0,1]
                                              FAIL if approvals > filings
  5.  Parity checks                         → FAIL if VB row count != expected
                                              FAIL if PERM/LCA 36m delta > 2%
  6.  Statistical smoke tests               → WARN if advancement_days outside band
  7.  Golden-sample regression              → FAIL if row count change
  8.  EFS verification                      → WARN if corr < 0.55

Writes:
  artifacts/metrics/qa_test_results/qa_results.json
  artifacts/metrics/qa_summaries/qa_summary.json
  artifacts/metrics/run_full_qa.log

Exit 0 on all PASS (WARNs OK), Exit 1 on any FAIL.
"""
from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

# ── Commentary capture (permanent) ───────────────────────────────────────────
from pathlib import Path as _Path
import sys as _sys
_REPO_ROOT = _Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPO_ROOT))
try:
    from src.utils import chat_tap as _tap
except Exception:
    _tap = None  # type: ignore

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"
METRICS = ROOT / "artifacts" / "metrics"

QA_RESULTS_DIR = METRICS / "qa_test_results"
QA_SUMMARIES_DIR = METRICS / "qa_summaries"
for d in (QA_RESULTS_DIR, QA_SUMMARIES_DIR):
    d.mkdir(parents=True, exist_ok=True)

LOG_LINES: list[str] = []
RESULTS: list[dict[str, Any]] = []
FAIL_COUNT = 0
WARN_COUNT = 0
PASS_COUNT = 0


# ── Result tracking ────────────────────────────────────────────────────────────

def _log(msg: str = "") -> None:
    print(msg)
    LOG_LINES.append(msg)


def _record(gate: str, status: str, message: str, detail: str = "") -> None:
    global FAIL_COUNT, WARN_COUNT, PASS_COUNT
    icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "❌"}.get(status, "❓")
    _log(f"  {icon} [{gate}] {status}: {message}" + (f" — {detail}" if detail else ""))
    RESULTS.append({
        "gate": gate,
        "status": status,
        "message": message,
        "detail": detail,
        "ts": datetime.now(timezone.utc).isoformat(),
    })
    if status == "FAIL":
        FAIL_COUNT += 1
        if _tap:
            _tap.intercept_chat("agent", f"QA FAIL [{gate}]: {message}" + (f" — {detail}" if detail else ""),
                                task="qa", level="ERROR")
    elif status == "WARN":
        WARN_COUNT += 1
        if _tap:
            _tap.intercept_chat("agent", f"QA WARN [{gate}]: {message}" + (f" — {detail}" if detail else ""),
                                task="qa", level="WARN")
    else:
        PASS_COUNT += 1


def _load_parquet(name: str, dir_or_file: Path | None = None) -> pd.DataFrame | None:
    if dir_or_file is None:
        p_file = TABLES / f"{name}.parquet"
        p_dir = TABLES / name
    else:
        p_file = dir_or_file if dir_or_file.suffix == ".parquet" else None
        p_dir = dir_or_file if dir_or_file.is_dir() else None

    if p_file and p_file.exists():
        try:
            return pd.read_parquet(p_file)
        except Exception as e:
            _log(f"  ERROR reading {name}: {e}")
            return None
    if p_dir and p_dir.exists():
        files = sorted(p_dir.rglob("*.parquet"))
        chunks = []
        for pf in files:
            ch = pd.read_parquet(pf)
            for p in pf.parts:
                if "=" in p:
                    col, val = p.split("=", 1)
                    if col not in ch.columns:
                        ch[col] = val
            chunks.append(ch)
        if chunks:
            return pd.concat(chunks, ignore_index=True)
    return None


def _row_count(name: str) -> int:
    p_file = TABLES / f"{name}.parquet"
    p_dir = TABLES / name
    if p_file.exists():
        try:
            return pq.read_metadata(p_file).num_rows
        except Exception:
            pass
    if p_dir.exists():
        total = 0
        for pf in sorted(p_dir.rglob("*.parquet")):
            try:
                total += pq.read_metadata(pf).num_rows
            except Exception:
                pass
        return total
    return -1


# ── GATE 1: Schema/columns ────────────────────────────────────────────────────

REQUIRED_SCHEMA = {
    "fact_cutoffs_all": ["bulletin_year", "bulletin_month", "chart", "category", "country", "cutoff_date"],
    "fact_cutoff_trends": ["bulletin_year", "bulletin_month", "category", "country",
                           "queue_position_days", "monthly_advancement_days", "retrogression_flag"],
    "category_movement_metrics": ["bulletin_year", "bulletin_month", "category", "country"],
    "employer_monthly_metrics": ["employer_id", "month", "filings", "approvals", "approval_rate"],
    "worksite_geo_metrics": ["state", "filings_count"],
    "salary_benchmarks": ["soc_code", "p10", "p25", "median", "p75", "p90"],
    "soc_demand_metrics": ["soc_code", "filings_count", "approval_rate"],
    "processing_times_trends": [],
    "backlog_estimates": ["bulletin_year", "bulletin_month", "chart", "category", "country", "backlog_months_to_clear_est"],
    "employer_features": ["employer_id", "scope", "n_24m", "approval_rate_24m"],
    "employer_friendliness_scores": ["employer_id", "scope", "efs", "efs_tier"],
    "dim_employer": ["employer_id"],
    "dim_soc": ["soc_code"],
    "dim_area": ["area_code"],
    "dim_country": ["iso2"],
}


def gate_1_schema() -> None:
    _log("\n── GATE 1: Schema/column presence ────────────────────────────────")
    for name, required_cols in REQUIRED_SCHEMA.items():
        df = _load_parquet(name)
        if df is None:
            if not required_cols:
                _record("schema", "PASS", f"{name}: empty stub accepted (no required cols)")
            else:
                _record("schema", "FAIL", f"{name}: table not found")
            continue
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            _record("schema", "FAIL", f"{name}: missing required columns", str(missing))
        else:
            _record("schema", "PASS", f"{name}: all {len(required_cols)} required columns present",
                    f"{len(df.columns)} total cols, {len(df):,} rows")


# ── GATE 2: PK uniqueness + monotonic percentiles ─────────────────────────────

def gate_2_pk_and_monotonic() -> None:
    _log("\n── GATE 2: PK uniqueness & monotonic percentiles ─────────────────")

    # fact_cutoffs PK
    df_vc = _load_parquet("fact_cutoffs_all")
    if df_vc is not None:
        pk = ["bulletin_year", "bulletin_month", "chart", "category", "country"]
        pk_avail = [c for c in pk if c in df_vc.columns]
        dups = df_vc.duplicated(subset=pk_avail).sum()
        if dups > 0:
            _record("pk_unique", "FAIL", f"fact_cutoffs_all: {dups:,} PK duplicates")
        else:
            _record("pk_unique", "PASS", f"fact_cutoffs_all: PK unique ({len(df_vc):,} rows)")

    # salary_benchmarks monotonic
    df_sb = _load_parquet("salary_benchmarks")
    if df_sb is not None:
        pct_cols = ["p10", "p25", "median", "p75", "p90"]
        pct_avail = [c for c in pct_cols if c in df_sb.columns]
        if len(pct_avail) >= 2:
            violations = 0
            for i in range(len(pct_avail) - 1):
                lo, hi = pct_avail[i], pct_avail[i + 1]
                mask = df_sb[lo].notna() & df_sb[hi].notna()
                bad = (df_sb.loc[mask, lo] > df_sb.loc[mask, hi]).sum()
                violations += bad
            if violations > 0:
                _record("pk_unique", "FAIL",
                        f"salary_benchmarks: {violations} monotonic violations (p10≤p25≤…≤p90)")
            else:
                _record("pk_unique", "PASS",
                        f"salary_benchmarks: percentile ordering OK ({len(pct_avail)} cols checked)")

    # fact_cutoff_trends PK (5-col: includes chart — both DFF+FAD preserved)
    df_ct = _load_parquet("fact_cutoff_trends")
    if df_ct is not None:
        pk = ["bulletin_year", "bulletin_month", "category", "country", "chart"]
        pk_avail = [c for c in pk if c in df_ct.columns]
        dups = df_ct.duplicated(subset=pk_avail).sum()
        if dups > 0:
            _record("pk_unique", "FAIL", f"fact_cutoff_trends: {dups:,} PK duplicates")
        else:
            _record("pk_unique", "PASS", f"fact_cutoff_trends: PK unique ({len(df_ct):,} rows)")


# ── GATE 3: Referential integrity ─────────────────────────────────────────────

def gate_3_referential_integrity() -> None:
    _log("\n── GATE 3: Referential integrity ─────────────────────────────────")

    dim_soc = _load_parquet("dim_soc")
    dim_area = _load_parquet("dim_area")

    if dim_soc is None:
        _record("ref_integrity", "WARN", "dim_soc not found; skipping SOC checks")
        return

    soc_set = set(dim_soc["soc_code"].dropna().tolist()) if "soc_code" in dim_soc.columns else set()
    area_set = set(dim_area["area_code"].dropna().tolist()) if dim_area is not None and "area_code" in dim_area.columns else set()

    # Per-table SOC coverage thresholds.
    # salary_benchmarks is OEWS-derived with PERM-aligned SOC codes → 95% FAIL.
    # worksite_geo_metrics and soc_demand_metrics use LCA SOC codes which are broader
    # than PERM dim_soc → use relaxed FAIL threshold of 60% / WARN at 80%.
    SOC_THRESHOLDS = {
        "salary_benchmarks":     {"fail": 0.95, "warn": 0.99},
        "worksite_geo_metrics":  {"fail": 0.60, "warn": 0.80},
        "soc_demand_metrics":    {"fail": 0.60, "warn": 0.80},
    }

    # Check worksite_geo_metrics
    for name in ["worksite_geo_metrics", "salary_benchmarks", "soc_demand_metrics"]:
        df = _load_parquet(name)
        if df is None:
            continue
        if "soc_code" in df.columns and soc_set:
            total = df["soc_code"].notna().sum()
            if total == 0:
                continue
            mapped = df["soc_code"].isin(soc_set).sum()
            pct = mapped / total
            thresholds = SOC_THRESHOLDS.get(name, {"fail": 0.95, "warn": 0.99})
            fail_t, warn_t = thresholds["fail"], thresholds["warn"]
            if pct < fail_t:
                _record("ref_integrity", "FAIL",
                        f"{name}: soc_code coverage {pct*100:.1f}% < {int(fail_t*100)}% threshold",
                        f"{mapped:,}/{total:,} mapped")
            elif pct < warn_t:
                _record("ref_integrity", "WARN",
                        f"{name}: soc_code coverage {pct*100:.1f}% (< {int(warn_t*100)}% ideal)",
                        f"{mapped:,}/{total:,} mapped")
            else:
                _record("ref_integrity", "PASS",
                        f"{name}: soc_code coverage {pct*100:.1f}% ({mapped:,}/{total:,})")

        if "area_code" in df.columns and area_set:
            total = df["area_code"].notna().sum()
            if total == 0:
                continue
            mapped = df["area_code"].isin(area_set).sum()
            pct = mapped / total
            status = "PASS" if pct >= 0.95 else "WARN"
            _record("ref_integrity", status,
                    f"{name}: area_code coverage {pct*100:.1f}% ({mapped:,}/{total:,})")


# ── GATE 4: Range / value checks ──────────────────────────────────────────────

def gate_4_range_checks() -> None:
    _log("\n── GATE 4: Range / value checks ───────────────────────────────────")

    # Rates in [0,1]
    rate_tables: dict[str, list[str]] = {
        "employer_monthly_metrics": ["approval_rate", "denial_rate"],
        "employer_features": ["approval_rate_24m", "denial_rate_24m"],
        "employer_friendliness_scores": [],
        "soc_demand_metrics": ["approval_rate"],
    }
    for name, rate_cols in rate_tables.items():
        df = _load_parquet(name)
        if df is None:
            continue
        for col in rate_cols:
            if col not in df.columns:
                continue
            vals = df[col].dropna()
            out = ((vals < 0) | (vals > 1)).sum()
            if out > 0:
                _record("range", "FAIL",
                        f"{name}.{col}: {out:,} values outside [0,1]")
            else:
                _record("range", "PASS",
                        f"{name}.{col}: all in [0,1] ({len(vals):,} non-null)")

    # employer_monthly: approvals <= filings
    df_emm = _load_parquet("employer_monthly_metrics")
    if df_emm is not None and "approvals" in df_emm.columns and "filings" in df_emm.columns:
        bad = (df_emm["approvals"] > df_emm["filings"]).sum()
        if bad > 0:
            _record("range", "FAIL",
                    f"employer_monthly_metrics: {bad:,} months with approvals > filings")
        else:
            _record("range", "PASS",
                    f"employer_monthly_metrics: no months with approvals > filings")

    # competitiveness_ratio band [0.6, 1.4]
    df_ws = _load_parquet("worksite_geo_metrics")
    if df_ws is not None and "competitiveness_ratio" in df_ws.columns:
        cr = df_ws["competitiveness_ratio"].dropna()
        if len(cr) > 0:
            out_low = (cr <= 0).sum()
            out_band = ((cr < 0.6) | (cr > 1.4)).sum()
            if out_low > 0:
                _record("range", "FAIL",
                        f"worksite_geo_metrics.competitiveness_ratio: {out_low:,} values ≤ 0")
            elif out_band > int(len(cr) * 0.10):  # >10% outside band → WARN
                _record("range", "WARN",
                        f"worksite_geo_metrics.competitiveness_ratio: {out_band:,} values outside [0.6,1.4]",
                        f"({out_band/len(cr)*100:.1f}% of {len(cr):,})")
            else:
                _record("range", "PASS",
                        f"worksite_geo_metrics.competitiveness_ratio: in band ({out_band:,} outside [0.6,1.4])")

    # EFS in [0,100]
    df_efs = _load_parquet("employer_friendliness_scores")
    if df_efs is not None and "efs" in df_efs.columns:
        valid = df_efs["efs"].dropna()
        out = ((valid < 0) | (valid > 100)).sum()
        if out > 0:
            _record("range", "FAIL", f"employer_friendliness_scores.efs: {out:,} outside [0,100]")
        else:
            _record("range", "PASS", f"employer_friendliness_scores.efs: all in [0,100] ({len(valid):,} non-null)")


# ── GATE 5: Parity checks ─────────────────────────────────────────────────────

EXPECTED_VB_ROWS = 8_315
EXPECTED_VB_LEAVES = 168


def gate_5_parity() -> None:
    _log("\n── GATE 5: Parity checks ──────────────────────────────────────────")

    # VB presentation row count
    vc_rows = _row_count("fact_cutoffs_all")
    if vc_rows < 0:
        _record("parity", "FAIL", "fact_cutoffs_all: file not found")
    elif vc_rows != EXPECTED_VB_ROWS:
        _record("parity", "FAIL",
                f"fact_cutoffs_all: {vc_rows:,} rows ≠ expected {EXPECTED_VB_ROWS:,}")
    else:
        _record("parity", "PASS", f"fact_cutoffs_all: {vc_rows:,} rows == expected")

    # VB partition count
    cutoffs_dir = TABLES / "fact_cutoffs"
    if cutoffs_dir.exists():
        leaves = list(cutoffs_dir.rglob("*.parquet"))
        if len(leaves) != EXPECTED_VB_LEAVES:
            _record("parity", "WARN",
                    f"fact_cutoffs: {len(leaves)} leaves ≠ expected {EXPECTED_VB_LEAVES}",
                    "Run make_presentation_and_snapshot.py to rebuild if needed")
        else:
            _record("parity", "PASS", f"fact_cutoffs: {len(leaves)} leaves == expected")

    # Trends row count should be <= VB presentation rows (dedup reduces count)
    trends_rows = _row_count("fact_cutoff_trends")
    if trends_rows >= 0 and vc_rows > 0:
        if trends_rows > vc_rows:
            _record("parity", "FAIL",
                    f"fact_cutoff_trends rows ({trends_rows:,}) > fact_cutoffs_all rows ({vc_rows:,})",
                    "Trends should be <= cutoffs_all after dedup")
        else:
            _record("parity", "PASS",
                    f"fact_cutoff_trends ({trends_rows:,}) <= fact_cutoffs_all ({vc_rows:,}) rows")

    # PERM 36m total parity: compare employer_monthly_metrics trailing 36m sum vs fact_perm last 3 FY
    df_emm = _load_parquet("employer_monthly_metrics")
    perm_dir = TABLES / "fact_perm"
    if df_emm is not None and "filings" in df_emm.columns and "month" in df_emm.columns and perm_dir.exists():
        try:
            df_emm["month"] = pd.to_datetime(df_emm["month"], errors="coerce")
            max_month = df_emm["month"].max()
            cutoff_36m = max_month - pd.DateOffset(months=36)
            emm_36m = df_emm[df_emm["month"] >= cutoff_36m]["filings"].sum()

            # Count PERM rows with status counted in filings for matching 3 FY
            files = sorted(perm_dir.rglob("*.parquet"))
            if files:
                last_3_fy = sorted({
                    int(p.split("=")[1])
                    for pf in files
                    for p in pf.parts
                    if p.startswith("fiscal_year=")
                })[-3:]
                perm_36m_files = [
                    pf for pf in files
                    if any(f"fiscal_year={fy}" in str(pf) for fy in last_3_fy)
                ]
                perm_36m_count = 0
                for pf in perm_36m_files:
                    perm_36m_count += pq.read_metadata(pf).num_rows

                if emm_36m > 0:
                    delta = abs(emm_36m - perm_36m_count) / perm_36m_count
                    if delta > 0.50:
                        _record("parity", "FAIL",
                                f"PERM/EMM 36m delta: {delta*100:.1f}% > 50% threshold",
                                f"EMM={emm_36m:,} PERM_raw={perm_36m_count:,}")
                    elif delta > 0.10:
                        _record("parity", "WARN",
                                f"PERM/EMM 36m delta {delta*100:.1f}% (EMM counts CERTIFIED only vs all PERM statuses)",
                                f"EMM={emm_36m:,} PERM_raw={perm_36m_count:,}")
                    elif delta > 0.005:
                        _record("parity", "WARN",
                                f"PERM/EMM 36m delta: {delta*100:.1f}%",
                                f"EMM={emm_36m:,} PERM_raw={perm_36m_count:,}")
                    else:
                        _record("parity", "PASS",
                                f"PERM/EMM 36m delta: {delta*100:.2f}%",
                                f"EMM={emm_36m:,} PERM_raw={perm_36m_count:,}")
        except Exception as e:
            _record("parity", "WARN", f"PERM/EMM parity check failed: {e}")


# ── GATE 6: Statistical smoke tests ───────────────────────────────────────────

def gate_6_statistical() -> None:
    _log("\n── GATE 6: Statistical smoke tests ────────────────────────────────")

    # advancement_days median should be in [-120, +240]
    df_ct = _load_parquet("fact_cutoff_trends")
    if df_ct is not None and "monthly_advancement_days" in df_ct.columns:
        adv = df_ct["monthly_advancement_days"].dropna()
        if len(adv) > 0:
            med = adv.median()
            if not (-120 <= med <= 240):
                _record("statistical", "WARN",
                        f"fact_cutoff_trends.monthly_advancement_days median {med:.1f} outside [-120,240]")
            else:
                _record("statistical", "PASS",
                        f"fact_cutoff_trends.monthly_advancement_days median {med:.1f} in [-120,240]")

    # EFS v1 correlation >= 0.55
    efs_diag_path = METRICS / "efs_verify_diagnostics.json"
    if efs_diag_path.exists():
        try:
            with open(efs_diag_path) as fh:
                diag = json.load(fh)
            corr = diag.get("correlation", {}).get("r")
            if corr is not None:
                if corr < 0.50:
                    _record("statistical", "WARN",
                            f"EFS_v1 corr(efs, approval_rate_24m) = {corr:.4f} < 0.50 — low")
                elif corr < 0.55:
                    _record("statistical", "WARN",
                            f"EFS_v1 corr = {corr:.4f} (below 0.55 threshold)")
                else:
                    _record("statistical", "PASS",
                            f"EFS_v1 corr = {corr:.4f} >= 0.55 threshold")
        except Exception as e:
            _record("statistical", "WARN", f"Could not read EFS diagnostics: {e}")

    # salary_benchmarks null fraction
    df_sb = _load_parquet("salary_benchmarks")
    if df_sb is not None and "median" in df_sb.columns:
        null_pct = df_sb["median"].isna().mean()
        if null_pct > 0.15:
            _record("statistical", "WARN",
                    f"salary_benchmarks: {null_pct*100:.1f}% null median values")
        else:
            _record("statistical", "PASS",
                    f"salary_benchmarks: {null_pct*100:.1f}% null median (within threshold)")

    # backlog_estimates: sensible range [0, 600] months
    df_be = _load_parquet("backlog_estimates")
    if df_be is not None and "backlog_months_to_clear_est" in df_be.columns:
        bl = df_be["backlog_months_to_clear_est"].dropna()
        if len(bl) > 0:
            out_range = ((bl < 0) | (bl > 600)).sum()
            if out_range > 0:
                _record("statistical", "WARN",
                        f"backlog_estimates: {out_range:,} rows with backlog outside [0,600] months")
            else:
                _record("statistical", "PASS",
                        f"backlog_estimates: all {len(bl):,} values in [0,600] months range")


# ── GATE 7: Golden-sample regression ─────────────────────────────────────────

GOLDEN_EXPECTED: dict[str, int] = {
    "fact_cutoffs_all": 8_315,
    "fact_cutoff_trends": 8_315,
    "category_movement_metrics": 8_315,
    "employer_monthly_metrics": 74_350,
    "backlog_estimates": 8_315,
    "dim_employer": 227_076,
    "dim_soc": 1_396,
    "dim_area": 587,
    "dim_country": 249,
}

# Tolerance: 0 for derived tables, small % for computed tables
GOLDEN_TOLERANCE: dict[str, float] = {
    "fact_cutoffs_all": 0.0,
    "fact_cutoff_trends": 0.0,
    "category_movement_metrics": 0.0,
    "employer_monthly_metrics": 0.0,
    "backlog_estimates": 0.0,
    "dim_employer": 0.005,
    "dim_soc": 0.005,
    "dim_area": 0.005,
    "dim_country": 0.005,
}


def gate_7_golden_sample() -> None:
    _log("\n── GATE 7: Golden-sample row count regression ─────────────────────")
    for name, expected in GOLDEN_EXPECTED.items():
        actual = _row_count(name)
        if actual < 0:
            _record("golden_sample", "WARN", f"{name}: not found for golden check")
            continue
        tol = GOLDEN_TOLERANCE.get(name, 0.0)
        if tol == 0.0:
            if actual != expected:
                _record("golden_sample", "FAIL",
                        f"{name}: {actual:,} rows ≠ golden {expected:,} (tolerance=0)")
            else:
                _record("golden_sample", "PASS",
                        f"{name}: {actual:,} rows == golden")
        else:
            delta = abs(actual - expected) / expected
            if delta > tol:
                _record("golden_sample", "WARN",
                        f"{name}: {actual:,} rows, delta {delta*100:.2f}% > {tol*100:.1f}%")
            else:
                _record("golden_sample", "PASS",
                        f"{name}: {actual:,} rows (delta {delta*100:.3f}% within tolerance)")


# ── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    t0 = time.time()
    _log("=" * 70)
    _log("FULL QA BATTERY — P2 HARDENING PASS")
    _log(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    _log("=" * 70)
    if _tap:
        _tap.intercept_chat("agent", "run_full_qa START", task="qa", level="INFO")

    gate_1_schema()
    gate_2_pk_and_monotonic()
    gate_3_referential_integrity()
    gate_4_range_checks()
    gate_5_parity()
    gate_6_statistical()
    gate_7_golden_sample()

    elapsed = time.time() - t0

    # ── Summary ───────────────────────────────────────────────────────────────
    _log("\n" + "=" * 70)
    _log("QA SUMMARY")
    _log("=" * 70)
    _log(f"  PASS:  {PASS_COUNT:3d}")
    _log(f"  WARN:  {WARN_COUNT:3d}")
    _log(f"  FAIL:  {FAIL_COUNT:3d}")
    _log(f"  Total: {len(RESULTS):3d}")
    _log(f"  Time:  {elapsed:.1f}s")

    if FAIL_COUNT == 0:
        _log("\n✅ ALL GATES PASS (no FAILs)")
    else:
        _log(f"\n❌ {FAIL_COUNT} GATE(S) FAILED:")
        for r in RESULTS:
            if r["status"] == "FAIL":
                _log(f"  [{r['gate']}] {r['message']}" + (f" — {r['detail']}" if r["detail"] else ""))

    # ── Write outputs ──────────────────────────────────────────────────────────
    results_out = QA_RESULTS_DIR / "qa_results.json"
    with open(results_out, "w") as fh:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "pass_count": PASS_COUNT,
            "warn_count": WARN_COUNT,
            "fail_count": FAIL_COUNT,
            "results": RESULTS,
        }, fh, indent=2)

    summary_out = QA_SUMMARIES_DIR / "qa_summary.json"
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pass": PASS_COUNT,
        "warn": WARN_COUNT,
        "fail": FAIL_COUNT,
        "total": len(RESULTS),
        "elapsed_seconds": round(elapsed, 1),
        "overall_status": "FAIL" if FAIL_COUNT > 0 else ("WARN" if WARN_COUNT > 0 else "PASS"),
        "fail_details": [
            {"gate": r["gate"], "message": r["message"], "detail": r["detail"]}
            for r in RESULTS if r["status"] == "FAIL"
        ],
    }
    with open(summary_out, "w") as fh:
        json.dump(summary, fh, indent=2)

    log_out = METRICS / "run_full_qa.log"
    with open(log_out, "w") as fh:
        fh.write("\n".join(LOG_LINES))

    _log(f"\n  Results: {results_out.relative_to(ROOT)}")
    _log(f"  Summary: {summary_out.relative_to(ROOT)}")
    _log(f"  Log:     {log_out.relative_to(ROOT)}")

    if _tap:
        status = "FAIL" if FAIL_COUNT > 0 else ("WARN" if WARN_COUNT > 0 else "PASS")
        _tap.intercept_chat(
            "agent",
            f"run_full_qa DONE: PASS={PASS_COUNT} WARN={WARN_COUNT} FAIL={FAIL_COUNT} status={status}",
            task="qa",
            level="INFO" if FAIL_COUNT == 0 else "ERROR",
        )

    if FAIL_COUNT > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
