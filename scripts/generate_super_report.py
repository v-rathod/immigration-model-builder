#!/usr/bin/env python3
"""
generate_super_report.py
========================
Appends two sections to artifacts/metrics/FINAL_SINGLE_REPORT.md:

  1) ## P2 HARDENING ADDENDUM
     - Per-table: rows, partitions, year span, PK status, sample columns
     - Top anomalies: out-of-bounds ratios, big monthly deltas, missing SOC/area maps
     - Before→After correction stats (salary_benchmarks, employer_monthly)
     - EFS v1 & v2 (if built) correlation, Brier, top features
     - Full WARN/FAIL log excerpts (top 100 lines per file)

  2) ## P3 METRICS READINESS  (replaces prior version appended by append_p3_metrics_readiness.py)
     - Row counts, date ranges, QA PASS/WARN/FAIL counts
     - One-line DIGEST JSON

Run:  python scripts/generate_super_report.py
"""
from __future__ import annotations

import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

# ── Commentary capture (permanent) ───────────────────────────────────────────
import sys as _sys
from pathlib import Path as _Path
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
REPORT = METRICS / "FINAL_SINGLE_REPORT.md"

LOG_LINES: list[str] = []


def log(msg: str = "") -> None:
    print(msg)
    LOG_LINES.append(msg)


# ── Data loading helpers ──────────────────────────────────────────────────────

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


def _load_sample(name: str, max_rows: int = 500_000) -> pd.DataFrame | None:
    p_file = TABLES / f"{name}.parquet"
    p_dir = TABLES / name
    try:
        if p_file.exists():
            return pd.read_parquet(p_file)
        if p_dir.exists():
            files = sorted(p_dir.rglob("*.parquet"))
            chunks = []
            cnt = 0
            for pf in files:
                ch = pd.read_parquet(pf)
                for part in pf.parts:
                    if "=" in part:
                        col, val = part.split("=", 1)
                        if col not in ch.columns:
                            ch[col] = val
                chunks.append(ch)
                cnt += len(ch)
                if cnt >= max_rows:
                    break
            return pd.concat(chunks, ignore_index=True) if chunks else None
    except Exception as e:
        log(f"  WARNING: could not load {name}: {e}")
    return None


def _date_range(df: pd.DataFrame, year_col: str | None, month_col: str | None,
                date_col: str | None) -> str:
    try:
        if year_col and month_col and year_col in df.columns and month_col in df.columns:
            ym = (df[year_col].astype(str) + "-" + df[month_col].astype(str).str.zfill(2))
            return f"{ym.min()} → {ym.max()}"
        if date_col and date_col in df.columns:
            vals = pd.to_datetime(df[date_col], errors="coerce").dropna()
            if len(vals):
                return f"{vals.min().date()} → {vals.max().date()}"
    except Exception:
        pass
    return "n/a"


def _read_log_issues(name: str, max_lines: int = 100) -> list[str]:
    """Read WARN/ERROR/FAIL lines from a log file."""
    log_map = {
        "salary_benchmarks": "salary_benchmarks_fix.log",
    }
    fname = log_map.get(name, f"{name}.log")
    log_path = METRICS / fname
    if not log_path.exists():
        return []
    lines = log_path.read_text().splitlines()
    return [l for l in lines if any(kw in l.upper() for kw in ["WARN", "ERROR", "FAIL"])][:max_lines]


# ── P3 table metadata ─────────────────────────────────────────────────────────

P3_TABLES = [
    ("fact_cutoff_trends", "bulletin_year", "bulletin_month", None),
    ("employer_monthly_metrics", None, None, "month"),
    ("category_movement_metrics", "bulletin_year", "bulletin_month", None),
    ("worksite_geo_metrics", None, None, None),
    ("salary_benchmarks", None, None, None),
    ("soc_demand_metrics", None, None, None),
    ("processing_times_trends", None, None, "as_of_date"),
    ("backlog_estimates", "bulletin_year", "bulletin_month", None),
]


# ── Section builders ──────────────────────────────────────────────────────────

def build_addendum() -> str:
    lines: list[str] = []
    lines.append("## P2 HARDENING ADDENDUM\n")
    lines.append(f"_Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n")

    # ── Table inventory ────────────────────────────────────────────────────────
    lines.append("\n### Table Inventory\n")
    lines.append("| Table | Rows | Files/Partitions | Year Span | Columns |")
    lines.append("|-------|------|------------------|-----------|---------|")

    all_tables = [
        "fact_cutoffs_all", "fact_cutoff_trends", "category_movement_metrics",
        "employer_monthly_metrics", "worksite_geo_metrics", "salary_benchmarks",
        "soc_demand_metrics", "processing_times_trends", "backlog_estimates",
        "employer_features", "employer_friendliness_scores",
        "dim_employer", "dim_soc", "dim_area", "dim_country", "dim_visa_class",
    ]

    for name in all_tables:
        rows = _row_count(name)
        p_file = TABLES / f"{name}.parquet"
        p_dir = TABLES / name
        if p_dir.exists() and not p_file.exists():
            n_files = len(list(p_dir.rglob("*.parquet")))
            files_str = f"{n_files} files"
            # year span from partition names
            years = sorted({
                int(p.split("=")[1])
                for pf in sorted(p_dir.rglob("*.parquet"))
                for p in pf.parts
                if p.startswith(("fiscal_year=", "bulletin_year="))
            })
            span = f"{years[0]}–{years[-1]}" if years else "n/a"
        elif p_file.exists():
            files_str = "1 file"
            span = "n/a"
        else:
            files_str = "MISSING"
            span = "n/a"

        col_count = "?"
        try:
            if p_file.exists():
                meta = pq.read_metadata(p_file)
                col_count = str(meta.num_columns)
            elif p_dir.exists():
                files = list(p_dir.rglob("*.parquet"))
                if files:
                    col_count = str(pq.read_metadata(files[0]).num_columns)
        except Exception:
            pass

        row_str = f"{rows:,}" if rows >= 0 else "MISSING"
        lines.append(f"| {name} | {row_str} | {files_str} | {span} | {col_count} cols |")

    # ── Correction stats (before→after) ───────────────────────────────────────
    lines.append("\n### Before→After Correction Stats\n")

    # salary_benchmarks corrections
    sb_fix_log = METRICS / "salary_benchmarks_fix.log"
    if sb_fix_log.exists():
        log_text = sb_fix_log.read_text()
        lines.append("**salary_benchmarks (percentile enforcement):**")
        lines.append("```")
        for line in log_text.splitlines():
            if any(kw in line for kw in ["corrections_applied", "QA PASS", "QA FAIL", "rows_with_null"]):
                lines.append(f"  {line}")
        lines.append("```\n")

    # employer_monthly_metrics guard
    emm_log = METRICS / "employer_monthly_metrics.log"
    if emm_log.exists():
        log_text = emm_log.read_text()
        lines.append("**employer_monthly_metrics (approvals guard):**")
        lines.append("```")
        for line in log_text.splitlines():
            if any(kw in line for kw in ["QA PASS", "QA FAIL", "WARN", "guard", "approvals"]):
                lines.append(f"  {line}")
        lines.append("```\n")

    # ── EFS stats ─────────────────────────────────────────────────────────────
    lines.append("\n### EFS v1 Statistics\n")
    efs_diag_path = METRICS / "efs_verify_diagnostics.json"
    if efs_diag_path.exists():
        try:
            with open(efs_diag_path) as fh:
                diag = json.load(fh)
            corr = diag.get("correlation", {})
            if corr.get("r") is not None:
                lines.append(f"- Pearson r (EFS_v1 vs approval_rate_24m): **{corr['r']:.4f}**")
                lines.append(f"- 95% bootstrap CI: [{corr.get('ci_lo', '?'):.4f}, {corr.get('ci_hi', '?'):.4f}]")
                lines.append(f"- n = {corr.get('n', '?'):,}")
            qs = diag.get("quantiles", {})
            if qs:
                lines.append(f"\n**EFS distribution quantiles:**")
                for q, v in sorted(qs.items()):
                    lines.append(f"  - {q}: {v:.1f}")
            cov = diag.get("coverage", {})
            if cov:
                lines.append(f"\n**Coverage:** {cov.get('overall_scored','?'):,} / "
                             f"{cov.get('overall_total','?'):,} employers scored")
        except Exception as e:
            lines.append(f"_(Error reading EFS diagnostics: {e})_")
    else:
        lines.append("_(efs_verify_diagnostics.json not found)_")

    # EFS v2 ML (if available)
    ml_diag_path = METRICS / "employer_score_ml_diagnostics.json"
    if ml_diag_path.exists():
        try:
            with open(ml_diag_path) as fh:
                ml_diag = json.load(fh)
            lines.append("\n### EFS v2 ML Statistics\n")
            lines.append(f"- n_train: {ml_diag.get('n_train', '?'):,}")
            lines.append(f"- CV AUC: {ml_diag.get('cv_auc_mean', '?'):.4f} ± {ml_diag.get('cv_auc_std', '?'):.4f}")
            lines.append(f"- Brier score: {ml_diag.get('brier_score', '?'):.4f}")
            lines.append(f"- n_employers_scored: {ml_diag.get('n_employers_scored', '?'):,}")
            corr_v1_ml = ml_diag.get("corr_efs_ml_vs_v1")
            if corr_v1_ml is not None:
                lines.append(f"- corr(EFS_ml, EFS_v1): {corr_v1_ml:.4f}")
            corr_ar = ml_diag.get("corr_efs_ml_vs_approval_rate_24m")
            if corr_ar is not None:
                lines.append(f"- corr(EFS_ml, approval_rate_24m): {corr_ar:.4f}")
            fi = ml_diag.get("feature_importances") or ml_diag.get("shap_mean_abs", {})
            if fi:
                lines.append("\n**Top features:**")
                for feat, imp in sorted(fi.items(), key=lambda x: -x[1])[:8]:
                    lines.append(f"  - {feat}: {imp:.5f}")
        except Exception as e:
            lines.append(f"\n_(Error reading ML diagnostics: {e})_")

    # ── QA summary ─────────────────────────────────────────────────────────────
    lines.append("\n### QA Battery Summary\n")
    qa_summary_path = METRICS / "qa_summaries" / "qa_summary.json"
    if qa_summary_path.exists():
        try:
            with open(qa_summary_path) as fh:
                qa_sum = json.load(fh)
            lines.append(f"- **Overall status:** {qa_sum.get('overall_status', '?')}")
            lines.append(f"- PASS: {qa_sum.get('pass', '?')}")
            lines.append(f"- WARN: {qa_sum.get('warn', '?')}")
            lines.append(f"- FAIL: {qa_sum.get('fail', '?')}")
            if qa_sum.get("fail_details"):
                lines.append("\n**FAIL details:**")
                for fd in qa_sum["fail_details"]:
                    lines.append(f"  - [{fd['gate']}] {fd['message']}" + (f" — {fd['detail']}" if fd.get("detail") else ""))
        except Exception as e:
            lines.append(f"_(Error reading QA summary: {e})_")
    else:
        lines.append("_(QA summary not found — run python scripts/run_full_qa.py)_")

    # ── Top anomalies ──────────────────────────────────────────────────────────
    lines.append("\n### Top Anomalies\n")

    # Employer monthly: WARN employers (low 36m approval rate)
    emm_log_path = METRICS / "employer_monthly_metrics.log"
    if emm_log_path.exists():
        emm_warns = [l for l in emm_log_path.read_text().splitlines()
                     if "WARN" in l.upper() or "outlier" in l.lower()][:10]
        if emm_warns:
            lines.append("**employer_monthly_metrics WARNs (top 10):**")
            lines.append("```")
            for w in emm_warns:
                lines.append(f"  {w}")
            lines.append("```\n")

    # Salary benchmarks: null row count
    sb_log_path = METRICS / "salary_benchmarks_fix.log"
    if sb_log_path.exists():
        sb_warns = [l for l in sb_log_path.read_text().splitlines()
                    if "null" in l.lower() or "WARN" in l.upper()][:5]
        if sb_warns:
            lines.append("**salary_benchmarks null/WARN lines:**")
            lines.append("```")
            for w in sb_warns:
                lines.append(f"  {w}")
            lines.append("```\n")

    # ── WARN/ERROR log excerpts ────────────────────────────────────────────────
    lines.append("\n### WARN/ERROR/FAIL Log Excerpts (top 100 per file)\n")
    log_files = sorted(METRICS.glob("*.log"))
    found_any = False
    for lf in log_files:
        try:
            file_lines = lf.read_text().splitlines()
            matches = [l for l in file_lines
                       if any(kw in l.upper() for kw in ["WARN", "ERROR", "FAIL"])][:100]
            if matches:
                found_any = True
                lines.append(f"**{lf.name}:**")
                lines.append("```")
                for m in matches:
                    lines.append(f"  {m}")
                lines.append("```\n")
        except Exception:
            pass
    if not found_any:
        lines.append("_No WARN/ERROR/FAIL lines found in any log file._\n")

    lines.append("\n---\n")
    return "\n".join(lines)


def build_p3_readiness() -> str:
    """Build the P3 Metrics Readiness section (replaces prior version)."""
    lines: list[str] = []
    lines.append("## P3 Metrics Readiness\n")
    lines.append(f"_Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_\n")

    # Table summary
    lines.append("| Dataset | Rows | Date Range | Status |")
    lines.append("|---------|------|------------|--------|")

    digest: dict[str, Any] = {}
    all_warns: list[str] = []
    all_fails: list[str] = []

    for name, yr_col, mo_col, dt_col in P3_TABLES:
        rows = _row_count(name)
        if rows < 0:
            status = "❌"
            all_fails.append(f"FAIL: {name} not found")
            date_range_str = "n/a"
        else:
            df = _load_sample(name)
            date_range_str = _date_range(df, yr_col, mo_col, dt_col) if df is not None else "n/a"

            issues = _read_log_issues(name)
            has_fail = any("FAIL" in l.upper() for l in issues)
            has_warn = any("WARN" in l.upper() for l in issues)
            if has_fail:
                status = "❌"
                for l in issues:
                    if "FAIL" in l.upper():
                        all_fails.append(f"FAIL ({name}): {l.strip()}")
            elif has_warn:
                status = "✅ ⚠️"
                for l in issues:
                    if "WARN" in l.upper():
                        all_warns.append(f"WARN ({name}): {l.strip()}")
            else:
                status = "✅"

        digest[name] = rows if rows >= 0 else "MISSING"
        lines.append(f"| {name} | {rows:,} | {date_range_str} | {status} |")

    # WARN lines
    if all_warns:
        lines.append("\n### ⚠️ WARN Lines\n")
        for w in all_warns:
            lines.append(f"- {w}")

    # FAIL lines
    if all_fails:
        lines.append("\n### ❌ FAIL Lines\n")
        for f in all_fails:
            lines.append(f"- {f}")

    # QA battery result
    qa_path = METRICS / "qa_summaries" / "qa_summary.json"
    if qa_path.exists():
        try:
            with open(qa_path) as fh:
                qa = json.load(fh)
            lines.append(f"\n### QA Battery: {qa.get('overall_status', '?')}")
            lines.append(f"- PASS: {qa.get('pass', 0)} | WARN: {qa.get('warn', 0)} | FAIL: {qa.get('fail', 0)}")
            if qa.get("fail_details"):
                lines.append("- FAILs: " + "; ".join(
                    d["message"] for d in qa["fail_details"]
                ))
        except Exception:
            pass

    # DIGEST
    lines.append(f"\n### DIGEST\n")
    lines.append('```json')
    lines.append(json.dumps({"p3_metrics": digest}, separators=(",", ":")))
    lines.append('```')

    lines.append("\n---\n")
    return "\n".join(lines)


# ── Append to report ──────────────────────────────────────────────────────────

def _remove_prior_sections(text: str) -> str:
    """Remove any prior P2 HARDENING ADDENDUM, P3 Metrics Readiness, or
    Commentary & Execution Artifacts sections so they can be re-appended fresh."""
    # Remove from ## P2 HARDENING ADDENDUM to next ## heading (or EOF)
    text = re.sub(r"(?m)^## P2 HARDENING ADDENDUM.*?(?=^## |\Z)", "", text, flags=re.DOTALL)
    # Remove any prior P3 Metrics Readiness sections
    text = re.sub(r"(?m)^## P3 Metrics Readiness.*?(?=^## |\Z)", "", text, flags=re.DOTALL)
    # Remove Commentary & Execution Artifacts section (re-appended at bottom below)
    text = re.sub(r"(?m)^---\s*\n## Commentary & Execution Artifacts.*?(?=^## |\Z)", "", text, flags=re.DOTALL)
    text = re.sub(r"(?m)^## Commentary & Execution Artifacts.*?(?=^## |\Z)", "", text, flags=re.DOTALL)
    return text.rstrip() + "\n"


def main() -> None:
    t0 = time.time()
    log("=" * 60)
    log("GENERATE SUPER REPORT")
    log(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)
    if _tap:
        _tap.intercept_chat("agent", "generate_super_report START", task="super_report", level="INFO")

    if not REPORT.exists():
        log(f"ERROR: {REPORT} not found — run generate_final_report.py first")
        sys.exit(1)

    current = REPORT.read_text()
    log(f"  Current report: {len(current.splitlines())} lines")

    # Remove old hardening/p3 sections
    cleaned = _remove_prior_sections(current)

    # Build new sections
    log("  Building P2 Hardening Addendum …")
    addendum = build_addendum()

    log("  Building P3 Metrics Readiness …")
    p3 = build_p3_readiness()

    # Append
    final = cleaned + "\n" + addendum + "\n" + p3
    REPORT.write_text(final)

    # Re-append Commentary & Execution Artifacts at bottom (always last)
    if _tap:
        try:
            _tap.append_commentary_section(REPORT)
        except Exception:
            pass

    elapsed = time.time() - t0
    log(f"\n✓ Super report written: {REPORT.relative_to(ROOT)}")
    log(f"  Lines: {len(final.splitlines()):,}")
    log(f"  Elapsed: {elapsed:.1f}s")

    # Write log
    log_path = METRICS / "generate_super_report.log"
    with open(log_path, "w") as fh:
        fh.write("\n".join(LOG_LINES))

    if _tap:
        _tap.intercept_chat("agent",
            f"generate_super_report DONE: {len(final.splitlines())} lines  elapsed={elapsed:.1f}s",
            task="super_report", level="INFO")


if __name__ == "__main__":
    main()
