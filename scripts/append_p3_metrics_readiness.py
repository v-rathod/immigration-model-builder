#!/usr/bin/env python3
"""
Append "P3 Metrics Readiness" section to FINAL_SINGLE_REPORT.md.

Reads all 8 P3 parquets, collects:
  - Row counts and date ranges
  - WARN/FAIL lines from each .log file
  - DIGEST JSON summary

Then appends the section to artifacts/metrics/FINAL_SINGLE_REPORT.md
and prints READY_TO_UPLOAD_FILE.
"""
import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"
METRICS = ROOT / "artifacts" / "metrics"
REPORT = METRICS / "FINAL_SINGLE_REPORT.md"

P3_TABLES = [
    "fact_cutoff_trends",
    "employer_monthly_metrics",
    "category_movement_metrics",
    "worksite_geo_metrics",
    "salary_benchmarks",
    "soc_demand_metrics",
    "processing_times_trends",
    "backlog_estimates",
]

DATE_COLS = {
    "fact_cutoff_trends": ["bulletin_year", "bulletin_month"],
    "employer_monthly_metrics": ["month"],
    "category_movement_metrics": ["bulletin_year", "bulletin_month"],
    "worksite_geo_metrics": [],
    "salary_benchmarks": [],
    "soc_demand_metrics": [],
    "processing_times_trends": ["as_of_date"],
    "backlog_estimates": ["bulletin_year", "bulletin_month"],
}


def _date_range_str(df, name):
    cols = DATE_COLS.get(name, [])
    if not cols or df.empty:
        return "n/a"
    try:
        if "bulletin_year" in cols and "bulletin_month" in cols:
            df["_ym"] = df["bulletin_year"].astype(str) + "-" + df["bulletin_month"].astype(str).str.zfill(2)
            return f"{df['_ym'].min()} → {df['_ym'].max()}"
        for c in cols:
            if c in df.columns and df[c].notna().any():
                vals = pd.to_datetime(df[c], errors="coerce").dropna()
                if len(vals):
                    return f"{vals.min().date()} → {vals.max().date()}"
    except Exception:
        pass
    return "n/a"


LOG_OVERRIDES = {
    "salary_benchmarks": "salary_benchmarks_fix.log",
}


def _get_log_issues(name):
    filename = LOG_OVERRIDES.get(name, f"{name}.log")
    log_path = METRICS / filename
    warns, fails = [], []
    if log_path.exists():
        for line in log_path.read_text().splitlines():
            upper = line.upper()
            if "FAIL" in upper:
                fails.append(line.strip())
            elif "WARN" in upper:
                warns.append(line.strip())
    return warns, fails


def build_section():
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = []
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## P3 Metrics Readiness")
    lines.append("")
    lines.append(f"_Generated: {now}_")
    lines.append("")

    digest = {}
    all_warns = []
    all_fails = []
    table_rows = []

    for name in P3_TABLES:
        pq = TABLES / f"{name}.parquet"
        if pq.exists():
            try:
                df = pd.read_parquet(pq)
                nrows = len(df)
                date_range = _date_range_str(df, name)
                status = "✅"
            except Exception as e:
                nrows = 0
                date_range = f"ERROR: {e}"
                status = "❌"
        else:
            nrows = 0
            date_range = "FILE MISSING"
            status = "❌"

        warns, fails = _get_log_issues(name)
        all_warns.extend(warns)
        all_fails.extend(fails)
        digest[name] = nrows

        warn_flag = " ⚠️" if warns else ""
        fail_flag = " ❌" if fails else ""
        table_rows.append(
            f"| {name} | {nrows:,} | {date_range} | {status}{warn_flag}{fail_flag} |"
        )

    lines.append("### Dataset Summary")
    lines.append("")
    lines.append("| Dataset | Rows | Date Range | Status |")
    lines.append("|---------|------|------------|--------|")
    lines.extend(table_rows)
    lines.append("")

    if all_fails:
        lines.append("### ❌ FAIL Lines")
        lines.append("")
        for f in all_fails:
            lines.append(f"- `{f}`")
        lines.append("")

    if all_warns:
        lines.append("### ⚠️ WARN Lines")
        lines.append("")
        for w in all_warns:
            lines.append(f"- `{w}`")
        lines.append("")

    lines.append("### DIGEST")
    lines.append("")
    lines.append("```json")
    lines.append(json.dumps({"p3_metrics": digest}, indent=2))
    lines.append("```")
    lines.append("")

    return "\n".join(lines)


def main():
    section = build_section()

    # Append to report (avoid duplicate section)
    existing = REPORT.read_text() if REPORT.exists() else ""
    if "## P3 Metrics Readiness" in existing:
        # Replace existing section
        updated = re.sub(
            r"\n---\n\n## P3 Metrics Readiness.*$",
            section,
            existing,
            flags=re.DOTALL,
        )
    else:
        updated = existing.rstrip() + "\n" + section

    REPORT.write_text(updated)
    print(f"Appended P3 Metrics Readiness to {REPORT}")
    print(f"READY_TO_UPLOAD_FILE: artifacts/metrics/FINAL_SINGLE_REPORT.md")


if __name__ == "__main__":
    main()
