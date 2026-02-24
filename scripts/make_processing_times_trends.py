#!/usr/bin/env python3
"""
STEP 7 — Build processing_times_trends.parquet
Parses USCIS I-485 performance data (quarterly cross-tabulations) to produce
a time series of employment-based I-485 processing volume and throughput metrics.

Source data: USCIS quarterly I-485 performance reports (CSV/XLSX) downloaded by
Project 1 into:
    <data_root>/USCIS_IMMIGRATION/employment_based/{year}/i485_performancedata_*

The cross-tab format has:
    - 22 columns: 2 location cols + 5 categories × 4 metrics each
    - Category blocks: Family-based, Employment-based, Humanitarian, Others, Total
    - Metric sub-columns: Applications Received, Approved, Denied, Pending
    - A "Total" row provides national-level aggregates
    - Date range header row identifies the reporting quarter

We extract the EB-specific national totals and derive:
    - approval_rate = approved / (approved + denied)
    - throughput = approved + denied (completions per quarter)
    - net_intake = received - throughput (backlog change per quarter)
    - backlog_months = pending / (throughput / 3)  (months of backlog at current pace)

Output: artifacts/tables/processing_times_trends.parquet
Log:    artifacts/metrics/processing_times_trends.log
"""
from datetime import datetime, timezone, date
from pathlib import Path
import re
import logging
import glob

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
OUT_PATH = TABLES / "processing_times_trends.parquet"
LOG_PATH = METRICS / "processing_times_trends.log"

# Default data root (Project 1's downloads)
DATA_ROOT = Path(__file__).resolve().parent.parent.parent / "fetch-immigration-data" / "downloads"
USCIS_EB = DATA_ROOT / "USCIS_IMMIGRATION" / "employment_based"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# EB columns are detected dynamically (different files have different column counts)
# We scan header rows for "Employment" to find the EB Received column,
# then EB Approved = +1, EB Denied = +2, EB Pending = +3.

# Fiscal year quarter date ranges
QUARTER_MONTHS = {
    "Q1": (10, 12),  # Oct-Dec of prior calendar year
    "Q2": (1, 3),    # Jan-Mar
    "Q3": (4, 6),    # Apr-Jun
    "Q4": (7, 9),    # Jul-Sep
}


def _parse_number(val) -> float:
    """Parse a number from USCIS format (may have commas, spaces, 'D' for suppressed)."""
    if pd.isna(val):
        return np.nan
    s = str(val).strip()
    if s in ("", "-", "D", "d", "N/A", "*", "X"):
        return np.nan
    # Remove commas and spaces
    s = s.replace(",", "").replace(" ", "")
    try:
        return float(s)
    except ValueError:
        return np.nan


def _detect_date_range(df_raw: pd.DataFrame) -> tuple[str | None, int | None, int | None]:
    """Scan first 5 rows for a date range like 'October 1 - December 31, 2013' or
    'July 1, 2023 - September 30, 2023'. Returns (date_str, fiscal_year, quarter)."""

    month_to_num = {
        "january": 1, "february": 2, "march": 3, "april": 4,
        "may": 5, "june": 6, "july": 7, "august": 8,
        "september": 9, "october": 10, "november": 11, "december": 12,
    }

    for i in range(min(5, len(df_raw))):
        for j in range(min(3, df_raw.shape[1])):
            cell = str(df_raw.iloc[i, j]).strip()
            if not cell or cell == "nan":
                continue

            # Pattern: "Month Day, Year - Month Day, Year" or "Month Day - Month Day, Year"
            m = re.search(
                r"(\w+)\s+\d+[,]?\s*(?:\d{4})?\s*[-–]\s*(\w+)\s+\d+[,]?\s*(\d{4})",
                cell,
            )
            if m:
                end_month_name = m.group(2).lower()
                end_year = int(m.group(3))
                end_month = month_to_num.get(end_month_name)

                if end_month:
                    # Determine FY and quarter from ending month
                    if end_month >= 10:
                        fy = end_year + 1
                        qtr = 1
                    elif end_month <= 3:
                        fy = end_year
                        qtr = 2
                    elif end_month <= 6:
                        fy = end_year
                        qtr = 3
                    else:
                        fy = end_year
                        qtr = 4
                    return cell, fy, qtr

    return None, None, None


def _find_eb_columns(df_raw: pd.DataFrame) -> int | None:
    """Find the first column index containing 'Employment' in any header row (first 10 rows).
    The EB metrics block is 4 consecutive columns: Received, Approved, Denied, Pending."""
    for i in range(min(10, len(df_raw))):
        for j in range(df_raw.shape[1]):
            val = str(df_raw.iloc[i, j]).strip().lower()
            if "employment" in val or "employ" in val:
                return j
    return None


def _find_total_columns(df_raw: pd.DataFrame) -> int | None:
    """Find the first column for 'Total' category in the header rows (not total data row).
    Returns column index of total Received."""
    for i in range(min(10, len(df_raw))):
        for j in range(2, df_raw.shape[1]):  # skip first 2 location cols
            val = str(df_raw.iloc[i, j]).strip().lower()
            if val == "total" or val == "total1":
                # Verify this is a category header (should have sub-cols after it)
                return j
    return None


def _find_total_row(df_raw: pd.DataFrame) -> int | None:
    """Find the row index containing 'Total' in column 0."""
    for i in range(len(df_raw)):
        val = str(df_raw.iloc[i, 0]).strip().lower()
        if val == "total":
            return i
    return None


def _parse_single_file(fpath: Path, log_lines: list) -> dict | None:
    """Parse a single I-485 performance data file and return a dict of metrics."""
    try:
        if fpath.suffix.lower() == ".csv":
            df = pd.read_csv(fpath, encoding="latin-1", header=None, dtype=str)
        elif fpath.suffix.lower() in (".xlsx", ".xls"):
            df = pd.read_excel(fpath, header=None, dtype=str)
        else:
            return None
    except Exception as e:
        log_lines.append(f"  ERROR reading {fpath.name}: {e}")
        return None

    # Detect date range
    date_str, fy, qtr = _detect_date_range(df)
    if fy is None:
        # Try to infer from filename
        m = re.search(r"fy(\d{4})", fpath.name.lower())
        if m:
            fy = int(m.group(1))
        m2 = re.search(r"q(?:tr)?(\d)", fpath.name.lower())
        if m2:
            qtr = int(m2.group(1))
        if fy is None:
            log_lines.append(f"  SKIP {fpath.name}: cannot determine FY/quarter")
            return None

    # Dynamically find Employment-based column block
    eb_start = _find_eb_columns(df)
    if eb_start is None:
        log_lines.append(f"  SKIP {fpath.name}: no 'Employment' header found (cols={df.shape[1]})")
        return None

    # Find total row
    total_idx = _find_total_row(df)
    if total_idx is None:
        log_lines.append(f"  SKIP {fpath.name}: no 'Total' row found")
        return None

    total_row = df.iloc[total_idx]

    # Extract EB metrics from the detected column positions
    record = {
        "fiscal_year": fy,
        "quarter": qtr,
        "reporting_period": f"FY{fy} Q{qtr}",
        "form_type": "I-485",
        "category": "Employment-based",
    }

    eb_metrics = {
        "eb_received": eb_start,
        "eb_approved": eb_start + 1,
        "eb_denied": eb_start + 2,
        "eb_pending": eb_start + 3,
    }

    for metric, col_idx in eb_metrics.items():
        if col_idx < len(total_row):
            record[metric] = _parse_number(total_row.iloc[col_idx])
        else:
            record[metric] = np.nan

    # Try to find Total category columns
    total_cat_start = _find_total_columns(df)
    if total_cat_start is not None:
        total_metrics = {
            "total_received": total_cat_start,
            "total_approved": total_cat_start + 1,
            "total_denied": total_cat_start + 2,
            "total_pending": total_cat_start + 3,
        }
        for metric, col_idx in total_metrics.items():
            if col_idx < len(total_row):
                record[metric] = _parse_number(total_row.iloc[col_idx])
            else:
                record[metric] = np.nan
    else:
        for metric in ["total_received", "total_approved", "total_denied", "total_pending"]:
            record[metric] = np.nan

    log_lines.append(
        f"  OK {fpath.name}: FY{fy} Q{qtr} cols={df.shape[1]} eb_start={eb_start} "
        f"EB recv={record.get('eb_received', '?')} appr={record.get('eb_approved', '?')} pend={record.get('eb_pending', '?')}"
    )
    return record


def build_processing_times_trends(log_lines: list) -> pd.DataFrame:
    """Scan all I-485 performance data files and produce processing metrics."""

    if not USCIS_EB.exists():
        log_lines.append(f"WARN: Source directory not found: {USCIS_EB}")
        return _empty_frame()

    # Find all performance data files
    patterns = ["**/[Ii]485_performancedata_*", "**/i485_performancedata_*"]
    all_files = set()
    for pattern in patterns:
        all_files.update(USCIS_EB.glob(pattern))

    all_files = sorted(all_files)
    log_lines.append(f"Found {len(all_files)} I-485 performance data files")

    records = []
    for fpath in all_files:
        record = _parse_single_file(fpath, log_lines)
        if record is not None:
            records.append(record)

    if not records:
        log_lines.append("WARN: no records extracted")
        return _empty_frame()

    df = pd.DataFrame(records)

    # Deduplicate: keep one record per (fiscal_year, quarter)
    df = df.sort_values(["fiscal_year", "quarter"]).drop_duplicates(
        subset=["fiscal_year", "quarter"], keep="last"
    ).reset_index(drop=True)

    # Derive processing metrics
    approved = df["eb_approved"].fillna(0)
    denied = df["eb_denied"].fillna(0)
    received = df["eb_received"].fillna(0)
    pending = df["eb_pending"].fillna(0)

    completions = approved + denied
    df["approval_rate"] = np.where(completions > 0, approved / completions, np.nan)
    df["throughput"] = completions
    df["net_intake"] = received - completions
    df["backlog_months"] = np.where(
        completions > 0,
        pending / (completions / 3.0),  # quarterly → monthly
        np.nan,
    )

    # Compute quarter-over-quarter changes
    df = df.sort_values(["fiscal_year", "quarter"]).reset_index(drop=True)
    df["pending_change"] = df["eb_pending"].diff()
    df["throughput_change"] = df["throughput"].diff()

    # Build a proper period_end_date for time series
    period_dates = []
    for _, row in df.iterrows():
        fy = int(row["fiscal_year"])
        qtr = int(row["quarter"])
        qtr_months = QUARTER_MONTHS.get(f"Q{qtr}", (9, 9))
        end_month = qtr_months[1]
        cal_year = fy if end_month <= 9 else fy - 1
        # Last day of the ending month
        if end_month == 12:
            end_date = date(cal_year, 12, 31)
        elif end_month in (3, 6, 9):
            import calendar
            end_date = date(cal_year, end_month, calendar.monthrange(cal_year, end_month)[1])
        else:
            end_date = date(cal_year, end_month, 28)
        period_dates.append(end_date)
    df["period_end_date"] = pd.to_datetime(period_dates)

    # Column ordering
    col_order = [
        "fiscal_year", "quarter", "reporting_period", "period_end_date",
        "form_type", "category",
        "eb_received", "eb_approved", "eb_denied", "eb_pending",
        "total_received", "total_approved", "total_denied", "total_pending",
        "approval_rate", "throughput", "net_intake",
        "backlog_months", "pending_change", "throughput_change",
    ]
    df = df[[c for c in col_order if c in df.columns]]

    log_lines.append(f"Output: {len(df)} quarterly records, FY{df['fiscal_year'].min()}-FY{df['fiscal_year'].max()}")
    return df


def _empty_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=[
        "fiscal_year", "quarter", "reporting_period", "period_end_date",
        "form_type", "category",
        "eb_received", "eb_approved", "eb_denied", "eb_pending",
        "total_received", "total_approved", "total_denied", "total_pending",
        "approval_rate", "throughput", "net_intake",
        "backlog_months", "pending_change", "throughput_change",
    ])


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log_lines = [f"=== processing_times_trends build {datetime.now(timezone.utc).isoformat()} ==="]

    df_out = build_processing_times_trends(log_lines)

    if not args.dry_run:
        OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp = OUT_PATH.parent / f".tmp_{ts}_processing_times_trends.parquet"
        df_out.to_parquet(tmp, index=False)
        if OUT_PATH.exists():
            OUT_PATH.unlink()
        tmp.rename(OUT_PATH)
        log_lines.append(f"Written: {OUT_PATH}")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"processing_times_trends: {len(df_out)} rows")


if __name__ == "__main__":
    main()
