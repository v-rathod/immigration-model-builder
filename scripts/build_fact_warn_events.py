#!/usr/bin/env python3
"""
build_fact_warn_events.py
Parse WARN Act notices → fact_warn_events.parquet
Schema: state, notice_date, employer_name_raw, city, employees_affected,
        employer_id (nullable), source_file, ingested_at

Files:
  - WARN/CA/WARN_Report.xlsx  (sheet: "Detailed WARN Report ")
  - WARN/TX/warn-act-listings-2026-twc.xlsx  (sheet: Sheet1, clean columnar)
"""
import argparse
import logging
import pathlib
import re
import sys
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def safe_date(val) -> str:
    if pd.isna(val):
        return None
    if isinstance(val, (pd.Timestamp, datetime)):
        return str(val.date())
    s = str(val).strip()
    try:
        return str(pd.to_datetime(s).date())
    except Exception:
        return s


def parse_ca_warn(path: pathlib.Path) -> pd.DataFrame:
    """Parse California WARN Report XLSX."""
    fname = path.name
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        xl = pd.ExcelFile(path)
        # Find the detailed sheet
        detail_sheet = None
        for s in xl.sheet_names:
            if "detail" in s.lower():
                detail_sheet = s
                break
        if not detail_sheet:
            return pd.DataFrame()

        df_raw = pd.read_excel(xl, sheet_name=detail_sheet, header=None, dtype=str)
    except Exception as e:
        log.warning("CA WARN error: %s", e)
        return pd.DataFrame()

    # Header row detection: skip row 0 (description text), look for row with 5+ non-null cells
    h_idx = 1  # default: row 1 is the header
    for i in range(1, min(10, len(df_raw))):
        row_vals = [c for c in df_raw.iloc[i].tolist() if pd.notna(c) and str(c).strip() not in ("", "nan")]
        row_strs = [str(c).replace("\n", " ").strip().lower() for c in row_vals]
        # Genuine header: multiple fields that are short strings (< 30 chars each)
        short_fields = [c for c in row_strs if len(c) < 30]
        kw_hits = sum(1 for c in short_fields if any(k in c for k in
                       ["county", "company", "employer", "closure", "layoff", "employees", "no.", "notice", "date"]))
        if len(short_fields) >= 4 and kw_hits >= 2:
            h_idx = i
            break

    header = [str(c).replace("\n", " ").strip() if pd.notna(c) and str(c).strip() != "nan" else f"col_{j}"
              for j, c in enumerate(df_raw.iloc[h_idx].tolist())]
    data = df_raw.iloc[h_idx + 1:].reset_index(drop=True)
    data.columns = header

    # Map CA columns (headers contain newlines already stripped above)
    col_map = {}
    for c in header:
        cl = c.lower()
        if "county" in cl or "parish" in cl:
            col_map[c] = "city"
        elif "company" in cl or "employer" in cl and "city" not in col_map.values():
            col_map[c] = "employer_name_raw"
        elif "notice" in cl:
            col_map[c] = "notice_date"
        elif "address" in cl:
            col_map[c] = "address"
        elif "no." in cl or "employee" in cl or "workers" in cl:
            col_map[c] = "employees_affected"
        elif "layoff" in cl or "closure" in cl:
            col_map[c] = "layoff_type"

    data = data.rename(columns=col_map)
    data["state"] = "CA"

    rows_out = []
    for _, row in data.iterrows():
        employer = str(row.get("employer_name_raw", "")).strip()
        if not employer or employer.lower() in ("nan", ""):
            continue
        notice_date = safe_date(row.get("notice_date"))
        city = str(row.get("city", "")).strip()
        emp_affected = row.get("employees_affected", 0)
        try:
            emp_affected = int(float(str(emp_affected).replace(",", "")))
        except (ValueError, TypeError):
            emp_affected = 0

        rows_out.append({
            "state": "CA",
            "notice_date": notice_date,
            "employer_name_raw": employer,
            "city": city,
            "employees_affected": max(0, emp_affected),
            "employer_id": None,
            "source_file": fname,
            "ingested_at": now_ts,
        })

    return pd.DataFrame(rows_out)


def parse_tx_warn(path: pathlib.Path) -> pd.DataFrame:
    """Parse Texas WARN Act XLSX."""
    fname = path.name
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        df = pd.read_excel(path, sheet_name=0, dtype=str)
    except Exception as e:
        log.warning("TX WARN error: %s", e)
        return pd.DataFrame()

    col_map = {}
    for c in df.columns:
        cl = c.lower()
        if "notice_date" in cl or "notice" in cl:
            col_map[c] = "notice_date"
        elif "job_site" in cl or "company" in cl or "employer" in cl:
            col_map[c] = "employer_name_raw"
        elif "city" in cl:
            col_map[c] = "city"
        elif "county" in cl:
            col_map[c] = "city"
        elif "total_layoff" in cl or "employees" in cl or "layoff_number" in cl:
            col_map[c] = "employees_affected"

    df = df.rename(columns=col_map)

    rows_out = []
    for _, row in df.iterrows():
        employer = str(row.get("employer_name_raw", "")).strip()
        if not employer or employer.lower() in ("nan", ""):
            continue
        notice_date = safe_date(row.get("notice_date"))
        city = str(row.get("city", "")).strip()
        emp_affected = row.get("employees_affected", 0)
        try:
            emp_affected = int(float(str(emp_affected).replace(",", "")))
        except (ValueError, TypeError):
            emp_affected = 0

        rows_out.append({
            "state": "TX",
            "notice_date": notice_date,
            "employer_name_raw": employer,
            "city": city,
            "employees_affected": max(0, emp_affected),
            "employer_id": None,
            "source_file": fname,
            "ingested_at": now_ts,
        })

    return pd.DataFrame(rows_out)


def parse_generic_warn(path: pathlib.Path, state: str) -> pd.DataFrame:
    """Generic parser for unknown WARN formats."""
    fname = path.name
    now_ts = datetime.now(timezone.utc).isoformat()

    suffix = path.suffix.lower()
    try:
        if suffix in (".xlsx", ".xls"):
            xl = pd.ExcelFile(path)
            df = pd.read_excel(xl, sheet_name=xl.sheet_names[0], dtype=str)
        elif suffix == ".csv":
            df = pd.read_csv(path, dtype=str)
        else:
            return pd.DataFrame()
    except Exception as e:
        log.warning("Generic WARN parse error %s: %s", fname, e)
        return pd.DataFrame()

    # Auto-detect columns
    col_map = {}
    for c in df.columns:
        cl = str(c).lower()
        if any(k in cl for k in ["company", "employer", "establishment", "firm"]):
            col_map[c] = "employer_name_raw"
        elif any(k in cl for k in ["notice", "date", "received"]):
            col_map[c] = "notice_date"
        elif any(k in cl for k in ["city", "location", "county"]):
            col_map[c] = "city"
        elif any(k in cl for k in ["employee", "worker", "affected", "layoff_number"]):
            col_map[c] = "employees_affected"

    df = df.rename(columns=col_map)

    rows_out = []
    for _, row in df.iterrows():
        employer = str(row.get("employer_name_raw", "")).strip()
        if not employer or employer.lower() in ("nan", ""):
            continue
        rows_out.append({
            "state": state,
            "notice_date": safe_date(row.get("notice_date")),
            "employer_name_raw": employer,
            "city": str(row.get("city", "")).strip(),
            "employees_affected": max(0, int(float(str(row.get("employees_affected", 0)).replace(",", "") or "0")) if True else 0),
            "employer_id": None,
            "source_file": fname,
            "ingested_at": now_ts,
        })

    return pd.DataFrame(rows_out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--downloads", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_files = sorted(droot.rglob("*.xlsx")) + sorted(droot.rglob("*.xls")) + sorted(droot.rglob("*.csv"))
    log.info("Found %d files in %s", len(all_files), droot)

    all_frames = []
    parsed_ok = 0

    for f in all_files:
        log.info("Parsing: %s", f.relative_to(droot))
        state = f.parent.name.upper() if len(f.parent.name) == 2 else "UNKNOWN"

        # Use specialized parsers for known states
        if state == "CA":
            df = parse_ca_warn(f)
        elif state == "TX":
            df = parse_tx_warn(f)
        else:
            df = parse_generic_warn(f, state)

        if len(df) > 0:
            log.info("  %d rows from %s", len(df), f.name)
            all_frames.append(df)
            parsed_ok += 1
        else:
            log.warning("  No rows from %s", f.name)

    log.info("Parsed %d/%d files", parsed_ok, len(all_files))

    if not all_frames:
        log.warning("No WARN data; creating empty parquet")
        df = pd.DataFrame(columns=["state", "notice_date", "employer_name_raw", "city",
                                    "employees_affected", "employer_id", "source_file", "ingested_at"])
    else:
        df = pd.concat(all_frames, ignore_index=True)

    df["employees_affected"] = pd.to_numeric(df["employees_affected"], errors="coerce").fillna(0).astype(int)
    df = df.dropna(subset=["employer_name_raw"])
    df = df[df["employer_name_raw"] != ""]

    pk = ["state", "notice_date", "employer_name_raw", "city"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df.sort_values(["state", "notice_date"]).reset_index(drop=True)
    df = df[["state", "notice_date", "employer_name_raw", "city", "employees_affected",
             "employer_id", "source_file", "ingested_at"]]

    dup_count = df.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.warning("PK not unique after dedup: %d remaining", dup_count)

    df.to_parquet(out_path, index=False)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_warn_events: %d rows across states: %s",
             len(df), sorted(df["state"].unique()) if len(df) > 0 else [])


if __name__ == "__main__":
    main()
