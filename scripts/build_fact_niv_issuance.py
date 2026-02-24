#!/usr/bin/env python3
"""
build_fact_niv_issuance.py
Parse NIV_Statistics → fact_niv_issuance.parquet
Schema: fiscal_year, visa_class, country, issued, source_file, ingested_at

Files: ~32 files — XLS/XLSX detail tables (country × visa_class wide format)
       + PDF workload files (skip; XLS is primary source)
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


def extract_fy(fname: str) -> str:
    """Extract fiscal year from NIV filename like FY17%20NIV%20Detail%20Table.xlsx"""
    fname_dec = fname.replace("%20", " ")
    m = re.search(r"FY(\d{2,4})", fname_dec, re.IGNORECASE)
    if m:
        yr = int(m.group(1))
        if yr < 100:
            yr = 2000 + yr
        return f"FY{yr}"
    return "FY_UNKNOWN"


def fy_from_sheet(sheet_name: str) -> str:
    """Derive FY from a sheet name like 'FY97', 'FY2021', 'FY24'."""
    m = re.match(r"FY(\d{2,4})$", sheet_name.strip(), re.IGNORECASE)
    if m:
        yr = int(m.group(1))
        if yr < 100:
            yr = 2000 + yr if yr <= 30 else 1900 + yr
        return f"FY{yr}"
    return "FY_UNKNOWN"


def parse_niv_sheet(df_raw: pd.DataFrame, fiscal_year: str, fname: str) -> list[dict]:
    """Parse one NIV wide-format sheet: country × visa_class → rows."""
    if df_raw is None or df_raw.empty or len(df_raw) < 3:
        return []
    # Find header row: row with ≥5 non-null string values in visa-class columns
    visa_class_row = 0
    for i in range(min(5, len(df_raw))):
        row = df_raw.iloc[i].tolist()
        str_count = sum(1 for c in row[1:] if pd.notna(c) and str(c).strip())
        if str_count >= 5:
            visa_class_row = i
            break

    header = df_raw.iloc[visa_class_row].tolist()
    visa_classes = [str(h).strip() if pd.notna(h) else "" for h in header[1:]]

    now_ts = datetime.now(timezone.utc).isoformat()
    rows_out = []

    for row_idx in range(visa_class_row + 1, len(df_raw)):
        row = df_raw.iloc[row_idx].tolist()
        country = str(row[0]).strip() if pd.notna(row[0]) else ""
        if not country or country.lower() in ("nan", "", "grand total", "total", "region"):
            continue
        values = row[1:]
        if all(pd.isna(v) for v in values):
            continue

        for j, vc in enumerate(visa_classes):
            if not vc or vc.lower() in ("nan", ""):
                continue
            val = values[j] if j < len(values) else None
            if pd.isna(val):
                continue
            try:
                issued = int(float(str(val).replace(",", "")))
            except (ValueError, TypeError):
                continue
            if issued < 0:
                continue
            rows_out.append({
                "fiscal_year": fiscal_year,
                "visa_class": vc,
                "country": country,
                "issued": issued,
                "source_file": fname,
                "ingested_at": now_ts,
            })
    return rows_out


def parse_niv_xls(xls_path: pathlib.Path) -> pd.DataFrame:
    """Parse NIV detail table: wide format country × visa_class.
    
    Supports both single-year files (one sheet) and multi-year files where
    each sheet name is 'FY97', 'FY2021', etc.
    """
    fname = xls_path.name
    fiscal_year_from_name = extract_fy(fname)

    try:
        xl = pd.ExcelFile(xls_path)
        sheet_names = xl.sheet_names
    except Exception as e:
        log.warning("Cannot open %s: %s", fname, e)
        return pd.DataFrame()

    all_rows = []

    for sheet in sheet_names:
        # Derive FY: prefer sheet name if it looks like 'FY97' etc.; fallback to filename
        sheet_fy = fy_from_sheet(sheet)
        use_fy = sheet_fy if sheet_fy != "FY_UNKNOWN" else fiscal_year_from_name

        try:
            df_raw = pd.read_excel(xl, sheet_name=sheet, header=None)
        except Exception as e:
            log.debug("  Skip sheet %s in %s: %s", sheet, fname, e)
            continue

        rows = parse_niv_sheet(df_raw, use_fy, fname)
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows)
    log.info("  %s → %d rows (%d sheets)", fname, len(df), len(sheet_names))
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--downloads", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    xls_files = sorted(droot.rglob("*.xls")) + sorted(droot.rglob("*.xlsx"))
    log.info("Found %d XLS/XLSX files in %s", len(xls_files), droot)

    all_frames = []
    parsed_ok = 0
    for f in xls_files:
        df = parse_niv_xls(f)
        if len(df) > 0:
            all_frames.append(df)
            parsed_ok += 1

    log.info("Parsed %d/%d files successfully", parsed_ok, len(xls_files))

    if not all_frames:
        log.warning("No data extracted; creating empty parquet")
        df = pd.DataFrame(columns=["fiscal_year", "visa_class", "country", "issued", "source_file", "ingested_at"])
    else:
        df = pd.concat(all_frames, ignore_index=True)

    df["issued"] = pd.to_numeric(df["issued"], errors="coerce").fillna(0).astype(int)
    df = df[df["issued"] >= 0]

    pk = ["fiscal_year", "visa_class", "country"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df.sort_values(pk).reset_index(drop=True)
    df = df[["fiscal_year", "visa_class", "country", "issued", "source_file", "ingested_at"]]

    dup_count = df.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.warning("PK not unique after dedup: %d remaining", dup_count)

    df.to_parquet(out_path, index=False)
    coverage_pct = 100.0 * parsed_ok / max(len(xls_files), 1)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_niv_issuance: %d rows, parse coverage=%.1f%%", len(df), coverage_pct)
    fy_range = sorted(df["fiscal_year"].unique()) if len(df) > 0 else []
    log.info("FY range: %s", fy_range)


if __name__ == "__main__":
    main()
