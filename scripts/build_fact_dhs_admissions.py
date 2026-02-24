#!/usr/bin/env python3
"""
build_fact_dhs_admissions.py
Parse DHS_Yearbook → fact_dhs_admissions.parquet
Schema: fiscal_year, class_of_admission, country, admissions, source_file, ingested_at

Files: 1 XLSX (2025_0812_ohss_yearbook_refugees_fy2024.xlsx) with refugee arrival data.
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

NUMERIC_RE = re.compile(r"^[\d,\.\-\*]+$")


def safe_int(val) -> int:
    if pd.isna(val):
        return 0
    s = str(val).replace(",", "").replace("*", "").strip()
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def parse_table13(df_raw: pd.DataFrame, fiscal_year: str, fname: str) -> pd.DataFrame:
    """
    Table 13: Refugee Arrivals FY1980-2024
    Typically: Year | Total | Africa | East Asia | Europe | ... | Unknown
    """
    now_ts = datetime.now(timezone.utc).isoformat()
    rows_out = []

    # Find header row
    h_idx = 0
    for i in range(min(10, len(df_raw))):
        row_strs = [str(c).strip().lower() for c in df_raw.iloc[i].tolist() if pd.notna(c)]
        if any(k in " ".join(row_strs) for k in ["fiscal year", "total", "africa", "europe"]):
            h_idx = i
            break

    header = [str(c).strip() if pd.notna(c) else f"col_{j}" for j, c in enumerate(df_raw.iloc[h_idx].tolist())]
    data = df_raw.iloc[h_idx + 1:].reset_index(drop=True)
    data.columns = header

    # First col is year, remaining are regions/countries
    year_col = header[0]
    region_cols = header[1:]

    for _, row in data.iterrows():
        fy_raw = str(row[year_col]).strip()
        yr_m = re.search(r"(\d{4})", fy_raw)
        if not yr_m:
            continue
        fy = f"FY{yr_m.group(1)}"

        for reg in region_cols:
            if not reg or reg.lower() in ("nan", ""):
                continue
            val = row.get(reg, 0)
            admissions = safe_int(val)
            if admissions <= 0:
                continue
            rows_out.append({
                "fiscal_year": fy,
                "class_of_admission": "REFUGEE",
                "country": reg,
                "admissions": admissions,
                "source_file": fname,
                "ingested_at": now_ts,
            })

    return pd.DataFrame(rows_out)


def parse_table14(df_raw: pd.DataFrame, fname: str) -> pd.DataFrame:
    """
    Table 14: Refugee Arrivals by Region and Country of Nationality FY2015-2024
    Typically: Country | FY2015 | FY2016 | ... | FY2024
    """
    now_ts = datetime.now(timezone.utc).isoformat()
    rows_out = []

    h_idx = 0
    for i in range(min(10, len(df_raw))):
        row_strs = [str(c).strip().lower() for c in df_raw.iloc[i].tolist() if pd.notna(c)]
        if any(re.search(r"20\d{2}", c) for c in row_strs):
            h_idx = i
            break

    header = [str(c).strip() if pd.notna(c) else f"col_{j}" for j, c in enumerate(df_raw.iloc[h_idx].tolist())]
    data = df_raw.iloc[h_idx + 1:].reset_index(drop=True)
    data.columns = header

    country_col = header[0]
    fy_cols = [h for h in header[1:] if re.search(r"20\d{2}", str(h))]

    for _, row in data.iterrows():
        country = str(row[country_col]).strip()
        if not country or country.lower() in ("nan", "", "total"):
            continue

        for fy_col in fy_cols:
            yr_m = re.search(r"(\d{4})", str(fy_col))
            if not yr_m:
                continue
            fy = f"FY{yr_m.group(1)}"
            admissions = safe_int(row.get(fy_col, 0))
            if admissions <= 0:
                continue
            rows_out.append({
                "fiscal_year": fy,
                "class_of_admission": "REFUGEE",
                "country": country,
                "admissions": admissions,
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

    xlsx_files = sorted(droot.rglob("*.xlsx")) + sorted(droot.rglob("*.xls"))
    log.info("Found %d XLSX files in %s", len(xlsx_files), droot)

    all_frames = []
    parsed_ok = 0

    for f in xlsx_files:
        fname = f.name
        log.info("Parsing: %s", fname)
        try:
            xl = pd.ExcelFile(f)
        except Exception as e:
            log.warning("Cannot open %s: %s", fname, e)
            continue

        for sheet in xl.sheet_names:
            if sheet.strip() == "TOC":
                continue
            try:
                df_raw = pd.read_excel(xl, sheet_name=sheet, header=None, dtype=str)
            except Exception:
                continue

            sheet_l = sheet.lower().strip()
            if "table 13" in sheet_l or "13" == sheet_l:
                df_out = parse_table13(df_raw, "FY2024", fname)
            elif "table 14" in sheet_l or "14" == sheet_l:
                df_out = parse_table14(df_raw, fname)
            else:
                # Generic: try table14 style
                df_out = parse_table14(df_raw, fname)

            if len(df_out) > 0:
                log.info("  Sheet '%s' → %d rows", sheet, len(df_out))
                all_frames.append(df_out)

        parsed_ok += 1

    log.info("Parsed %d/%d files", parsed_ok, len(xlsx_files))

    if not all_frames:
        log.warning("No data extracted; creating empty parquet")
        df = pd.DataFrame(columns=["fiscal_year", "class_of_admission", "country", "admissions", "source_file", "ingested_at"])
    else:
        df = pd.concat(all_frames, ignore_index=True)

    df["admissions"] = pd.to_numeric(df["admissions"], errors="coerce").fillna(0).astype(int)
    df = df[df["admissions"] > 0]

    pk = ["fiscal_year", "class_of_admission", "country"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df.sort_values(pk).reset_index(drop=True)
    df = df[["fiscal_year", "class_of_admission", "country", "admissions", "source_file", "ingested_at"]]

    dup_count = df.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.warning("PK not unique: %d duplicates", dup_count)

    df.to_parquet(out_path, index=False)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_dhs_admissions: %d rows", len(df))
    fy_range = sorted(df["fiscal_year"].unique()) if len(df) > 0 else []
    log.info("FY range: %s", fy_range)


if __name__ == "__main__":
    main()
