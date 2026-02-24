#!/usr/bin/env python3
"""
build_fact_waiting_list.py
Parse DOS_Waiting_List → fact_waiting_list.parquet
Schema: report_year, category, country, count_waiting, source_file, ingested_at

Files present:
  - waiting_list_2023.csv  (239 B, 9 rows of stub/test data)
  - waiting_list_2023.pdf  (DOS Immigrant Numbers for Waiting List report)
"""
import argparse
import logging
import pathlib
import re
import sys
from datetime import datetime, timezone

import pandas as pd
import pdfplumber

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

CATEGORY_ALIASES = {
    # Family
    "1st": "F1", "f1": "F1", "first": "F1",
    "2a": "F2A", "f2a": "F2A",
    "2b": "F2B", "f2b": "F2B",
    "3rd": "F3", "f3": "F3", "third": "F3",
    "4th": "F4", "f4": "F4", "fourth": "F4",
    # Employment
    "1st preference": "EB1", "eb1": "EB1",
    "2nd preference": "EB2", "eb2": "EB2",
    "3rd preference": "EB3", "eb3": "EB3",
    "4th preference": "EB4", "eb4": "EB4",
    "5th preference": "EB5", "eb5": "EB5",
}


def normalize_category(raw: str) -> str:
    r = str(raw).strip().lower()
    for k, v in CATEGORY_ALIASES.items():
        if r == k or r.startswith(k):
            return v
    # Return cleaned upper
    return str(raw).strip().upper()


def parse_csv(csv_path: pathlib.Path) -> pd.DataFrame:
    """Parse a waiting-list CSV file."""
    fy_m = re.search(r"(\d{4})", csv_path.parent.name)
    default_year = int(fy_m.group(1)) if fy_m else 0

    df = pd.read_csv(csv_path)
    log.info("  CSV columns: %s", list(df.columns))

    rename_map = {}
    col_lower = {c.lower().strip(): c for c in df.columns}
    if "fiscal_year" in col_lower or "year" in col_lower:
        rename_map[col_lower.get("fiscal_year") or col_lower.get("year")] = "report_year"
    if "category" in col_lower:
        rename_map[col_lower["category"]] = "category"
    if "country" in col_lower or "nationality" in col_lower:
        rename_map[col_lower.get("country") or col_lower.get("nationality")] = "country"
    for cnt_key in ["count", "count_waiting", "waiting", "total", "number"]:
        if cnt_key in col_lower:
            rename_map[col_lower[cnt_key]] = "count_waiting"
            break

    if rename_map:
        df = df.rename(columns=rename_map)

    if "report_year" not in df.columns:
        df["report_year"] = default_year
    if "country" not in df.columns:
        df["country"] = "All Countries"
    if "count_waiting" not in df.columns:
        # try numeric detection
        num_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if num_cols:
            df["count_waiting"] = df[num_cols[-1]]
        else:
            df["count_waiting"] = 0

    if "category" in df.columns:
        df["category"] = df["category"].apply(normalize_category)

    df["source_file"] = csv_path.name
    df["ingested_at"] = datetime.now(timezone.utc).isoformat()

    return df[["report_year", "category", "country", "count_waiting", "source_file", "ingested_at"]]


def parse_pdf_waiting_list(pdf_path: pathlib.Path) -> pd.DataFrame:
    """Extract waiting list tables from DOS PDF."""
    rows = []
    fy_m = re.search(r"(\d{4})", pdf_path.parent.name)
    report_year = int(fy_m.group(1)) if fy_m else 0
    fname = pdf_path.name

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for tbl in tables:
                    if not tbl or len(tbl) < 2:
                        continue
                    # Best-effort: look for country/category/count pattern
                    for row in tbl[1:]:
                        if not row:
                            continue
                        cells = [str(c).strip() if c else "" for c in row]
                        if len(cells) < 2:
                            continue
                        # Try to extract a count from the last non-empty cell
                        count_str = ""
                        category = ""
                        country = cells[0] if cells[0] else "Unknown"
                        for c in reversed(cells):
                            c_clean = c.replace(",", "").replace("*", "").replace("(", "").replace(")", "")
                            try:
                                count_val = int(c_clean)
                                count_str = count_val
                                break
                            except ValueError:
                                pass
                        if count_str == "":
                            continue
                        # Category from second column
                        category = normalize_category(cells[1]) if len(cells) > 1 else "UNKNOWN"
                        rows.append({
                            "report_year": report_year,
                            "category": category,
                            "country": country,
                            "count_waiting": count_str,
                            "source_file": fname,
                            "ingested_at": datetime.now(timezone.utc).isoformat(),
                        })
    except Exception as e:
        log.warning("PDF parse error %s: %s", pdf_path, e)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--downloads", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_frames = []

    csvs = sorted(droot.rglob("*.csv"))
    pdfs = sorted(droot.rglob("*.pdf"))
    log.info("Found %d CSV(s), %d PDF(s)", len(csvs), len(pdfs))

    for csv_path in csvs:
        log.info("Parsing CSV: %s", csv_path.name)
        try:
            df = parse_csv(csv_path)
            if len(df) > 0:
                log.info("  %d rows", len(df))
                all_frames.append(df)
        except Exception as e:
            log.warning("  CSV error: %s", e)

    for pdf_path in pdfs:
        log.info("Parsing PDF: %s", pdf_path.name)
        df = parse_pdf_waiting_list(pdf_path)
        if len(df) > 0:
            log.info("  %d rows from PDF", len(df))
            all_frames.append(df)
        else:
            log.warning("  No data extracted from PDF; skipping")

    if not all_frames:
        log.error("No data parsed from any source in %s", droot)
        sys.exit(1)

    df = pd.concat(all_frames, ignore_index=True)
    df["count_waiting"] = pd.to_numeric(df["count_waiting"], errors="coerce").fillna(0).astype(int)
    df["report_year"] = pd.to_numeric(df["report_year"], errors="coerce").fillna(0).astype(int)
    df = df[df["category"].notna() & (df["category"] != "")]
    df = df.dropna(subset=["report_year", "category", "country"])

    pk = ["report_year", "category", "country"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df.sort_values(pk).reset_index(drop=True)
    df = df[["report_year", "category", "country", "count_waiting", "source_file", "ingested_at"]]

    dup_count = df.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.error("PK not unique: %d duplicate rows", dup_count)
        sys.exit(1)

    df.to_parquet(out_path, index=False)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_waiting_list: %d rows", len(df))


if __name__ == "__main__":
    main()
