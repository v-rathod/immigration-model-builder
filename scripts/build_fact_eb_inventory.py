#!/usr/bin/env python3
"""
Build fact_eb_inventory.parquet from USCIS Employment-Based I-485 Pending
Inventory Excel files (P1 Horizon downloads).

Each Excel file is a monthly snapshot of pending I-485 applications by:
  - country of chargeability (ROW, China, India, Mexico, Philippines)
  - EB preference category (EB1-EB5, EW3, CRW, EB4)
  - visa status (Available, Awaiting Availability)
  - priority date month x year grid

Values: integer counts, "D" (suppressed <10), "-" (zero).

Output schema:
  snapshot_date   DATE     -- "As of ..." from each file
  country         TEXT     -- CHN / IND / MEX / PHL / ROW
  category        TEXT     -- EB1 / EB2 / EB3 / EB4 / EB5 / EW3 / CRW
  visa_status     TEXT     -- Available / Awaiting Availability
  pd_month        INT      -- 1-12
  pd_year         INT      -- actual year or 0 for "Prior Years"
  pending_count   INT      -- pending I-485 count (D mapped to 5, - to 0)

Usage:
    python3 scripts/build_fact_eb_inventory.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from datetime import datetime

import openpyxl
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

P1_ROOT = Path(__file__).resolve().parent.parent.parent / "fetch-immigration-data"
EB_DIR = P1_ROOT / "downloads" / "USCIS_IMMIGRATION" / "employment_based"
OUT_PATH = Path(__file__).resolve().parent.parent / "artifacts" / "tables" / "fact_eb_inventory.parquet"

MONTH_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

# Map sheet country names to canonical codes
COUNTRY_MAP = {
    "rest of the world": "ROW",
    "china": "CHN",
    "india": "IND",
    "mexico": "MEX",
    "philippines": "PHL",
}

# Extract EB category from long descriptions
CATEGORY_RE = re.compile(
    r"(Employment-Based\s+)?(\d+)\w*\s+Preference\s+Category\s*\(?(EB\d)\)?",
    re.IGNORECASE,
)
# Special categories
SPECIAL_CAT_RE = re.compile(
    r"(EW[- ]?3|EB[- ]?4|CRW|EB[- ]?5|Certain Religious Workers)",
    re.IGNORECASE,
)


def _parse_category(raw: str) -> str:
    """Normalize preference category string to short code (EB1, EB2, etc.)."""
    if not raw:
        return "UNKNOWN"
    m = CATEGORY_RE.search(raw)
    if m:
        return m.group(3).upper()
    raw_upper = raw.upper().strip()
    for code in ("EB1", "EB2", "EB3", "EB4", "EB5", "EW3", "CRW"):
        if code in raw_upper:
            return code
    if "CERTAIN RELIGIOUS" in raw_upper:
        return "CRW"
    return raw_upper[:20]


def _parse_snapshot_date(text: str) -> str | None:
    """Extract date from 'As of October 2, 2025' style strings."""
    if not text:
        return None
    m = re.search(r"As of\s+(\w+\s+\d{1,2},?\s+\d{4})", text, re.IGNORECASE)
    if m:
        raw = m.group(1).replace(",", "")
        for fmt in ("%B %d %Y", "%b %d %Y"):
            try:
                return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    return None


def _cell_to_int(val) -> int:
    """Convert cell value to integer count. D->5 (midpoint), - or None->0."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val) if val == val else 0  # NaN check
    s = str(val).strip()
    if s in ("D", "d"):
        return 5  # suppressed <10, use midpoint estimate
    if s in ("-", "", "nan", "NaN"):
        return 0
    try:
        return int(float(s.replace(",", "")))
    except (ValueError, TypeError):
        return 0


def _infer_country_from_sheet(sheet_name: str) -> str:
    """Map sheet name to country code."""
    sn = sheet_name.lower().strip()
    for pattern, code in COUNTRY_MAP.items():
        if pattern in sn:
            return code
    if "india" in sn:
        return "IND"
    return "UNKNOWN"


def parse_one_xlsx(filepath: Path) -> list[dict]:
    """Parse a single eb_inventory Excel file into flat records."""
    wb = openpyxl.load_workbook(filepath, data_only=True, read_only=True)
    records = []

    for sheet_name in wb.sheetnames:
        if "how to read" in sheet_name.lower():
            continue

        ws = wb[sheet_name]
        rows = list(ws.rows)
        if len(rows) < 5:
            continue

        country = _infer_country_from_sheet(sheet_name)

        # Parse snapshot date from row 3 (0-indexed: row 2)
        snapshot_date = None
        for r_idx in range(min(5, len(rows))):
            cell_val = rows[r_idx][0].value
            if cell_val and "as of" in str(cell_val).lower():
                snapshot_date = _parse_snapshot_date(str(cell_val))
                break

        # Find header row (has "Country Of Chargeability" or "Priority Date Month")
        header_idx = None
        for r_idx in range(min(10, len(rows))):
            first_cell = str(rows[r_idx][0].value or "").lower()
            if "country" in first_cell or "chargeability" in first_cell:
                header_idx = r_idx
                break

        if header_idx is None:
            continue

        # Parse year columns from header
        header_cells = [c.value for c in rows[header_idx]]
        year_columns = {}  # col_idx -> year_int (0 means "Prior Years")
        for ci, hv in enumerate(header_cells):
            if hv is None:
                continue
            hs = str(hv).strip()
            if "Prior Years" in hs:
                year_columns[ci] = 0
            else:
                m = re.search(r"(\d{4})", hs)
                if m and ci >= 4:
                    year_columns[ci] = int(m.group(1))

        if not year_columns:
            continue

        # Parse data rows
        for r_idx in range(header_idx + 1, len(rows)):
            cells = [c.value for c in rows[r_idx]]
            if not cells or not cells[1]:
                continue

            category = _parse_category(str(cells[1] or ""))
            visa_status = str(cells[2] or "").strip()
            pd_month_str = str(cells[3] or "").strip().lower()
            pd_month = MONTH_MAP.get(pd_month_str)

            if not pd_month or not visa_status:
                continue

            for ci, pd_year in year_columns.items():
                if ci >= len(cells):
                    continue
                count = _cell_to_int(cells[ci])
                records.append({
                    "snapshot_date": snapshot_date,
                    "country": country,
                    "category": category,
                    "visa_status": visa_status,
                    "pd_month": pd_month,
                    "pd_year": pd_year,
                    "pending_count": count,
                })

    wb.close()
    return records


def _snapshot_from_filename(filepath: Path) -> str | None:
    """Guess snapshot month from filename like eb_inventory_october_2025.xlsx."""
    stem = filepath.stem.lower()
    m = re.search(r"eb_inventory_(\w+)_?(\d{4})?", stem)
    if m:
        month_name = m.group(1)
        year = m.group(2)
        mo = MONTH_MAP.get(month_name)
        if mo and year:
            return f"{year}-{mo:02d}-01"
    return None


def main():
    # Find all xlsx eb_inventory files (skip the 2012 CSV and "How to Read" only files)
    files = sorted(EB_DIR.rglob("eb_inventory*.xlsx"))
    if not files:
        print("ERROR: No eb_inventory xlsx files found in P1 downloads", file=sys.stderr)
        sys.exit(1)

    print(f"Found {len(files)} eb_inventory files")

    all_records = []
    for fp in files:
        try:
            recs = parse_one_xlsx(fp)
            print(f"  {fp.name}: {len(recs):,} records")
            # Backfill snapshot_date from filename if not parsed from header
            if recs and recs[0].get("snapshot_date") is None:
                fallback = _snapshot_from_filename(fp)
                for r in recs:
                    if r["snapshot_date"] is None:
                        r["snapshot_date"] = fallback
            all_records.extend(recs)
        except Exception as e:
            print(f"  WARNING: {fp.name}: {e}", file=sys.stderr)

    if not all_records:
        print("ERROR: No records parsed from any file", file=sys.stderr)
        sys.exit(1)

    df = pd.DataFrame(all_records)

    # Type enforcement
    df["snapshot_date"] = pd.to_datetime(df["snapshot_date"], errors="coerce")
    df["pd_month"] = df["pd_month"].astype("Int32")
    df["pd_year"] = df["pd_year"].astype("Int32")
    df["pending_count"] = df["pending_count"].astype("Int32")

    # Drop rows with no snapshot_date (unparseable files)
    before = len(df)
    df = df.dropna(subset=["snapshot_date"])
    if len(df) < before:
        print(f"  Dropped {before - len(df)} rows with no snapshot_date")

    # Drop UNKNOWN country rows
    df = df[df["country"] != "UNKNOWN"].reset_index(drop=True)

    # Deduplicate on PK columns (can arise from overlapping sheet parsing)
    pk_cols = ["snapshot_date", "country", "category", "visa_status", "pd_month", "pd_year"]
    before = len(df)
    df = df.drop_duplicates(subset=pk_cols, keep="first")
    if len(df) < before:
        print(f"  Deduped {before - len(df)} duplicate PK rows")

    # Sort
    df = df.sort_values(
        ["snapshot_date", "country", "category", "visa_status", "pd_year", "pd_month"]
    ).reset_index(drop=True)

    # Save
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"\nWrote {len(df):,} rows x {len(df.columns)} cols -> {OUT_PATH}")
    print(f"Snapshot dates: {df['snapshot_date'].dt.strftime('%Y-%m').unique().tolist()}")
    print(f"Countries: {sorted(df['country'].unique())}")
    print(f"Categories: {sorted(df['category'].unique())}")


if __name__ == "__main__":
    main()
