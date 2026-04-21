#!/usr/bin/env python3
"""
Build fact_i140_demand.parquet from USCIS I-140 Receipt/Status files
(P1 Horizon downloads).

Each Excel file (FY2024-2025) contains per-country sheets with I-140 petition
counts by fiscal year: Total Petitions, Approved, Denied, Pending for each
EB preference category. Older CSV files (FY2021-2023) have similar structure.

Output schema:
  report_period   TEXT     -- e.g. "FY2025_Q4" (file identifier)
  country         TEXT     -- ALL / IND / CHN / PHL / BRA / VNM / KOR
  category        TEXT     -- EB1 / EB2 / EB3 / TOTAL
  fiscal_year     INT      -- 2014-2025
  received        INT      -- total petitions received
  approved        INT      -- approved petitions
  denied          INT      -- denied petitions
  pending         INT      -- pending/other petitions

Usage:
    python3 scripts/build_fact_i140_demand.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import openpyxl
import pandas as pd

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

P1_ROOT = Path(__file__).resolve().parent.parent.parent / "fetch-immigration-data"
EB_DIR = P1_ROOT / "downloads" / "USCIS_IMMIGRATION" / "employment_based"
OUT_PATH = (
    Path(__file__).resolve().parent.parent
    / "artifacts"
    / "tables"
    / "fact_i140_demand.parquet"
)

COUNTRY_MAP = {
    "all countries": "ALL",
    "india": "IND",
    "china": "CHN",
    "philippines": "PHL",
    "brazil": "BRA",
    "vietnam": "VNM",
    "south korea": "KOR",
}


def _infer_country(sheet_name: str) -> str:
    """Map sheet name to country code."""
    sn = sheet_name.lower().strip()
    # Remove FY suffix like "FY25"
    sn_clean = re.sub(r"\s*fy\d{2,4}.*$", "", sn).strip()
    for pattern, code in COUNTRY_MAP.items():
        if pattern in sn_clean:
            return code
    return "UNKNOWN"


def _parse_int(val) -> int:
    """Parse cell value to int. Handles comma-formatted numbers, '-', None."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        return int(val) if val == val else 0
    s = str(val).strip().replace(",", "").replace(" ", "")
    if s in ("-", "", "nan", "NaN"):
        return 0
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return 0


def _infer_report_period(filepath: Path) -> str:
    """Extract report period from filename like i140_rec_by_class_country_fy2025_q4_v1.xlsx."""
    stem = filepath.stem.lower()
    m = re.search(r"fy(\d{4})_?(q\d)", stem)
    if m:
        return f"FY{m.group(1)}_{m.group(2).upper()}"
    m = re.search(r"fy(\d{4})", stem)
    if m:
        return f"FY{m.group(1)}"
    return stem


def parse_xlsx(filepath: Path) -> list[dict]:
    """Parse a single I-140 xlsx file into flat records."""
    wb = openpyxl.load_workbook(filepath, data_only=True, read_only=True)
    report_period = _infer_report_period(filepath)
    records = []

    for sheet_name in wb.sheetnames:
        country = _infer_country(sheet_name)
        if country == "UNKNOWN":
            continue

        ws = wb[sheet_name]
        rows = list(ws.rows)
        if len(rows) < 5:
            continue

        # Find the header row with fiscal years
        header_idx = None
        fy_columns = {}  # col_idx -> fiscal_year_int

        for r_idx in range(min(10, len(rows))):
            cells = [c.value for c in rows[r_idx]]
            # Look for row that has "Petitions by Employment Preference" or FY years
            first = str(cells[0] or "").strip()
            if "petitions by" in first.lower() or "employment preference" in first.lower():
                header_idx = r_idx
                for ci, cv in enumerate(cells[1:], start=1):
                    if cv is None:
                        continue
                    if isinstance(cv, (int, float)) and 2000 <= cv <= 2030:
                        fy_columns[ci] = int(cv)
                    elif isinstance(cv, str):
                        m = re.search(r"(\d{4})", str(cv))
                        if m and 2000 <= int(m.group(1)) <= 2030:
                            fy_columns[ci] = int(m.group(1))
                break

        if header_idx is None or not fy_columns:
            continue

        # Parse data rows - look for category sections
        current_category = "TOTAL"
        row_type = None

        for r_idx in range(header_idx + 1, len(rows)):
            cells = [c.value for c in rows[r_idx]]
            if not cells:
                continue

            label = str(cells[0] or "").strip()
            if not label:
                continue

            label_lower = label.lower()

            # Detect category headers
            if "first preference" in label_lower or "(eb1)" in label_lower:
                current_category = "EB1"
                continue
            elif "second preference" in label_lower or "(eb2)" in label_lower:
                current_category = "EB2"
                continue
            elif "third preference" in label_lower or "(eb3)" in label_lower:
                current_category = "EB3"
                continue
            elif "other and unknown" in label_lower:
                current_category = "OTHER"
                continue
            elif "approvals by category" in label_lower:
                continue  # skip sub-breakdowns
            elif "table key" in label_lower:
                break
            elif "aliens with" in label_lower or "outstanding" in label_lower:
                continue  # sub-category detail
            elif "multinational" in label_lower or "professionals with" in label_lower:
                continue
            elif "skilled workers" in label_lower or "needed unskilled" in label_lower:
                continue

            # Detect metric type
            if label_lower.startswith("total"):
                row_type = "received"
            elif label_lower.startswith("approved"):
                row_type = "approved"
            elif label_lower.startswith("denied"):
                row_type = "denied"
            elif "pending" in label_lower:
                row_type = "pending"
            else:
                continue

            # Extract values for each fiscal year
            for ci, fy in fy_columns.items():
                if ci >= len(cells):
                    continue
                val = _parse_int(cells[ci])
                # Find or create record
                key = (report_period, country, current_category, fy)
                existing = None
                for r in records:
                    if (r["report_period"], r["country"], r["category"], r["fiscal_year"]) == key:
                        existing = r
                        break
                if existing is None:
                    existing = {
                        "report_period": report_period,
                        "country": country,
                        "category": current_category,
                        "fiscal_year": fy,
                        "received": 0,
                        "approved": 0,
                        "denied": 0,
                        "pending": 0,
                    }
                    records.append(existing)
                existing[row_type] = val

    wb.close()
    return records


def parse_csv(filepath: Path) -> list[dict]:
    """Parse a CSV I-140 file into flat records."""
    report_period = _infer_report_period(filepath)
    records = []

    with open(filepath, "r", encoding="utf-8-sig") as fh:
        lines = fh.readlines()

    # Detect country from header lines
    country = "ALL"
    for line in lines[:5]:
        ll = line.lower()
        for pat, code in COUNTRY_MAP.items():
            if pat in ll:
                country = code
                break

    # Find header row with fiscal years
    fy_columns = {}
    header_line_idx = None
    for i, line in enumerate(lines):
        if "petitions by employment preference" in line.lower():
            header_line_idx = i
            parts = [p.strip().strip('"') for p in line.split(",")]
            for ci, p in enumerate(parts):
                m = re.search(r"(\d{4})", p)
                if m and 2000 <= int(m.group(1)) <= 2030:
                    fy_columns[ci] = int(m.group(1))
            break

    if header_line_idx is None or not fy_columns:
        return records

    current_category = "TOTAL"

    for i in range(header_line_idx + 1, len(lines)):
        line = lines[i].strip()
        if not line:
            continue

        # Parse CSV with quoted fields
        parts = []
        in_quotes = False
        current = ""
        for ch in line:
            if ch == '"':
                in_quotes = not in_quotes
            elif ch == ',' and not in_quotes:
                parts.append(current.strip().strip('"'))
                current = ""
            else:
                current += ch
        parts.append(current.strip().strip('"'))

        label = parts[0].strip() if parts else ""
        label_lower = label.lower()

        if not label:
            continue

        # Category detection
        if "first preference" in label_lower:
            current_category = "EB1"
            continue
        elif "second preference" in label_lower:
            current_category = "EB2"
            continue
        elif "third preference" in label_lower:
            current_category = "EB3"
            continue
        elif "other and unknown" in label_lower:
            current_category = "OTHER"
            continue
        elif "table key" in label_lower:
            break
        elif any(x in label_lower for x in ("approvals by", "aliens with", "outstanding",
                                              "multinational", "professionals with",
                                              "skilled workers", "needed unskilled")):
            continue

        if label_lower.startswith("total"):
            row_type = "received"
        elif label_lower.startswith("approved"):
            row_type = "approved"
        elif label_lower.startswith("denied"):
            row_type = "denied"
        elif "pending" in label_lower:
            row_type = "pending"
        else:
            continue

        for ci, fy in fy_columns.items():
            if ci >= len(parts):
                continue
            val = _parse_int(parts[ci])
            key = (report_period, country, current_category, fy)
            existing = None
            for r in records:
                if (r["report_period"], r["country"], r["category"], r["fiscal_year"]) == key:
                    existing = r
                    break
            if existing is None:
                existing = {
                    "report_period": report_period,
                    "country": country,
                    "category": current_category,
                    "fiscal_year": fy,
                    "received": 0,
                    "approved": 0,
                    "denied": 0,
                    "pending": 0,
                }
                records.append(existing)
            existing[row_type] = val

    return records


def main():
    # Find all I-140 files
    xlsx_files = sorted(EB_DIR.rglob("i140_rec_by_class_country*.xlsx")) + \
                 sorted(EB_DIR.rglob("I140_rec_by_class_country*.xlsx"))
    csv_files = sorted(EB_DIR.rglob("i140_rec_by_class_country*.csv")) + \
                sorted(EB_DIR.rglob("I140_rec_by_class_country*.csv"))
    per_country_csvs = sorted(EB_DIR.rglob("i140_rec_by_class_[!c]*.csv")) + \
                       sorted(EB_DIR.rglob("I140_rec_by_class_[!cC]*.csv"))

    # Deduplicate
    all_xlsx = sorted(set(xlsx_files), key=lambda p: p.name)
    all_csv = sorted(set(csv_files + per_country_csvs), key=lambda p: p.name)

    print(f"Found {len(all_xlsx)} xlsx + {len(all_csv)} csv I-140 files")

    all_records = []

    for fp in all_xlsx:
        try:
            recs = parse_xlsx(fp)
            print(f"  {fp.name}: {len(recs)} records")
            all_records.extend(recs)
        except Exception as e:
            print(f"  WARNING: {fp.name}: {e}", file=sys.stderr)

    for fp in all_csv:
        try:
            recs = parse_csv(fp)
            print(f"  {fp.name}: {len(recs)} records")
            all_records.extend(recs)
        except Exception as e:
            print(f"  WARNING: {fp.name}: {e}", file=sys.stderr)

    if not all_records:
        print("ERROR: No records parsed from any file", file=sys.stderr)
        sys.exit(1)

    df = pd.DataFrame(all_records)

    # Filter out OTHER category (negligible) and UNKNOWN country
    df = df[~df["category"].isin(["OTHER", "UNKNOWN"])].reset_index(drop=True)

    # Type enforcement
    df["fiscal_year"] = df["fiscal_year"].astype("Int32")
    df["received"] = df["received"].astype("Int64")
    df["approved"] = df["approved"].astype("Int64")
    df["denied"] = df["denied"].astype("Int64")
    df["pending"] = df["pending"].astype("Int64")

    # Sort
    df = df.sort_values(
        ["report_period", "country", "category", "fiscal_year"]
    ).reset_index(drop=True)

    # Save
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"\nWrote {len(df):,} rows x {len(df.columns)} cols -> {OUT_PATH}")
    print(f"Report periods: {sorted(df['report_period'].unique())}")
    print(f"Countries: {sorted(df['country'].unique())}")
    print(f"Categories: {sorted(df['category'].unique())}")

    # Show latest period summary for India
    latest = sorted(df["report_period"].unique())[-1]
    india = df[(df["report_period"] == latest) & (df["country"] == "IND")]
    print(f"\n--- India ({latest}) ---")
    for cat in ["TOTAL", "EB1", "EB2", "EB3"]:
        subset = india[india["category"] == cat]
        if len(subset) > 0:
            print(f"  {cat}: approved={subset['approved'].sum():,}  pending={subset['pending'].sum():,}")


if __name__ == "__main__":
    main()
