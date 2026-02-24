#!/usr/bin/env python3
"""
build_fact_visa_issuance.py
Parse Visa_Annual_Reports → fact_visa_issuance.parquet
Schema: fiscal_year, category, country, issued, source_file, ingested_at

Files: ~274 PDFs across FY2015-FY2024 (Table I through Table X etc.)
Strategy: pdfplumber table extraction; structure Tables I/II = IV issuance by class.
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

# Table name → category mapping
TABLE_CATEGORY_MAP = {
    "table i": "TOTAL_IV",
    "table ii": "IMMEDIATE_RELATIVE",
    "table iii": "FAMILY_PREF",
    "table iv": "EMPLOYMENT_PREF",
    "table v": "DIVERSITY",
    "table vi": "REFUGEE_SIV",
    "table vii": "TOTAL_NIV",
    "table viii": "NIV_DETAIL",
    "table ix": "ADDITIONAL",
    "table x": "TOTAL",
}

def extract_fy(filename: str) -> str:
    """Extract FY from filename like FY15AnnualReport-TableI.pdf"""
    m = re.search(r"FY(\d{2,4})", filename, re.IGNORECASE)
    if m:
        yr = int(m.group(1))
        if yr < 100:
            yr = 2000 + yr
        return f"FY{yr}"
    # Try parent dir year
    return "FY_UNKNOWN"


def extract_table_type(filename: str) -> str:
    """Extract table type from filename."""
    m = re.search(r"Table([IVX]+|[0-9]+)", filename, re.IGNORECASE)
    if m:
        return f"Table {m.group(1).upper()}"
    return "UNKNOWN"


def _safe_int(s: str) -> int:
    try:
        return int(s.replace(",", "").replace("*", "").strip())
    except (ValueError, AttributeError):
        return -1


# Regex for a data row: text label + 2-10 space-separated numbers (possibly with commas)
# Captures: label, and all number tokens
DATA_ROW_RE = re.compile(
    r"^(.+?)\s{2,}((?:[\d,]+\s+){1,9}[\d,]+)\s*$"
)
# Simpler fallback: any line ending in 2+ numbers after text
DATA_ROW_ALT_RE = re.compile(
    r"^([\w\s,'.'\-]+?)\s+((?:[\d,]+\s*){2,})$"
)
# Pure numbers-only line (year header like "2019 2020 2021 2022 2023")
YEAR_LINE_RE = re.compile(r"^(\d{4})(?:\s+\d{4}){1,9}\s*$")
# Single-year line in title ("Fiscal Year 2023")
TITLE_FY_RE = re.compile(r"Fiscal\s+Year[s]?\s+(\d{4})", re.IGNORECASE)
# Multi-year range in title ("Fiscal Years 2019-2023")
MULTI_FY_RE = re.compile(r"Fiscal\s+Years?\s+(\d{4})(?:\W+\d{4})*\W+(\d{4})", re.IGNORECASE)
# Region header detection (title case word(s), no digits)
REGION_RE = re.compile(r"^[A-Z][a-zA-Z\s,]+$")
SKIP_WORDS = frozenset([
    "total", "grand total", "worldwide", "all countries", "subtotal",
    "note", "source", "fiscal year", "table", "immigrant", "nonimmigrant",
    "foreign state", "categories", "statistics", "border", "adjusted",
    "region", "country", "chargeability",
])


def parse_pdf_visa_report(pdf_path: pathlib.Path) -> pd.DataFrame:
    """Extract issuance data from a DOS Annual Report PDF table via text parsing."""
    fname = pdf_path.name
    fy = extract_fy(fname)
    if fy == "FY_UNKNOWN":
        m = re.search(r"\b(\d{4})\b", str(pdf_path.parent))
        if m:
            fy = f"FY{m.group(1)}"

    table_type_raw = extract_table_type(fname)
    table_key = table_type_raw.lower()
    category = TABLE_CATEGORY_MAP.get(table_key, table_type_raw)

    rows_out = []
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_text = "\n".join(
                page.extract_text() or "" for page in pdf.pages
            )
    except Exception as e:
        log.debug("PDF open error %s: %s", fname, e)
        return pd.DataFrame()

    if not all_text.strip():
        return pd.DataFrame()

    # Override FY from title if present (more reliable than filename)
    title_m = TITLE_FY_RE.search(all_text[:500])
    if title_m:
        fy = f"FY{title_m.group(1)}"

    # For multi-year tables (Table I style), build a year list so we can
    # record the most recent year as the fiscal_year of each row.
    multi_fys: list[str] = []
    multi_m = MULTI_FY_RE.search(all_text[:500])
    if multi_m:
        # Find all 4-digit years in the title line
        year_tokens = re.findall(r"\b(20\d{2}|19\d{2})\b", all_text[:300])
        multi_fys = [f"FY{y}" for y in year_tokens]

    lines = all_text.split("\n")
    year_header_seen = False
    current_years: list[str] = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Detect year-header lines like "2019 2020 2021 2022 2023"
        if YEAR_LINE_RE.match(line):
            current_years = [f"FY{y}" for y in line.split()]
            year_header_seen = True
            continue

        # Skip known skip patterns
        lline = line.lower()
        if any(w in lline for w in SKIP_WORDS) and len(line) < 60:
            continue

        # Try to match a data row (label + space-separated numbers)
        # Accept lines with ≥2 numeric tokens
        tokens = line.split()
        if len(tokens) < 2:
            continue

        # Find split point: last non-numeric token
        num_tokens = []
        label_tokens = []
        for tok in reversed(tokens):
            cleaned = tok.replace(",", "").replace("*", "").replace("[", "").replace("]", "")
            if re.match(r"^\d+$", cleaned):
                num_tokens.insert(0, cleaned)
            else:
                label_tokens = tokens[: len(tokens) - len(num_tokens)]
                break

        if len(num_tokens) < 2:
            continue

        label = " ".join(label_tokens).strip().rstrip("1234567890").strip()
        if not label or len(label) < 2:
            continue
        if label.lower() in SKIP_WORDS:
            continue
        # Skip pure region headers (single all-caps or title-case region name)
        if re.match(r"^[A-Z][a-z]+(\s+[A-Za-z]+){0,3}$", label) and len(num_tokens) < 3:
            continue

        # Use last number as total issued (rightmost = Total column for multi-cat tables)
        issued_str = num_tokens[-1]
        issued = _safe_int(issued_str)
        if issued < 0:
            continue

        # Determine fiscal year for this row
        row_fy = fy
        if current_years:
            # For multi-year tables, each row has values for multiple years
            # Emit one row per year
            for i, col_fy in enumerate(current_years):
                if i < len(num_tokens):
                    val = _safe_int(num_tokens[i])
                    if val >= 0:
                        rows_out.append({
                            "fiscal_year": col_fy,
                            "category": category,
                            "country": "TOTAL",  # Table I has no country, just categories
                            "issued": val,
                            "source_file": fname,
                            "ingested_at": now_ts,
                        })
            continue

        rows_out.append({
            "fiscal_year": row_fy,
            "category": category,
            "country": label,
            "issued": issued,
            "source_file": fname,
            "ingested_at": now_ts,
        })

    return pd.DataFrame(rows_out) if rows_out else pd.DataFrame()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--downloads", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(droot.rglob("*.pdf"))
    log.info("Found %d PDFs in %s", len(pdfs), droot)

    all_frames = []
    parsed_ok = 0
    for i, pdf in enumerate(pdfs):
        if (i + 1) % 25 == 0:
            log.info("  Processing %d/%d ...", i + 1, len(pdfs))
        df = parse_pdf_visa_report(pdf)
        if len(df) > 0:
            all_frames.append(df)
            parsed_ok += 1

    log.info("Parsed %d/%d PDFs successfully", parsed_ok, len(pdfs))

    if not all_frames:
        log.warning("No data extracted; creating empty parquet with correct schema")
        df = pd.DataFrame(columns=["fiscal_year", "category", "country", "issued", "source_file", "ingested_at"])
    else:
        df = pd.concat(all_frames, ignore_index=True)

    df["issued"] = pd.to_numeric(df["issued"], errors="coerce").fillna(0).astype(int)
    df = df[df["issued"] >= 0]

    pk = ["fiscal_year", "category", "country"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df.sort_values(["fiscal_year", "category", "country"]).reset_index(drop=True)
    df = df[["fiscal_year", "category", "country", "issued", "source_file", "ingested_at"]]

    dup_count = df.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.warning("PK not unique after dedup: %d remaining duplicates (investigate)", dup_count)

    df.to_parquet(out_path, index=False)
    coverage_pct = 100.0 * parsed_ok / max(len(pdfs), 1)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_visa_issuance: %d rows, parse coverage=%.1f%%", len(df), coverage_pct)
    if coverage_pct < 95:
        log.warning("Coverage %.1f%% < 95%% threshold", coverage_pct)


if __name__ == "__main__":
    main()
