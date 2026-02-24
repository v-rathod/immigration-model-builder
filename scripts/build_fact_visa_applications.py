#!/usr/bin/env python3
"""
build_fact_visa_applications.py
Parse Visa_Statistics → fact_visa_applications.parquet
Schema: fiscal_year, visa_class, category, country, applications, refusals, source_file, ingested_at

Files: ~198 PDFs — monthly "IV Issuances by FSC and Visa Class" + "by Post and Visa Class"
Strategy: pdfplumber table extraction; wide-format country × visa_class → melt to long.
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

# Month → fiscal_year assignment (Oct-Sep = FY)
MONTH_FY_MAP = {
    "JANUARY": ("Q2", 0), "FEBRUARY": ("Q2", 0), "MARCH": ("Q2", 0),
    "APRIL": ("Q3", 0), "MAY": ("Q3", 0), "JUNE": ("Q3", 0),
    "JULY": ("Q4", 0), "AUGUST": ("Q4", 0), "SEPTEMBER": ("Q4", 0),
    "OCTOBER": ("Q1", 1), "NOVEMBER": ("Q1", 1), "DECEMBER": ("Q1", 1),
}


def extract_fy_from_name(fname: str) -> str:
    """Derive fiscal_year from filename like 'APRIL 2017 - IV Issuances...'"""
    m = re.search(r"(\w+)\s+(\d{4})", fname)
    if m:
        month = m.group(1).upper()
        cal_year = int(m.group(2))
        if month in MONTH_FY_MAP:
            _, offset = MONTH_FY_MAP[month]
            return f"FY{cal_year + offset}"
        return f"FY{cal_year}"
    m2 = re.search(r"(\d{4})", fname)
    if m2:
        return f"FY{m2.group(1)}"
    return "FY_UNKNOWN"


def classify_report(fname: str) -> str:
    """Classify as FSC (by Foreign State of Chargeability) or POST."""
    fl = fname.upper()
    if "FSC" in fl or "CHARGEABILITY" in fl:
        return "FSC"
    elif "POST" in fl:
        return "POST"
    return "OTHER"


def classify_visa_class(vc: str) -> str:
    """Map visa class code to broad category."""
    vc = vc.upper().strip()
    if vc.startswith("IR") or vc.startswith("CR") or vc in ("IW", "IB"):
        return "IMMEDIATE_RELATIVE"
    if vc.startswith("F") and vc[1:].isdigit():
        return "FAMILY_PREF"
    if vc.startswith("FX") or vc == "FX":
        return "FAMILY_PREF"
    if vc.startswith("E") and len(vc) <= 3:
        return "EMPLOYMENT_PREF"
    if vc == "EW":
        return "EMPLOYMENT_PREF"
    if vc == "DV":
        return "DIVERSITY"
    if vc.startswith("SQ") or vc.startswith("SI") or vc.startswith("SB"):
        return "SPECIAL_IMMIGRANT"
    if vc in ("AM", "AM1", "AM2"):
        return "AMERASIAN"
    return "OTHER"


# One data row: country-name-with-optional-spaces, visa_class, integer count
# e.g. "Afghanistan CR1 25" or "Congo, Dem. Rep. of the F1 3"
DATA_LINE_RE = re.compile(
    r"^(.+?)\s+([A-Z][A-Z0-9]{1,5})\s+([\d,]+)\s*$"
)


def parse_pdf_iv(pdf_path: pathlib.Path) -> pd.DataFrame:
    """Parse IV issuance FSC PDF into long-format rows using text extraction."""
    fname = pdf_path.name
    fiscal_year = extract_fy_from_name(fname)
    rpt_class = classify_report(fname)
    now_ts = datetime.now(timezone.utc).isoformat()

    # Only process FSC files (country-level data); skip POST files
    if rpt_class != "FSC":
        return pd.DataFrame()

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        log.debug("PDF error %s: %s", fname, e)
        return pd.DataFrame()

    if not all_text.strip():
        return pd.DataFrame()

    # Override FY from title text if present "January 2024 (FY 2024)"
    fy_in_title = re.search(r"\(FY\s*(\d{4})\)", all_text[:300], re.IGNORECASE)
    if fy_in_title:
        fiscal_year = f"FY{fy_in_title.group(1)}"

    rows_out = []
    skip_words = frozenset([
        "page", "immigrant visa issuances", "foreign state", "chargeability",
        "place of birth", "visa class", "issuances", "total",
    ])

    for line in all_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        # Skip header/footer lines
        ll = line.lower()
        if any(w in ll for w in skip_words) and len(line) < 80:
            continue

        m = DATA_LINE_RE.match(line)
        if not m:
            continue

        country = m.group(1).strip()
        visa_class = m.group(2).strip()
        count_str = m.group(3).replace(",", "")
        try:
            count = int(count_str)
        except ValueError:
            continue
        if count < 0:
            continue

        category = classify_visa_class(visa_class)

        rows_out.append({
            "fiscal_year": fiscal_year,
            "visa_class": visa_class,
            "category": category,
            "country": country,
            "applications": count,
            "refusals": 0,  # not available in these reports
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

    # Only use FSC reports (not POST) to avoid double-counting
    pdfs = sorted(droot.rglob("*.pdf"))
    fsc_pdfs = [p for p in pdfs if classify_report(p.name) == "FSC"]
    log.info("Found %d total PDFs; using %d FSC reports", len(pdfs), len(fsc_pdfs))

    all_frames = []
    parsed_ok = 0
    for i, pdf in enumerate(fsc_pdfs):
        if (i + 1) % 20 == 0:
            log.info("  Processing %d/%d ...", i + 1, len(fsc_pdfs))
        df = parse_pdf_iv(pdf)
        if len(df) > 0:
            all_frames.append(df)
            parsed_ok += 1

    log.info("Parsed %d/%d FSC PDFs", parsed_ok, len(fsc_pdfs))

    if not all_frames:
        log.warning("No data extracted; creating empty parquet")
        df = pd.DataFrame(columns=["fiscal_year", "visa_class", "category", "country",
                                    "applications", "refusals", "source_file", "ingested_at"])
    else:
        df = pd.concat(all_frames, ignore_index=True)

    df["applications"] = pd.to_numeric(df["applications"], errors="coerce").fillna(0).astype(int)
    df["refusals"] = pd.to_numeric(df["refusals"], errors="coerce").fillna(0).astype(int)
    df = df[df["applications"] >= 0]

    pk = ["fiscal_year", "visa_class", "category", "country"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df.sort_values(pk).reset_index(drop=True)
    df = df[["fiscal_year", "visa_class", "category", "country", "applications", "refusals", "source_file", "ingested_at"]]

    dup_count = df.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.warning("PK not unique after dedup: %d remaining", dup_count)

    df.to_parquet(out_path, index=False)
    coverage_pct = 100.0 * parsed_ok / max(len(fsc_pdfs), 1)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_visa_applications: %d rows, parse coverage=%.1f%%", len(df), coverage_pct)
    if coverage_pct < 95:
        log.warning("Coverage %.1f%% < 95%% threshold (PDF parsing limitation)", coverage_pct)


if __name__ == "__main__":
    main()
