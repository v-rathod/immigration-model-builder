#!/usr/bin/env python3
"""
build_fact_iv_post.py
Parse Visa_Statistics "IV Issuances by Post and Visa Class" PDFs
→ fact_iv_post.parquet

Schema: fiscal_year, month, calendar_year, post, visa_class, category, issued,
        source_file, ingested_at

These PDFs contain monthly *immigrant visa* issuance counts broken down by
consular post (city) and visa class. P2 previously skipped them (only processed
the FSC/country-level PDFs). This builder fills that gap so RAG can answer
queries like "How many F1 visas were issued in Amsterdam in Feb 2025?"

Source: /downloads/Visa_Statistics/<year>/<MONTH> <YEAR> - IV Issuances by Post and Visa Class.pdf
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

# ---------------------------------------------------------------------------
# Month helpers
# ---------------------------------------------------------------------------
MONTH_ORDER = [
    "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE",
    "JULY", "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
]
MONTH_FY_OFFSET = {
    # Oct-Dec of calendar year Y → FY Y+1
    "OCTOBER": 1, "NOVEMBER": 1, "DECEMBER": 1,
}


def _extract_month_year(fname: str):
    """Extract (MONTH, calendar_year) from filename like
    'FEBRUARY 2025 - IV Issuances by Post and Visa Class.pdf'."""
    m = re.match(r"(\w+)\s+(\d{4})", fname)
    if m:
        return m.group(1).upper(), int(m.group(2))
    return None, None


def _fiscal_year(month: str, cal_year: int) -> str:
    offset = MONTH_FY_OFFSET.get(month, 0)
    return f"FY{cal_year + offset}"


def _classify_visa_class(vc: str) -> str:
    """Map visa class to broad category."""
    vc = vc.upper().strip()
    if vc.startswith("IR") or vc.startswith("CR") or vc in ("IW", "IB"):
        return "IMMEDIATE_RELATIVE"
    if vc == "FX" or (vc.startswith("F") and len(vc) <= 3 and vc[1:].isdigit()):
        return "FAMILY_PREF"
    if vc.startswith("E") and len(vc) <= 3:
        return "EMPLOYMENT_PREF"
    if vc == "EW":
        return "EMPLOYMENT_PREF"
    if vc == "DV":
        return "DIVERSITY"
    if vc.startswith("SQ") or vc.startswith("SI") or vc.startswith("SB"):
        return "SPECIAL_IMMIGRANT"
    if vc.startswith("IH"):
        return "ADOPTION"
    if vc.startswith("RH") or vc.startswith("RU"):
        return "RETURNING_RESIDENT"
    if vc.startswith("GV"):
        return "GOBIERNO"
    if vc in ("AM", "AM1", "AM2"):
        return "AMERASIAN"
    if vc in ("SE", "BC"):
        return "SPECIAL"
    return "OTHER"


# Data line: "Amsterdam F1 2" or "Abu Dhabi DV 109"
DATA_LINE_RE = re.compile(
    r"^(.+?)\s+([A-Z][A-Z0-9]{1,5})\s+([\d,]+)\s*$"
)

SKIP_WORDS = frozenset([
    "page", "immigrant visa issuances", "post", "visa class", "issuances",
    "total", "grand total",
])


def parse_post_pdf(pdf_path: pathlib.Path) -> pd.DataFrame:
    """Parse one 'IV Issuances by Post and Visa Class' PDF."""
    fname = pdf_path.name
    month, cal_year = _extract_month_year(fname)
    if not month or not cal_year:
        log.warning("Cannot parse month/year from: %s", fname)
        return pd.DataFrame()

    fy = _fiscal_year(month, cal_year)
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as e:
        log.debug("PDF error %s: %s", fname, e)
        return pd.DataFrame()

    if not all_text.strip():
        return pd.DataFrame()

    # Override FY from title if present, e.g. "February 2025 (FY 2025)"
    fy_match = re.search(r"\(FY\s*(\d{4})\)", all_text[:400], re.IGNORECASE)
    if fy_match:
        fy = f"FY{fy_match.group(1)}"

    rows = []
    for line in all_text.split("\n"):
        line = line.strip()
        if not line:
            continue
        ll = line.lower()
        if any(w in ll for w in SKIP_WORDS) and len(line) < 80:
            continue

        m = DATA_LINE_RE.match(line)
        if not m:
            continue

        post = m.group(1).strip()
        visa_class = m.group(2).strip()
        try:
            issued = int(m.group(3).replace(",", ""))
        except ValueError:
            continue
        if issued < 0:
            continue

        rows.append({
            "fiscal_year": fy,
            "month": month.title(),
            "calendar_year": cal_year,
            "post": post,
            "visa_class": visa_class,
            "category": _classify_visa_class(visa_class),
            "issued": issued,
            "source_file": fname,
            "ingested_at": now_ts,
        })

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def main():
    ap = argparse.ArgumentParser(description="Build fact_iv_post from POST PDFs")
    ap.add_argument("--downloads", default="/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads")
    ap.add_argument("--out", default="artifacts/tables/fact_iv_post.parquet")
    args = ap.parse_args()

    droot = pathlib.Path(args.downloads) / "Visa_Statistics"
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(droot.rglob("*Post*.pdf"))
    log.info("Found %d POST PDFs in %s", len(pdfs), droot)

    frames = []
    parsed_ok = 0
    for i, pdf in enumerate(pdfs):
        if (i + 1) % 20 == 0:
            log.info("  Processing %d/%d ...", i + 1, len(pdfs))
        df = parse_post_pdf(pdf)
        if len(df) > 0:
            frames.append(df)
            parsed_ok += 1

    log.info("Parsed %d/%d POST PDFs successfully", parsed_ok, len(pdfs))

    if not frames:
        log.warning("No data extracted; creating empty parquet")
        df = pd.DataFrame(columns=[
            "fiscal_year", "month", "calendar_year", "post", "visa_class",
            "category", "issued", "source_file", "ingested_at"
        ])
    else:
        df = pd.concat(frames, ignore_index=True)

    df["issued"] = pd.to_numeric(df["issued"], errors="coerce").fillna(0).astype(int)

    pk = ["fiscal_year", "month", "post", "visa_class"]
    before = len(df)
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    if before != len(df):
        log.info("Deduped: %d → %d rows", before, len(df))

    df = df.sort_values(pk).reset_index(drop=True)

    # Summary
    posts = df["post"].nunique()
    classes = df["visa_class"].nunique()
    fys = sorted(df["fiscal_year"].unique())
    log.info("COMPLETE fact_iv_post: %d rows, %d posts, %d visa classes, FYs: %s",
             len(df), posts, classes, fys)

    df.to_parquet(out_path, index=False)
    log.info("Written to %s", out_path)

    # Quick Amsterdam/F1 check
    ams_f1 = df[(df["post"] == "Amsterdam") & (df["visa_class"] == "F1")]
    if len(ams_f1) > 0:
        print("\n--- Amsterdam F1 data ---")
        print(ams_f1[["fiscal_year", "month", "post", "visa_class", "issued"]].to_string(index=False))


if __name__ == "__main__":
    main()
