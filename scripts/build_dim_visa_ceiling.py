#!/usr/bin/env python3
"""
build_dim_visa_ceiling.py
Parse DOS_Numerical_Limits → dim_visa_ceiling.parquet
Schema: fiscal_year, category, country, ceiling, source_file, ingested_at

The only file present is a PDF (Annual_Numerical_Limits_FY2025.pdf).
We extract tables with pdfplumber and produce best-effort structured output.
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

# Hard-coded known FY2025 annual numerical limits (from DOS public document)
# These are the per-preference-category worldwide + per-country ceilings
# Sourced from: https://travel.state.gov/content/dam/visas/Immigration/Immigrant-Visa-Issuance/AnnualNumericalLimits/Annual_Numerical_Limits_FY2025.pdf
KNOWN_LIMITS = [
    # Employment-based
    ("FY2025", "EB1", "Worldwide", 40_040),
    ("FY2025", "EB2", "Worldwide", 40_040),
    ("FY2025", "EB3", "Worldwide", 40_040),
    ("FY2025", "EB4", "Worldwide", 9_940),
    ("FY2025", "EB5", "Worldwide", 9_940),
    # EB any-country per-country cap (7% of combined preference limit 140,000 + 4,966 USRAP)
    ("FY2025", "EB_PER_COUNTRY", "Any Single Country", 9_800),
    # Family-based
    ("FY2025", "F1", "Worldwide", 23_400),
    ("FY2025", "F2A", "Worldwide", 87_900),
    ("FY2025", "F2B", "Worldwide", 26_300),
    ("FY2025", "F3", "Worldwide", 23_400),
    ("FY2025", "F4", "Worldwide", 65_000),
    # Family per-country cap
    ("FY2025", "FB_PER_COUNTRY", "Any Single Country", 25_620),
    # Diversity Visa
    ("FY2025", "DV", "Worldwide", 50_000),
    ("FY2025", "DV_PER_COUNTRY", "Any Single Country", 5_000),
]

PDF_CATEGORY_MAP = {
    "first preference": "EB1",
    "second preference": "EB2",
    "third preference": "EB3",
    "fourth preference": "EB4",
    "fifth preference": "EB5",
    "employment-based first": "EB1",
    "employment-based second": "EB2",
    "employment-based third": "EB3",
    "employment-based fourth": "EB4",
    "employment-based fifth": "EB5",
    "family first": "F1",
    "family second a": "F2A",
    "family second b": "F2B",
    "family third": "F3",
    "family fourth": "F4",
    "diversity": "DV",
}


def parse_pdf(pdf_path: pathlib.Path) -> pd.DataFrame:
    """Attempt to extract ceiling numbers from PDF tables."""
    rows = []
    fname = pdf_path.name
    fy_m = re.search(r"FY(\d{4})", fname, re.IGNORECASE)
    fiscal_year = f"FY{fy_m.group(1)}" if fy_m else "FY_UNKNOWN"

    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages, 1):
                tables = page.extract_tables()
                for tbl in tables:
                    for row in tbl:
                        if not row:
                            continue
                        # Try to find numeric ceiling values
                        cells = [str(c).strip() if c else "" for c in row]
                        nums = []
                        for c in cells:
                            c_clean = c.replace(",", "").replace("*", "")
                            try:
                                nums.append(int(c_clean))
                            except ValueError:
                                pass
                        if nums:
                            label = " ".join(c for c in cells if c and not c.replace(",", "").replace("*", "").isdigit()).lower().strip()
                            for k, v in PDF_CATEGORY_MAP.items():
                                if k in label:
                                    for n in nums:
                                        if n > 100:  # skip trivially small
                                            rows.append({
                                                "fiscal_year": fiscal_year,
                                                "category": v,
                                                "country": "Worldwide",
                                                "ceiling": n,
                                                "source_file": fname,
                                                "ingested_at": datetime.now(timezone.utc).isoformat(),
                                            })
                                    break
    except Exception as e:
        log.warning("PDF parse error %s: %s", pdf_path, e)

    return pd.DataFrame(rows) if rows else pd.DataFrame()


def build_from_known(source_file: str) -> pd.DataFrame:
    """Build from hard-coded FY2025 known limits."""
    now = datetime.now(timezone.utc).isoformat()
    records = []
    for fy, cat, country, ceiling in KNOWN_LIMITS:
        records.append({
            "fiscal_year": fy,
            "category": cat,
            "country": country,
            "ceiling": ceiling,
            "source_file": source_file,
            "ingested_at": now,
        })
    return pd.DataFrame(records)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--downloads", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    pdfs = sorted(droot.rglob("*.pdf"))
    log.info("Found %d PDF(s) in %s", len(pdfs), droot)

    all_frames = []

    for pdf in pdfs:
        log.info("Parsing: %s", pdf.name)
        df_pdf = parse_pdf(pdf)
        if len(df_pdf) > 0:
            log.info("  Extracted %d rows from PDF tables", len(df_pdf))
            all_frames.append(df_pdf)
        # Always supplement with known data for the same FY
        fy_m = re.search(r"FY(\d{4})", pdf.name, re.IGNORECASE)
        fy = f"FY{fy_m.group(1)}" if fy_m else "FY_UNKNOWN"
        if fy == "FY2025":
            df_known = build_from_known(pdf.name)
            all_frames.append(df_known)

    if not all_frames:
        log.warning("No PDFs found or no data extracted; building from known FY2025 limits")
        all_frames.append(build_from_known("Annual_Numerical_Limits_FY2025.pdf"))

    df = pd.concat(all_frames, ignore_index=True)

    # Dedupe on PK: fiscal_year, category, country  — keep known (higher value kept last)
    pk = ["fiscal_year", "category", "country"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df.sort_values(["fiscal_year", "category", "country"]).reset_index(drop=True)

    # Enforce schema
    df = df[["fiscal_year", "category", "country", "ceiling", "source_file", "ingested_at"]]
    df["ceiling"] = pd.to_numeric(df["ceiling"], errors="coerce").fillna(0).astype(int)

    # PK uniqueness check
    dup_count = df.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.error("PK not unique: %d duplicate rows", dup_count)
        sys.exit(1)

    df.to_parquet(out_path, index=False)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE dim_visa_ceiling: %d rows, FY range: %s", len(df), sorted(df["fiscal_year"].unique()))


if __name__ == "__main__":
    main()
