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


def _parse_chart_tables(pdf, report_year: int, fname: str) -> list[dict]:
    """
    Parse the chart data tables on pages 2-3 of the DOS waiting list PDF.

    Page 2 has a table like:
      [None, 'F1', 'F2A', 'F2B', 'F3', 'F4']
      ['2022', '282,459', '383,653', '411,773', '617,140', '2,220,476']
      ['2023', '261,384', '337,958', '385,664', '588,883', '2,199,512']

    Page 3 has:
      [None, 'E1', 'E2', 'E3', 'EW', 'E4', 'E5']
      ['2022', '8,818', '43,962', '41,838', '26,729', '1,303', '45,498']
      ['2023', '20,582', '75,567', '78,207', '44,470', '1,951', '39,883']

    These are pivoted (year × category) → unpivot to long format.
    """
    CHART_CATEGORY_MAP = {
        "F1": "F1", "F2A": "F2A", "F2B": "F2B", "F3": "F3", "F4": "F4",
        "E1": "EB1", "E2": "EB2", "E3": "EB3", "EW": "EB3-OW", "E4": "EB4", "E5": "EB5",
    }
    rows = []
    now = datetime.now(timezone.utc).isoformat()
    # Only pages 2-3 (0-indexed: 1-2) have chart tables
    for page_idx in range(min(3, len(pdf.pages))):
        page = pdf.pages[page_idx]
        tables = page.extract_tables()
        for tbl in tables:
            if not tbl or len(tbl) < 2:
                continue
            # Detect chart table: header row should have known category codes
            header = [str(c).strip() if c else "" for c in tbl[0]]
            known = [h for h in header if h in CHART_CATEGORY_MAP]
            if len(known) < 3:
                continue  # Not a chart table
            # Parse data rows (years)
            for row in tbl[1:]:
                cells = [str(c).strip() if c else "" for c in row]
                # First cell is the year
                year_str = cells[0].replace(",", "")
                try:
                    yr = int(year_str)
                except ValueError:
                    continue
                for i, cat_code in enumerate(header):
                    if cat_code not in CHART_CATEGORY_MAP:
                        continue
                    if i >= len(cells):
                        continue
                    val_str = cells[i].replace(",", "").replace("*", "")
                    try:
                        count = int(val_str)
                    except ValueError:
                        continue
                    rows.append({
                        "report_year": yr,
                        "category": CHART_CATEGORY_MAP[cat_code],
                        "country": "Worldwide Total",
                        "count_waiting": count,
                        "source_file": fname,
                        "ingested_at": now,
                    })
    return rows


# Regex: country name followed by number(s) — matches lines like:
#   "Mexico 85,950 32.9%"  or  "Mexico 1,190,444"  or  "All Others 73,207 28.0%"
_COUNTRY_LINE = re.compile(
    r"^([A-Z][A-Za-z\s\.\-\*]+?)\s+([\d,]+)(?:\s+[\d,.%\(\)\-\+]+)*$"
)
# Category section headers in the PDF text
_SECTION_HEADERS = [
    (re.compile(r"Family\s+FIRST\s+Preference", re.I), "F1"),
    (re.compile(r"Family\s+2A\s+Preference|Family\s+SECOND.*\n.*2A:", re.I), "F2A"),
    (re.compile(r"2A:.*visa numbers", re.I), "F2A"),
    (re.compile(r"2B:.*Visa numbers|2B:.*adult sons", re.I), "F2B"),
    (re.compile(r"Family\s+THIRD\s+Preference", re.I), "F3"),
    (re.compile(r"Family\s+FOURTH\s+Preference", re.I), "F4"),
    (re.compile(r"Employment\s+FIRST\s+Preference", re.I), "EB1"),
    (re.compile(r"Employment\s+SECOND\s+Preference", re.I), "EB2"),
    (re.compile(r"Employment\s+THIRD\s+Preference", re.I), "EB3"),
    (re.compile(r"Other\s+Worker\s+Components", re.I), "EB3-OW"),
    (re.compile(r"Employment\s+FOURTH\s+Preference", re.I), "EB4"),
    (re.compile(r"Employment\s+FIFTH\s+Preference", re.I), "EB5"),
]

# Region names (not countries — appear in "by Region" sections)
_REGION_NAMES = {
    "africa", "asia", "europe", "n. america", "n. america*",
    "oceania", "s. america", "south america", "north america",
}

# Patterns for lines that are NOT country rows (skip these)
_SKIP_PATTERNS = [
    re.compile(r"^(Number|Applicants|Preference|Category|Country|Region|Total|Waiting|Percent|List)", re.I),
    re.compile(r"^\d{4}\s"),  # Year-starting lines (chart data in text)
    re.compile(r"^(As of|FAMILY|EMPLOYMENT|TOTAL|GRAND)", re.I),
    re.compile(r"^(Skilled|Other)\s+(Workers|Worker)", re.I),
    re.compile(r"^\d[\d,]+\s*$"),  # bare numbers
    re.compile(r"^(Immigrant|Annual|Immigration|Visa|Sufficiently|The|It|About|Upon|Some|This|These|Eligible|Each|Most|A breakdown)", re.I),
]


def _parse_text_country_tables(pdf, report_year: int, fname: str) -> list[dict]:
    """
    Parse country-by-country breakdowns from PDF text (pages 5-15).

    Each category section has a header followed by a table like:
        Country    Total    Percent
        Mexico     85,950   32.9%
        ...
        All Others 73,207   28.0%
        Total      261,384  100%
    """
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    # Concatenate all page text with page markers
    full_text = ""
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            full_text += text + "\n\n"

    if not full_text:
        return rows

    # Find section boundaries and their categories
    sections = []
    for pattern, cat in _SECTION_HEADERS:
        for m in pattern.finditer(full_text):
            sections.append((m.start(), cat))
    sections.sort(key=lambda x: x[0])

    if not sections:
        log.warning("No category section headers found in PDF text")
        return rows

    # For each section, extract country-count pairs until next section or end
    for idx, (start_pos, category) in enumerate(sections):
        end_pos = sections[idx + 1][0] if idx + 1 < len(sections) else len(full_text)
        section_text = full_text[start_pos:end_pos]
        lines = section_text.split("\n")

        for line in lines:
            line = line.strip()
            if not line or len(line) < 3:
                continue

            # Skip non-country lines
            skip = False
            for sp in _SKIP_PATTERNS:
                if sp.search(line):
                    skip = True
                    break
            if skip:
                continue

            m = _COUNTRY_LINE.match(line)
            if not m:
                continue

            country = m.group(1).strip().rstrip(",")
            count_str = m.group(2).replace(",", "")
            try:
                count = int(count_str)
            except ValueError:
                continue

            # Skip "Total" or "Worldwide Total" rows (we get those from chart tables)
            if country.lower() in ("total", "worldwide total", "family total",
                                   "employment total"):
                continue

            # Skip region names (Africa, Asia, etc.) — these leak from "by Region" sections
            if country.lower().rstrip("*") in _REGION_NAMES or country.lower() in _REGION_NAMES:
                continue

            rows.append({
                "report_year": report_year,
                "category": category,
                "country": country,
                "count_waiting": count,
                "source_file": fname,
                "ingested_at": now,
            })

    return rows


def _parse_worldwide_by_country(pdf, report_year: int, fname: str) -> list[dict]:
    """
    Parse page 4's worldwide top-10 by country (all categories combined).
    Also parse pages 5-6 and 11-12 for family/employment totals by country.
    """
    rows = []
    now = datetime.now(timezone.utc).isoformat()

    full_text = ""
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            full_text += text + "\n\n"

    # Find the "Immigrant Waiting List By Country" section (page 4)
    m = re.search(r"Immigrant Waiting List\s*\n\s*By Country", full_text, re.I)
    if not m:
        return rows

    # Find boundaries: from this header to next major section
    start = m.end()
    # Next section is "Family-sponsored Immigrant Waiting List by Country"
    next_sec = re.search(r"Family-sponsored Immigrant Waiting List by Country", full_text[start:], re.I)
    end = start + next_sec.start() if next_sec else start + 3000

    section = full_text[start:end]
    for line in section.split("\n"):
        line = line.strip()
        if not line:
            continue
        lm = _COUNTRY_LINE.match(line)
        if not lm:
            continue
        country = lm.group(1).strip().rstrip(",")
        if country.lower() in ("country", "total", "worldwide total"):
            continue
        count_str = lm.group(2).replace(",", "")
        try:
            count = int(count_str)
        except ValueError:
            continue
        rows.append({
            "report_year": report_year,
            "category": "ALL",
            "country": country,
            "count_waiting": count,
            "source_file": fname,
            "ingested_at": now,
        })

    return rows


def parse_pdf_waiting_list(pdf_path: pathlib.Path) -> pd.DataFrame:
    """
    Extract waiting list data from DOS "Annual Report of Immigrant Visa
    Applicants" PDF.

    Three extraction strategies:
    1. Chart data tables (pages 2-3): Category totals for each year
    2. Text country tables (pages 5-15): Per-country breakdowns by category
    3. Worldwide by country (page 4): Overall country ranking across all categories
    """
    fy_m = re.search(r"(\d{4})", pdf_path.parent.name)
    report_year = int(fy_m.group(1)) if fy_m else 0
    fname = pdf_path.name
    all_rows = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            # 1. Chart data tables (category totals, "Worldwide Total")
            chart_rows = _parse_chart_tables(pdf, report_year, fname)
            log.info("  Chart tables: %d rows", len(chart_rows))
            all_rows.extend(chart_rows)

            # 2. Text-based country breakdowns per category
            text_rows = _parse_text_country_tables(pdf, report_year, fname)
            log.info("  Text tables: %d country rows", len(text_rows))
            all_rows.extend(text_rows)

            # 3. Worldwide by country (all categories combined)
            ww_rows = _parse_worldwide_by_country(pdf, report_year, fname)
            log.info("  Worldwide by country: %d rows", len(ww_rows))
            all_rows.extend(ww_rows)

    except Exception as e:
        log.warning("PDF parse error %s: %s", pdf_path, e)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    log.info("  Total PDF rows before dedup: %d", len(df))
    return df


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

    # Drop CSV stub rows (trivial counts 1-4) when PDF data is available
    pdf_years = set(df.loc[df["source_file"].str.endswith(".pdf", na=False), "report_year"].unique())
    stub_mask = (
        df["source_file"].str.endswith(".csv", na=False)
        & df["report_year"].isin(pdf_years)
        & (df["count_waiting"] < 10)
    )
    if stub_mask.sum() > 0:
        log.info("Dropping %d CSV stub rows (PDF data available for same year)", stub_mask.sum())
        df = df[~stub_mask]

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
