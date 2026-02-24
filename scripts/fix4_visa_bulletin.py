#!/usr/bin/env python3
"""
FIX 4 (v2): Visa Bulletin — merge existing fact_cutoffs + legacy parse + dedupe.

Strategy:
  A) Read existing fact_cutoffs partitioned parquet (2015-2025 built by run_curate)
  B) Parse ALL PDFs: legacy 2011-2014 via text, modern via text+table
  C) Dedupe on PK (bulletin_year, bulletin_month, chart, category, country)
  D) Write clean partitioned parquet
"""
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import pdfplumber
import pyarrow.parquet as pq
import yaml

MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12,
    'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
    'jun': 6, 'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12,
}

COUNTRY_MAP = {
    'All Chargeability Areas Except Those Listed': 'ROW',
    'CHINA-mainland born': 'CHN',
    'INDIA': 'IND',
    'MEXICO': 'MEX',
    'PHILIPPINES': 'PHL',
    'All Chargeability Areas': 'ROW',
    'All Chargeability': 'ROW',
    'All Charge-': 'ROW',
    'CHINA - MAINLAND BORN': 'CHN',
    'CHINA': 'CHN',
    'China-mainland born': 'CHN',
    'China - Mainland Born': 'CHN',
    'China- mainland born': 'CHN',
    'India': 'IND',
    'Mexico': 'MEX',
    'Philippines': 'PHL',
}

CATEGORY_MAP = {
    '1st': 'EB1', '2nd': 'EB2', '3rd': 'EB3',
    'Other Workers': 'EB3-Other', 'Other': 'EB3-Other',
    '4th': 'EB4', '5th': 'EB5',
    'Certain Religious Workers': 'EB4-RW',
}

COUNTRIES_ORDERED = ['ROW', 'CHN', 'IND', 'MEX', 'PHL']


def parse_filename_enhanced(filename: str) -> Tuple[Optional[int], Optional[int]]:
    """Extract year and month from filename."""
    m = re.search(r'(?:visa[_-]?bulletin|VisaBulletin)[_-]?(\w+?)(\d{4})', filename, re.IGNORECASE)
    if m:
        month_str = m.group(1).lower().rstrip('_').rstrip('-')
        year = int(m.group(2))
        month = MONTH_MAP.get(month_str)
        if month:
            return (year, month)
    return (None, None)


def parse_date(date_str: str) -> Tuple[Optional[str], str]:
    """Parse date string: 01FEB23 -> (2023-02-01, D), C -> (None, C), U -> (None, U)."""
    if not date_str or not isinstance(date_str, str):
        return (None, 'U')
    ds = date_str.strip()
    if ds.upper() in ('C', 'CURRENT'):
        return (None, 'C')
    if ds.upper() in ('U', 'UNAVAILABLE'):
        return (None, 'U')
    m = re.match(r'(\d{1,2})([A-Za-z]{3})(\d{2,4})', ds)
    if m:
        day = m.group(1).zfill(2)
        mon_str = m.group(2).upper()
        yr = m.group(3)
        mon_map = {'JAN':'01','FEB':'02','MAR':'03','APR':'04','MAY':'05','JUN':'06',
                    'JUL':'07','AUG':'08','SEP':'09','OCT':'10','NOV':'11','DEC':'12'}
        mon = mon_map.get(mon_str)
        if mon:
            year = yr if len(yr) == 4 else '20' + yr
            return (f'{year}-{mon}-{day}', 'D')
    return (None, 'U')


def parse_text_eb_rows(text: str, year: int, month: int, chart: str,
                       source_file: str, page_ref: str) -> List[dict]:
    """Parse EB data from text lines. Works for both legacy and modern formats.
    
    Handles:
      1st C 08NOV22 01FEB22 C C
      2nd 01APR23 22APR20 01OCT12 01APR23 01APR23
      Other Workers 22MAY21 01JAN18 08JUN13 22MAY21 22MAY21
      Other 01MAY05 22APR03 01JUN02 01MAY05 01MAY05  (legacy split)
    """
    lines = text.split('\n')
    rows = []
    ingested_at = datetime.now(timezone.utc)

    for i, raw_line in enumerate(lines):
        line = raw_line.strip()

        # "1st", "2nd", "3rd", "4th", "5th" followed by values
        m = re.match(r'^(1st|2nd|3rd|4th|5th)\s+(.+)$', line)
        if m:
            cat_raw = m.group(1)
            rest = m.group(2).split()
            if len(rest) >= 5:
                category = CATEGORY_MAP.get(cat_raw, cat_raw)
                for ci, country in enumerate(COUNTRIES_ORDERED):
                    if ci < len(rest):
                        cutoff_date, status_flag = parse_date(rest[ci])
                        rows.append({
                            'bulletin_year': year,
                            'bulletin_month': month,
                            'chart': chart,
                            'category': category,
                            'country': country,
                            'cutoff_date': cutoff_date,
                            'status_flag': status_flag,
                            'source_file': source_file,
                            'page_ref': page_ref,
                            'ingested_at': ingested_at,
                        })
            continue

        # "Other Workers" + 5 values
        m = re.match(r'^Other\s*Workers?\s+(.+)$', line)
        if m:
            rest = m.group(1).split()
            if len(rest) >= 5:
                for ci, country in enumerate(COUNTRIES_ORDERED):
                    cutoff_date, status_flag = parse_date(rest[ci])
                    rows.append({
                        'bulletin_year': year,
                        'bulletin_month': month,
                        'chart': chart,
                        'category': 'EB3-Other',
                        'country': country,
                        'cutoff_date': cutoff_date,
                        'status_flag': status_flag,
                        'source_file': source_file,
                        'page_ref': page_ref,
                        'ingested_at': ingested_at,
                    })
            continue

        # "Other" + 5 values (legacy: "Other" on one line, "Workers" on next)
        m = re.match(r'^Other\s+(\S+\s+\S+\s+\S+\s+\S+\s+\S+)', line)
        if m and 'Workers' not in line:
            rest = m.group(1).split()
            if len(rest) >= 5:
                for ci, country in enumerate(COUNTRIES_ORDERED):
                    cutoff_date, status_flag = parse_date(rest[ci])
                    rows.append({
                        'bulletin_year': year,
                        'bulletin_month': month,
                        'chart': chart,
                        'category': 'EB3-Other',
                        'country': country,
                        'cutoff_date': cutoff_date,
                        'status_flag': status_flag,
                        'source_file': source_file,
                        'page_ref': page_ref,
                        'ingested_at': ingested_at,
                    })

    return rows


def parse_pdfplumber_table(table: list, year: int, month: int, chart: str,
                           source_file: str, page_ref: str) -> List[dict]:
    """Parse a pdfplumber-extracted table (modern PDFs)."""
    rows = []
    ingested_at = datetime.now(timezone.utc)
    if not table or len(table) < 2:
        return rows

    header = table[0]
    country_cols = []
    for cell in header[1:]:
        if not cell:
            continue
        cell_clean = cell.replace('\n', ' ').strip()
        country = None
        for k, v in COUNTRY_MAP.items():
            if k.lower() in cell_clean.lower():
                country = v
                break
        country_cols.append(country or cell_clean)

    for data_row in table[1:]:
        if not data_row or not data_row[0]:
            continue
        cat_raw = data_row[0].replace('\n', ' ').strip()
        if 'Set Aside' in cat_raw or 'Certain' in cat_raw:
            continue
        category = CATEGORY_MAP.get(cat_raw, cat_raw)
        if not category.startswith('EB'):
            continue

        for ci, country in enumerate(country_cols):
            if ci + 1 >= len(data_row):
                break
            date_str = str(data_row[ci + 1]).strip() if data_row[ci + 1] else ''
            cutoff_date, status_flag = parse_date(date_str)
            rows.append({
                'bulletin_year': year,
                'bulletin_month': month,
                'chart': chart,
                'category': category,
                'country': country,
                'cutoff_date': cutoff_date,
                'status_flag': status_flag,
                'source_file': source_file,
                'page_ref': page_ref,
                'ingested_at': ingested_at,
            })
    return rows


def main():
    print("=" * 70)
    print("FIX 4 (v2): VISA BULLETIN — MERGE + LEGACY + DEDUPE")
    print("=" * 70)

    with open("configs/paths.yaml") as f:
        paths = yaml.safe_load(f)
    data_root = Path(paths["data_root"])
    artifacts_root = Path(paths["artifacts_root"])
    output_dir = artifacts_root / "tables" / "fact_cutoffs"
    metrics_dir = artifacts_root / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = metrics_dir / "fact_cutoffs_dedupe.log"
    log_lines = []

    def log(msg):
        print(msg)
        log_lines.append(msg)

    # ── A) Read existing fact_cutoffs ──────────────────────────
    log("\n[A] Reading existing fact_cutoffs partitioned parquet")
    existing_parts = []
    if output_dir.exists():
        for part_dir in sorted(output_dir.glob("bulletin_year=*")):
            by = int(part_dir.name.split('=')[1])
            for month_dir in sorted(part_dir.glob("bulletin_month=*")):
                bm = int(month_dir.name.split('=')[1])
                for pf in month_dir.glob("*.parquet"):
                    try:
                        pdf = pd.read_parquet(pf)
                        pdf['bulletin_year'] = by
                        pdf['bulletin_month'] = bm
                        existing_parts.append(pdf)
                    except Exception as e:
                        log(f"  WARN: {pf}: {e}")
        if existing_parts:
            df_existing = pd.concat(existing_parts, ignore_index=True)
            log(f"  Loaded {len(df_existing)} existing rows from {len(existing_parts)} files")
        else:
            df_existing = pd.DataFrame()
            log("  No existing partitions found")
    else:
        df_existing = pd.DataFrame()
        log("  No existing fact_cutoffs directory")

    # ── B) Parse ALL PDFs ──────────────────────────────────────
    log("\n[B] Parsing Visa Bulletin PDFs")
    vb_dir = data_root / "Visa_Bulletin"
    pdf_files = sorted(vb_dir.glob("**/*.pdf"))
    log(f"  Found {len(pdf_files)} PDFs")

    new_rows = []
    files_ok = 0
    files_skip = 0

    for idx, pdf_file in enumerate(pdf_files, 1):
        year, month = parse_filename_enhanced(pdf_file.name)
        if not year or not month:
            files_skip += 1
            continue

        try:
            rel_path = str(pdf_file.relative_to(data_root))
        except ValueError:
            rel_path = str(pdf_file)

        is_legacy = year <= 2015  # 2015 early months also use legacy text layout
        rows_from_file = []

        try:
            with pdfplumber.open(pdf_file) as pdf:
                for page_num, page in enumerate(pdf.pages):
                    text = page.extract_text() or ''
                    text_upper = text.upper()

                    if is_legacy:
                        # Legacy: look for EB data in text (no FAD/DFF headers)
                        if '1st' in text or '1ST' in text:
                            rows = parse_text_eb_rows(text, year, month, 'FAD',
                                                      rel_path, f"page_{page_num+1}")
                            rows_from_file.extend(rows)
                    else:
                        # Modern: use pdfplumber tables + text fallback
                        tables = page.extract_tables()

                        for t in tables:
                            if not t or len(t) < 2:
                                continue
                            header_text = ' '.join(str(c) for c in t[0] if c).upper()
                            if 'EMPLOYMENT' in header_text or ('CHARGE' in header_text and 'FAMILY' not in header_text):
                                # Check if header has Family-sponsored indicator
                                if any('F1' in str(r[0] if r and r[0] else '') or
                                       'F2' in str(r[0] if r and r[0] else '')
                                       for r in t[1:3]):
                                    continue  # skip family-sponsored tables
                                chart = 'FAD'
                                if 'DATES FOR FILING' in text_upper:
                                    chart = 'DFF'
                                rows = parse_pdfplumber_table(t, year, month, chart,
                                                              rel_path, f"page_{page_num+1}")
                                rows_from_file.extend(rows)

                        # Text fallback for FAD
                        if 'FINAL ACTION DATES' in text_upper and ('EMPLOYMENT' in text_upper or '1st' in text):
                            rows = parse_text_eb_rows(text, year, month, 'FAD',
                                                      rel_path, f"page_{page_num+1}")
                            rows_from_file.extend(rows)

                        # Text fallback for DFF
                        if 'DATES FOR FILING' in text_upper and ('EMPLOYMENT' in text_upper or '1st' in text):
                            rows = parse_text_eb_rows(text, year, month, 'DFF',
                                                      rel_path, f"page_{page_num+1}")
                            rows_from_file.extend(rows)

            if rows_from_file:
                new_rows.extend(rows_from_file)
                files_ok += 1
                if is_legacy:
                    log(f"  [{idx}] LEGACY OK {pdf_file.name}: {len(rows_from_file)} rows")
            else:
                files_skip += 1

        except Exception as e:
            log(f"  [{idx}] ERROR {pdf_file.name}: {e}")
            files_skip += 1

    log(f"\n  PDF parse: {files_ok} files parsed, {files_skip} skipped")
    log(f"  New rows from PDFs: {len(new_rows)}")

    # ── C) Merge + dedupe ──────────────────────────────────────
    log("\n[C] Merging and deduplicating")
    frames = []
    if len(df_existing) > 0:
        frames.append(df_existing)
    if new_rows:
        frames.append(pd.DataFrame(new_rows))

    if not frames:
        log("  ERROR: No data!")
        with open(log_path, 'w') as f:
            f.write('\n'.join(log_lines))
        return

    df = pd.concat(frames, ignore_index=True)
    df['bulletin_year'] = pd.to_numeric(df['bulletin_year'], errors='coerce').fillna(0).astype(int)
    df['bulletin_month'] = pd.to_numeric(df['bulletin_month'], errors='coerce').fillna(0).astype(int)
    df['cutoff_date'] = pd.to_datetime(df['cutoff_date'], errors='coerce')
    if 'ingested_at' not in df.columns:
        df['ingested_at'] = datetime.now(timezone.utc)
    else:
        df['ingested_at'] = pd.to_datetime(df['ingested_at'], errors='coerce')

    rows_before = len(df)
    pk_cols = ['bulletin_year', 'bulletin_month', 'chart', 'category', 'country']

    # Priority: D > C > U, has date, smallest source_file
    flag_priority = {'D': 100, 'C': 50, 'U': 0}
    df['_priority'] = df['status_flag'].map(flag_priority).fillna(0)
    df['_has_date'] = df['cutoff_date'].notna().astype(int) * 10
    df['_src_sort'] = df['source_file'].fillna('zzz')
    df['_total_score'] = df['_priority'] + df['_has_date']

    df = df.sort_values(pk_cols + ['_total_score', '_src_sort'],
                        ascending=[True]*len(pk_cols) + [False, True])
    df = df.drop_duplicates(subset=pk_cols, keep='first')
    df = df.drop(columns=['_priority', '_has_date', '_src_sort', '_total_score'])

    rows_after = len(df)
    log(f"  Before: {rows_before}, after: {rows_after}, deduped: {rows_before - rows_after}")

    pk_unique = df.duplicated(subset=pk_cols).sum() == 0
    log(f"  PK uniqueness: {'PASS' if pk_unique else 'FAIL'}")

    year_counts = df.groupby('bulletin_year').size().to_dict()
    log(f"\n  Year coverage:")
    for y in sorted(year_counts):
        log(f"    {y}: {year_counts[y]} rows")

    # ── D) Write partitions ────────────────────────────────────
    log("\n[D] Writing partitioned output")
    backup_dir = artifacts_root / "_backup" / "fact_cutoffs" / datetime.now().strftime("%Y%m%d_%H%M%S")
    if output_dir.exists():
        backup_dir.mkdir(parents=True, exist_ok=True)
        for item in output_dir.iterdir():
            if item.name.startswith('bulletin_year='):
                shutil.copytree(str(item), str(backup_dir / item.name), dirs_exist_ok=True)
        shutil.rmtree(output_dir)
        log(f"  Backed up to {backup_dir}")

    output_dir.mkdir(parents=True, exist_ok=True)
    partition_count = 0
    for (yr, mo), group in df.groupby(['bulletin_year', 'bulletin_month']):
        part_dir = output_dir / f"bulletin_year={yr}" / f"bulletin_month={int(mo):02d}"
        part_dir.mkdir(parents=True, exist_ok=True)
        gd = group.drop(columns=['bulletin_year', 'bulletin_month'])
        for c in gd.columns:
            if gd[c].dtype == 'object' or str(gd[c].dtype) == 'string':
                gd[c] = gd[c].astype(str)
                gd.loc[gd[c].isin(['None', 'nan', 'NaT', '<NA>']), c] = None
        gd.to_parquet(part_dir / "part-0.parquet", index=False, engine='pyarrow')
        partition_count += 1

    log(f"  Written {partition_count} partitions to {output_dir}")

    with open(log_path, 'w') as f:
        f.write('\n'.join(log_lines))
    log(f"  Log: {log_path}")
    log("\n✓ FIX 4 COMPLETE")


if __name__ == "__main__":
    main()
