#!/usr/bin/env python3
"""
build_fact_uscis_approvals.py
Parse USCIS_IMMIGRATION → fact_uscis_approvals.parquet
Schema: fiscal_year, form, category, approvals, denials, source_file, ingested_at

Files: ~245 files (77 XLSX + 168 CSV) with performance data for various USCIS forms.
"""
import argparse
import logging
import pathlib
import re
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

FORM_PATTERN = re.compile(r"(I-?\d{3,4}[A-Z]?)", re.IGNORECASE)
FISCAL_YEAR_PATTERN = re.compile(r"fy\s*(\d{4})", re.IGNORECASE)
QUARTER_PATTERN = re.compile(r"q(\d)", re.IGNORECASE)

# Column aliases → canonical
COL_ALIASES = {
    "fiscal year": "fiscal_year",
    "fy": "fiscal_year",
    "quarter": "quarter",
    "qtr": "quarter",
    "form type": "form",
    "form": "form",
    "category": "category",
    "preference": "category",
    "class": "category",
    "type": "category",
    "receipts": "receipts",
    "approved": "approvals",
    "approvals": "approvals",
    "approval": "approvals",
    "denied": "denials",
    "denials": "denials",
    "denial": "denials",
    "withdrawn": "withdrawn",
    "pending": "pending",
}


def extract_form_from_name(fname: str) -> str:
    """Extract form number from filename."""
    m = FORM_PATTERN.search(fname)
    if m:
        return m.group(1).upper().replace(" ", "-")
    return "UNKNOWN"


def extract_fy_from_name(fname: str) -> str:
    """Extract fiscal year from filename."""
    m = FISCAL_YEAR_PATTERN.search(fname)
    if m:
        return f"FY{m.group(1)}"
    m2 = re.search(r"(\d{4})", fname)
    if m2:
        return f"FY{m2.group(1)}"
    return "FY_UNKNOWN"


def normalize_columns(cols: list) -> dict:
    """Map raw columns to canonical names."""
    mapping = {}
    for c in cols:
        cl = str(c).strip().lower()
        if cl in COL_ALIASES:
            mapping[c] = COL_ALIASES[cl]
        else:
            for alias, canon in COL_ALIASES.items():
                if alias in cl:
                    if c not in mapping:  # don't overwrite
                        mapping[c] = canon
                    break
    return mapping


def detect_format(df_raw: pd.DataFrame) -> str:
    """Detect if USCIS file is 'standard' (rows=entities, cols=metrics)
    or 'transposed' (rows=metrics like Approved/Denied, cols=years)."""
    # Look in first 15 rows for metric names in first column
    metric_words = {"approved", "denied", "approval", "denial", "receipts", "total",
                    "pending", "withdrawn", "certified"}
    for i in range(min(15, len(df_raw))):
        cell = str(df_raw.iloc[i, 0]).strip().lower() if pd.notna(df_raw.iloc[i, 0]) else ""
        if any(w in cell for w in metric_words):
            # Also check if other columns look like years
            row = df_raw.iloc[i].tolist()[1:]
            year_vals = sum(1 for c in row if re.search(r"20\d{2}|19\d{2}", str(c)))
            if year_vals >= 2:
                return "transposed"
    return "standard"


def safe_int(val) -> int:
    if val is None:
        return 0
    # Handle Series (duplicate column names)
    if isinstance(val, pd.Series):
        val = val.iloc[0] if len(val) > 0 else 0
    try:
        if pd.isna(val):
            return 0
    except (TypeError, ValueError):
        pass
    try:
        return int(float(str(val).replace(",", "").replace("*", "")))
    except (ValueError, TypeError):
        return 0


def find_header_row(df_raw: pd.DataFrame) -> int:
    """Find the row index that looks like a column header."""
    for i in range(min(10, len(df_raw))):
        row = df_raw.iloc[i].tolist()
        cell_strs = [str(c).strip().lower() for c in row if pd.notna(c) and str(c).strip()]
        # Header row has >= 3 text values and contains known column keywords
        known_kw = {"fiscal", "fy", "approved", "denied", "receipt", "category", "form", "quarter", "approval"}
        matches = sum(1 for c in cell_strs if any(k in c for k in known_kw))
        if matches >= 2 and len(cell_strs) >= 3:
            return i
    return 0


def parse_transposed_xlsx(df_raw: pd.DataFrame, default_form: str, fname: str) -> list:
    """Parse USCIS XLSX where rows are metrics and columns are fiscal years."""
    now_ts = datetime.now(timezone.utc).isoformat()
    rows_out = []

    # Find the header row (has fiscal year columns)
    h_idx = 0
    for i in range(min(10, len(df_raw))):
        row = df_raw.iloc[i].tolist()
        year_count = sum(1 for c in row[1:] if re.search(r"20\d{2}|19\d{2}", str(c)))
        if year_count >= 2:
            h_idx = i
            break

    header = [str(c).strip() if pd.notna(c) else f"col_{j}" for j, c in enumerate(df_raw.iloc[h_idx].tolist())]
    data = df_raw.iloc[h_idx + 1:].reset_index(drop=True)

    # Collect approved/denied rows by year
    year_approvals = {}
    year_denials = {}
    category = "ALL"

    for _, row in data.iterrows():
        metric_raw = str(row.iloc[0]).strip().lower() if pd.notna(row.iloc[0]) else ""
        if not metric_raw or metric_raw in ("nan",):
            continue

        is_approval = any(k in metric_raw for k in ["approved", "approval"])
        is_denial = any(k in metric_raw for k in ["denied", "denial"])

        if not is_approval and not is_denial:
            # Could be category/subcategory
            if len(metric_raw) > 0 and metric_raw not in ("total", "grand total"):
                category = metric_raw[:50].upper()
            continue

        for j, col_name in enumerate(header[1:], 1):
            yr_m = re.search(r"(20\d{2}|19\d{2})", str(col_name))
            if not yr_m:
                continue
            fy = f"FY{yr_m.group(1)}"
            val_raw = row.iloc[j] if j < len(row) else None
            val = safe_int(val_raw)

            if fy not in year_approvals:
                year_approvals[fy] = 0
                year_denials[fy] = 0

            if is_approval:
                year_approvals[fy] = max(year_approvals[fy], val)
            elif is_denial:
                year_denials[fy] = max(year_denials[fy], val)

    for fy in set(year_approvals) | set(year_denials):
        approvals = year_approvals.get(fy, 0)
        denials = year_denials.get(fy, 0)
        if approvals == 0 and denials == 0:
            continue
        rows_out.append({
            "fiscal_year": fy,
            "form": default_form,
            "category": category,
            "approvals": approvals,
            "denials": denials,
            "source_file": fname,
            "ingested_at": now_ts,
        })

    return rows_out


def parse_xlsx(path: pathlib.Path) -> pd.DataFrame:
    """Parse a USCIS XLSX file."""
    fname = path.name
    default_form = extract_form_from_name(fname)
    default_fy = extract_fy_from_name(fname)
    now_ts = datetime.now(timezone.utc).isoformat()

    rows_out = []
    try:
        xl = pd.ExcelFile(path)
    except Exception as e:
        log.debug("Cannot open XLSX %s: %s", fname, e)
        return pd.DataFrame()

    for sheet in xl.sheet_names:
        try:
            df_raw = pd.read_excel(xl, sheet_name=sheet, header=None, dtype=str)
        except Exception:
            continue

        if df_raw.empty or len(df_raw) < 3:
            continue

        fmt = detect_format(df_raw)

        if fmt == "transposed":
            rows_out.extend(parse_transposed_xlsx(df_raw, default_form, fname))
            continue

        h_idx = find_header_row(df_raw)
        header = [str(c).strip() if pd.notna(c) else "" for c in df_raw.iloc[h_idx].tolist()]
        col_map = normalize_columns(header)

        df = pd.read_excel(xl, sheet_name=sheet, header=h_idx, dtype=str)
        df = df.rename(columns={c: col_map.get(c, c) for c in df.columns})

        # Drop duplicate columns – keep first occurrence of each name
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

        # Ensure we have at least approvals or denials
        if "approvals" not in df.columns and "denials" not in df.columns:
            continue

        if "fiscal_year" not in df.columns:
            df["fiscal_year"] = default_fy
        else:
            df["fiscal_year"] = df["fiscal_year"].fillna(default_fy)

        if "form" not in df.columns:
            df["form"] = default_form
        if "category" not in df.columns:
            df["category"] = "ALL"

        df = df.dropna(how="all")

        for _, row in df.iterrows():
            fy_raw = str(row.get("fiscal_year", default_fy)).strip()
            fy_m = FISCAL_YEAR_PATTERN.search(fy_raw)
            fy = f"FY{fy_m.group(1)}" if fy_m else fy_raw
            if not fy.startswith("FY"):
                yr_m = re.search(r"(\d{4})", fy_raw)
                fy = f"FY{yr_m.group(1)}" if yr_m else default_fy

            form = str(row.get("form", default_form)).strip().upper()
            category = str(row.get("category", "ALL")).strip()
            approvals = safe_int(row.get("approvals", 0))
            denials = safe_int(row.get("denials", 0))

            if approvals == 0 and denials == 0:
                continue
            if not form or form == "NAN":
                form = default_form

            rows_out.append({
                "fiscal_year": fy,
                "form": form,
                "category": category,
                "approvals": approvals,
                "denials": denials,
                "source_file": fname,
                "ingested_at": now_ts,
            })

    return pd.DataFrame(rows_out) if rows_out else pd.DataFrame()


def parse_csv_uscis(path: pathlib.Path) -> pd.DataFrame:
    """Parse a USCIS CSV file."""
    fname = path.name
    default_form = extract_form_from_name(fname)
    default_fy = extract_fy_from_name(fname)
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        df_raw = pd.read_csv(path, header=None, dtype=str, encoding="utf-8", errors="replace")
    except Exception as e:
        log.debug("Cannot read CSV %s: %s", fname, e)
        return pd.DataFrame()

    if df_raw.empty or len(df_raw) < 3:
        return pd.DataFrame()

    # Check for transposed format
    if detect_format(df_raw) == "transposed":
        rows_out = parse_transposed_xlsx(df_raw, default_form, fname)
        return pd.DataFrame(rows_out) if rows_out else pd.DataFrame()

    h_idx = find_header_row(df_raw)
    try:
        df = pd.read_csv(path, header=h_idx, dtype=str, encoding="utf-8", errors="replace")
    except Exception:
        return pd.DataFrame()

    col_map = normalize_columns(list(df.columns))
    df = df.rename(columns={c: col_map.get(c, c) for c in df.columns})
    # Drop duplicate columns
    df = df.loc[:, ~df.columns.duplicated(keep="first")]

    if "approvals" not in df.columns and "denials" not in df.columns:
        return pd.DataFrame()

    if "fiscal_year" not in df.columns:
        df["fiscal_year"] = default_fy
    if "form" not in df.columns:
        df["form"] = default_form
    if "category" not in df.columns:
        df["category"] = "ALL"

    rows_out = []
    for _, row in df.iterrows():
        fy_raw = str(row.get("fiscal_year", default_fy)).strip()
        fy_m = FISCAL_YEAR_PATTERN.search(fy_raw)
        fy = f"FY{fy_m.group(1)}" if fy_m else None
        if not fy:
            yr_m = re.search(r"(\d{4})", fy_raw)
            fy = f"FY{yr_m.group(1)}" if yr_m else default_fy

        form = str(row.get("form", default_form)).strip().upper()
        if not form or form == "NAN":
            form = default_form
        category = str(row.get("category", "ALL")).strip()
        approvals = safe_int(row.get("approvals", 0))
        denials = safe_int(row.get("denials", 0))

        if approvals == 0 and denials == 0:
            continue

        rows_out.append({
            "fiscal_year": fy,
            "form": form,
            "category": category,
            "approvals": approvals,
            "denials": denials,
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

    xlsx_files = sorted(droot.rglob("*.xlsx")) + sorted(droot.rglob("*.xls"))
    csv_files = sorted(droot.rglob("*.csv"))
    log.info("Found %d XLSX, %d CSV files in %s", len(xlsx_files), len(csv_files), droot)

    all_frames = []
    parsed_ok = 0
    total_files = len(xlsx_files) + len(csv_files)

    for f in xlsx_files:
        df = parse_xlsx(f)
        if len(df) > 0:
            all_frames.append(df)
            parsed_ok += 1

    for f in csv_files:
        df = parse_csv_uscis(f)
        if len(df) > 0:
            all_frames.append(df)
            parsed_ok += 1

    log.info("Parsed %d/%d files successfully", parsed_ok, total_files)

    if not all_frames:
        log.warning("No data extracted; creating empty parquet")
        df = pd.DataFrame(columns=["fiscal_year", "form", "category", "approvals", "denials", "source_file", "ingested_at"])
    else:
        df = pd.concat(all_frames, ignore_index=True)

    df["approvals"] = pd.to_numeric(df["approvals"], errors="coerce").fillna(0).astype(int)
    df["denials"] = pd.to_numeric(df["denials"], errors="coerce").fillna(0).astype(int)
    df = df[(df["approvals"] >= 0) & (df["denials"] >= 0)]

    pk = ["fiscal_year", "form", "category"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df.sort_values(pk).reset_index(drop=True)
    df = df[["fiscal_year", "form", "category", "approvals", "denials", "source_file", "ingested_at"]]

    dup_count = df.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.warning("PK not unique after dedup: %d remaining", dup_count)

    df.to_parquet(out_path, index=False)
    coverage_pct = 100.0 * parsed_ok / max(total_files, 1)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_uscis_approvals: %d rows, parse coverage=%.1f%%", len(df), coverage_pct)
    fy_range = sorted(df["fiscal_year"].unique()) if len(df) > 0 else []
    log.info("FY range: %s", fy_range)


if __name__ == "__main__":
    main()
