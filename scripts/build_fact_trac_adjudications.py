#!/usr/bin/env python3
"""
build_fact_trac_adjudications.py
Parse TRAC FOIA records → fact_trac_adjudications.parquet
Schema: fiscal_year, form, measure, value, source_file, ingested_at

TRAC folder is currently empty (0 files). This script creates an empty parquet
with the correct schema and logs a WARN. When files arrive, the tabular parser
will be invoked automatically on next run.
"""
import argparse
import logging
import pathlib
import re
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SCHEMA_COLS = ["fiscal_year", "form", "measure", "value", "source_file", "ingested_at"]


def parse_trac_csv(path: pathlib.Path) -> pd.DataFrame:
    """Parse a TRAC FOIA CSV file."""
    fname = path.name
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8", errors="replace")
    except Exception as e:
        log.warning("Cannot parse %s: %s", fname, e)
        return pd.DataFrame()

    fy_raw = str(path.parent.name)
    fy_m = re.search(r"(\d{4})", fy_raw)
    default_fy = f"FY{fy_m.group(1)}" if fy_m else "FY_UNKNOWN"

    rows_out = []
    # Auto-map columns
    col_lower = {c.lower().strip(): c for c in df.columns}
    fy_col = col_lower.get("fiscal_year") or col_lower.get("fy") or col_lower.get("year")
    form_col = col_lower.get("form") or col_lower.get("form_type")
    measure_col = col_lower.get("measure") or col_lower.get("metric") or col_lower.get("category")
    value_col = col_lower.get("value") or col_lower.get("count") or col_lower.get("total")

    for _, row in df.iterrows():
        fy = str(row[fy_col]).strip() if fy_col else default_fy
        if not fy.startswith("FY"):
            yr_m = re.search(r"(\d{4})", fy)
            fy = f"FY{yr_m.group(1)}" if yr_m else default_fy
        form = str(row[form_col]).strip() if form_col else "UNKNOWN"
        measure = str(row[measure_col]).strip() if measure_col else "UNKNOWN"
        value_raw = str(row[value_col]).strip() if value_col else "0"
        try:
            value = float(value_raw.replace(",", ""))
        except ValueError:
            value = 0.0

        rows_out.append({
            "fiscal_year": fy,
            "form": form,
            "measure": measure,
            "value": value,
            "source_file": fname,
            "ingested_at": now_ts,
        })

    return pd.DataFrame(rows_out)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--downloads", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    csv_files = sorted(droot.rglob("*.csv"))
    log.info("Found %d CSV files in %s", len(csv_files), droot)

    if not csv_files:
        log.warning("TRAC folder is empty — writing empty schema parquet (no FOIA CSVs detected)")
        df = pd.DataFrame(columns=SCHEMA_COLS)
        df["value"] = df["value"].astype(float)
        df.to_parquet(out_path, index=False)
        log.info("Written empty schema (0 rows) to %s", out_path)
        log.info("COMPLETE fact_trac_adjudications: 0 rows (TRAC files not yet available)")
        return

    all_frames = []
    for f in csv_files:
        df = parse_trac_csv(f)
        if len(df) > 0:
            all_frames.append(df)

    if not all_frames:
        log.warning("No parseable TRAC data; writing empty parquet")
        df = pd.DataFrame(columns=SCHEMA_COLS)
        df["value"] = df["value"].astype(float)
    else:
        df = pd.concat(all_frames, ignore_index=True)
        df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0.0)

    pk = ["fiscal_year", "form", "measure"]
    df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    df = df[SCHEMA_COLS]

    df.to_parquet(out_path, index=False)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_trac_adjudications: %d rows", len(df))


if __name__ == "__main__":
    main()
