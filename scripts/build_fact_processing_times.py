#!/usr/bin/env python3
"""
build_fact_processing_times.py
Parse USCIS Processing Times snapshots → fact_processing_times.parquet

Schema:
  snapshot_date         TEXT   — date when the snapshot was captured (YYYY-MM-DD)
  snapshot_month        TEXT   — YYYY-MM for partitioning/grouping
  form                  TEXT   — USCIS form number (e.g. I-140, I-485)
  category              TEXT   — petition sub-category
  office                TEXT   — USCIS service center / field office
  processing_time_min   FLOAT — minimum processing time (in months)
  processing_time_max   FLOAT — maximum processing time (in months)
  unit                  TEXT   — time unit (usually 'Months')
  source_file           TEXT   — filename parsed from
  ingested_at           TEXT   — UTC ISO-8601 timestamp

Data source:
  USCIS Case Processing Times (https://egov.uscis.gov/processing-times/)
  Previously scraped by Horizon (P1), but the download directory has been deleted
  because the USCIS page is a Vue.js SPA and no usable data was extracted.
  Structure (when present): raw/ contains HTML (SPA shell), parsed/ contains CSV extracts

Note:
  The P1 source directory (USCIS_Processing_Times/) has been deleted.
  This builder will produce a 0-row stub unless --downloads points to a
  valid directory with parsed CSV data. Future P1 enhancements may capture
  the API responses directly via headless browser.

Primary Key: (snapshot_date, form, category, office)
"""
import argparse
import logging
import pathlib
import re
import sys
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SCHEMA_COLS = [
    "snapshot_date", "snapshot_month", "form", "category", "office",
    "processing_time_min", "processing_time_max", "unit",
    "source_file", "ingested_at",
]


def parse_processing_times_csv(path: pathlib.Path, snapshot_month: str) -> pd.DataFrame:
    """Parse a single processing times CSV file."""
    fname = path.name
    now_ts = datetime.now(timezone.utc).isoformat()

    try:
        df = pd.read_csv(path, dtype=str, encoding="utf-8-sig")
    except Exception as e:
        log.warning("Error reading %s: %s", path, e)
        return pd.DataFrame(columns=SCHEMA_COLS)

    if df.empty or len(df) == 0:
        log.warning("Empty or header-only CSV: %s (P1 SPA parsing may have failed)", path)
        return pd.DataFrame(columns=SCHEMA_COLS)

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Ensure required columns
    for c in ["form", "category", "office"]:
        if c not in df.columns:
            df[c] = ""

    # Snapshot date
    if "snapshot_date" not in df.columns:
        # Infer from directory name (e.g., 2026-02)
        df["snapshot_date"] = f"{snapshot_month}-01"

    df["snapshot_month"] = snapshot_month

    # Numeric columns
    for nc in ["processing_time_min", "processing_time_max"]:
        if nc in df.columns:
            df[nc] = pd.to_numeric(df[nc], errors="coerce")
        else:
            df[nc] = None

    if "unit" not in df.columns:
        df["unit"] = "Months"

    # Provenance
    df["source_file"] = fname
    df["ingested_at"] = now_ts

    # Select output columns
    for c in SCHEMA_COLS:
        if c not in df.columns:
            df[c] = None
    df = df[SCHEMA_COLS]

    log.info("  %s (%s): %d rows", fname, snapshot_month, len(df))
    return df


def main():
    ap = argparse.ArgumentParser(description="Build fact_processing_times from USCIS processing time snapshots")
    ap.add_argument("--downloads", default=None,
                    help="Path to USCIS_Processing_Times/ directory (deleted from P1; stub if missing)")
    ap.add_argument("--out", default=None,
                    help="Output parquet path")
    args = ap.parse_args()

    # Resolve paths
    if args.downloads is None or args.out is None:
        try:
            import yaml
            cfg = yaml.safe_load(open("configs/paths.yaml"))
            data_root = cfg.get("data_root", "")
            artifacts_root = cfg.get("artifacts_root", "./artifacts")
        except Exception:
            data_root = "/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads"
            artifacts_root = "./artifacts"

        if args.downloads is None:
            args.downloads = str(pathlib.Path(data_root) / "USCIS_Processing_Times")
        if args.out is None:
            args.out = str(pathlib.Path(artifacts_root) / "tables" / "fact_processing_times.parquet")

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("=== Building fact_processing_times ===")
    log.info("Source: %s", droot)
    log.info("Output: %s", out_path)

    # Discover parsed CSV files (in parsed/<YYYY-MM>/ directories)
    parsed_dir = droot / "parsed"
    frames = []

    if parsed_dir.exists():
        for month_dir in sorted(parsed_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            snapshot_month = month_dir.name  # e.g., "2026-02"
            for csv_file in sorted(month_dir.glob("*.csv")):
                df = parse_processing_times_csv(csv_file, snapshot_month)
                if len(df) > 0:
                    frames.append(df)

    # Also check raw directory for any additional CSVs
    raw_dir = droot / "raw"
    if raw_dir.exists():
        for month_dir in sorted(raw_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            snapshot_month = month_dir.name
            for csv_file in sorted(month_dir.glob("*.csv")):
                df = parse_processing_times_csv(csv_file, snapshot_month)
                if len(df) > 0:
                    frames.append(df)

    if not frames:
        log.warning("No processing times data found — writing empty stub")
        log.warning("  P1 source directory (USCIS_Processing_Times/) has been deleted")
        log.warning("  USCIS page is Vue.js SPA — no usable data was extracted")
        log.warning("  Requires headless browser or direct API integration in P1")
        stub = pd.DataFrame(columns=SCHEMA_COLS)
        stub.to_parquet(out_path, index=False)
        log.info("Wrote empty stub (0 rows) to %s", out_path)
        return

    merged = pd.concat(frames, ignore_index=True)

    # Dedup on PK
    pk = ["snapshot_date", "form", "category", "office"]
    before = len(merged)
    merged = merged.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)
    deduped = before - len(merged)
    if deduped > 0:
        log.info("Removed %d duplicates", deduped)

    merged = merged.sort_values(pk).reset_index(drop=True)

    # Write
    merged.to_parquet(out_path, index=False)

    log.info("=== DONE ===")
    log.info("  Rows: %s", f"{len(merged):,}")
    log.info("  Snapshots: %s", merged["snapshot_month"].nunique())
    log.info("  Forms: %s", merged["form"].nunique())
    log.info("  Output: %s", out_path)


if __name__ == "__main__":
    main()
