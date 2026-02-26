#!/usr/bin/env python3
"""
build_fact_bls_ces.py
Parse BLS Current Employment Statistics (CES) JSON → fact_bls_ces.parquet

Schema:
  series_id      TEXT   — BLS series identifier (e.g. CES0000000001)
  series_title   TEXT   — human-readable series description
  year           INT    — calendar year
  period         TEXT   — BLS period code (M01-M12 for monthly)
  period_name    TEXT   — month name (January, February, ...)
  value          FLOAT  — employment value (thousands)
  is_preliminary BOOL   — True if data is preliminary
  snapshot_date  TEXT   — date of the JSON snapshot file (YYYY-MM-DD)
  source_file    TEXT   — filename parsed from
  ingested_at    TEXT   — UTC ISO-8601 timestamp

Data source:
  BLS Current Employment Statistics (CES) via API
  Downloaded by Horizon (P1) daily as JSON files
  Files: BLS/ces_YYYYMMDD.json
  Contains total nonfarm + total private employment (2 series)

Usage note:
  CES provides macro employment context for labor market analysis.
  Series CES0000000001 = Total Nonfarm Employment (thousands)
  Series CES0500000001 = Total Private Employment (thousands)

Primary Key: (series_id, year, period, snapshot_date)
"""
import argparse
import json
import logging
import pathlib
import re
import sys
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

# Series title mappings
SERIES_TITLES = {
    "CES0000000001": "Total Nonfarm Employment (Seasonally Adjusted, Thousands)",
    "CES0500000001": "Total Private Employment (Seasonally Adjusted, Thousands)",
}

SCHEMA_COLS = [
    "series_id", "series_title", "year", "period", "period_name",
    "value", "is_preliminary", "snapshot_date",
    "source_file", "ingested_at",
]


def parse_ces_json(path: pathlib.Path) -> pd.DataFrame:
    """Parse a single BLS CES JSON file."""
    fname = path.name
    now_ts = datetime.now(timezone.utc).isoformat()

    # Extract snapshot date from filename: ces_YYYYMMDD.json
    date_match = re.search(r"ces_(\d{4})(\d{2})(\d{2})", fname)
    if date_match:
        snap_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
    else:
        snap_date = datetime.now().strftime("%Y-%m-%d")

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("Error reading %s: %s", fname, e)
        return pd.DataFrame(columns=SCHEMA_COLS)

    # Check for API errors
    status = data.get("status", "")
    if status != "REQUEST_SUCCEEDED":
        log.warning("API status '%s' in %s: %s", status, fname, data.get("message", ""))
        return pd.DataFrame(columns=SCHEMA_COLS)

    results = data.get("Results", {})
    series_list = results.get("series", [])

    if not series_list:
        log.warning("No series data in %s", fname)
        return pd.DataFrame(columns=SCHEMA_COLS)

    rows = []
    for series in series_list:
        series_id = series.get("seriesID", "")
        series_title = SERIES_TITLES.get(series_id, series_id)

        for dp in series.get("data", []):
            is_prelim = False
            footnotes = dp.get("footnotes", [])
            for fn in footnotes:
                if fn.get("code") == "P":
                    is_prelim = True

            rows.append({
                "series_id": series_id,
                "series_title": series_title,
                "year": int(dp.get("year", 0)),
                "period": dp.get("period", ""),
                "period_name": dp.get("periodName", ""),
                "value": float(dp.get("value", 0)),
                "is_preliminary": is_prelim,
                "snapshot_date": snap_date,
                "source_file": fname,
                "ingested_at": now_ts,
            })

    df = pd.DataFrame(rows, columns=SCHEMA_COLS)
    log.info("  %s: %d data points (%d series, snapshot=%s)",
             fname, len(df), len(series_list), snap_date)
    return df


def main():
    ap = argparse.ArgumentParser(description="Build fact_bls_ces from BLS CES JSON files")
    ap.add_argument("--downloads", default=None,
                    help="Path to BLS/ directory")
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
            args.downloads = str(pathlib.Path(data_root) / "BLS")
        if args.out is None:
            args.out = str(pathlib.Path(artifacts_root) / "tables" / "fact_bls_ces.parquet")

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    log.info("=== Building fact_bls_ces ===")
    log.info("Source: %s", droot)
    log.info("Output: %s", out_path)

    # Discover JSON files
    json_files = sorted(droot.glob("ces_*.json"))
    log.info("Found %d CES JSON files", len(json_files))

    if not json_files:
        log.warning("No BLS CES JSON files found — writing empty stub")
        pd.DataFrame(columns=SCHEMA_COLS).to_parquet(out_path, index=False)
        log.info("Wrote empty stub to %s", out_path)
        return

    # Parse all files
    frames = []
    for f in json_files:
        df = parse_ces_json(f)
        if len(df) > 0:
            frames.append(df)

    if not frames:
        log.warning("All files failed to parse — writing empty stub")
        pd.DataFrame(columns=SCHEMA_COLS).to_parquet(out_path, index=False)
        return

    merged = pd.concat(frames, ignore_index=True)
    log.info("Total rows before dedup: %d", len(merged))

    # Dedup — keep the latest snapshot for each (series_id, year, period)
    # Sort by snapshot_date descending so drop_duplicates keeps the latest
    merged = merged.sort_values("snapshot_date", ascending=False)
    pk = ["series_id", "year", "period"]
    before = len(merged)
    merged = merged.drop_duplicates(subset=pk, keep="first").reset_index(drop=True)
    deduped = before - len(merged)
    if deduped > 0:
        log.info("Removed %d duplicate rows (kept latest snapshot per series/year/period)", deduped)

    # Re-sort properly
    merged = merged.sort_values(["series_id", "year", "period"]).reset_index(drop=True)

    # Validate PK
    dup_count = merged.duplicated(subset=pk).sum()
    if dup_count > 0:
        log.warning("PK not fully unique: %d remaining duplicates", dup_count)
    else:
        log.info("PK is unique (%d rows)", len(merged))

    # Write
    merged = merged[SCHEMA_COLS]
    merged.to_parquet(out_path, index=False)

    year_range = f"{merged['year'].min()}-{merged['year'].max()}"
    log.info("=== DONE ===")
    log.info("  Rows:       %s", f"{len(merged):,}")
    log.info("  Year range: %s", year_range)
    log.info("  Series:     %d (%s)", merged["series_id"].nunique(),
             ", ".join(merged["series_id"].unique()))
    log.info("  Snapshots:  %d", merged["snapshot_date"].nunique())
    log.info("  Output:     %s", out_path)


if __name__ == "__main__":
    main()
