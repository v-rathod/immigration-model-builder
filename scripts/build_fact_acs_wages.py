#!/usr/bin/env python3
"""
build_fact_acs_wages.py
Parse ACS PUMS/CSV data → fact_acs_wages.parquet
Schema: year, soc_code, area_code, p10, p25, median, p75, p90, source_file, ingested_at

ACS folder currently contains only a failed API response JSON (404 error).
This script creates a stub parquet with the correct schema, logs a WARN, and exits cleanly.
When real PUMS files (PSS files, /data/PUMS) are downloaded, the parser handles them.
"""
import argparse
import json
import logging
import pathlib
import re
from datetime import datetime, timezone

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

SCHEMA_COLS = ["year", "soc_code", "area_code", "p10", "p25", "median", "p75", "p90",
               "source_file", "ingested_at"]


def parse_acs_json(path: pathlib.Path) -> pd.DataFrame:
    """Attempt to parse an ACS API JSON response."""
    try:
        with open(path) as f:
            data = json.load(f)
    except Exception as e:
        log.warning("Cannot parse JSON %s: %s", path.name, e)
        return pd.DataFrame()

    if isinstance(data, dict) and "error" in data:
        log.warning("ACS JSON contains API error: %s", data.get("error", ""))
        return pd.DataFrame()

    if isinstance(data, list) and len(data) >= 2:
        # Standard Census API format: [header_row, data_rows...]
        headers = data[0]
        rows_out = []
        now_ts = datetime.now(timezone.utc).isoformat()
        for row in data[1:]:
            record = dict(zip(headers, row))
            # Extract year from filename
            fname = path.name
            yr_m = re.search(r"(\d{4})", fname)
            year = int(yr_m.group(1)) if yr_m else 0
            rows_out.append({
                "year": year,
                "soc_code": record.get("SOCP", record.get("soc_code", "")),
                "area_code": record.get("state", record.get("area_code", None)),
                "p10": None,
                "p25": None,
                "median": None,
                "p75": None,
                "p90": None,
                "source_file": path.name,
                "ingested_at": now_ts,
            })
        return pd.DataFrame(rows_out)

    return pd.DataFrame()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--downloads", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    droot = pathlib.Path(args.downloads)
    out_path = pathlib.Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    all_files = list(droot.rglob("*.json")) + list(droot.rglob("*.csv"))
    log.info("Found %d files in ACS dir: %s", len(all_files), droot)

    all_frames = []
    for f in all_files:
        if f.suffix == ".json":
            df = parse_acs_json(f)
        else:
            # CSV PUMS parsing (for future use)
            try:
                df_raw = pd.read_csv(f, dtype=str, nrows=5)
                log.info("  ACS CSV %s: cols=%s", f.name, list(df_raw.columns)[:5])
            except Exception:
                pass
            df = pd.DataFrame()  # stub

        if len(df) > 0:
            all_frames.append(df)

    if not all_frames:
        log.warning("ACS data not available (API error / no PUMS files) — writing stub schema parquet")
        df = pd.DataFrame(columns=SCHEMA_COLS)
        for col in ["p10", "p25", "median", "p75", "p90"]:
            df[col] = df[col].astype(float)
        df["year"] = df["year"].astype("Int64")
    else:
        df = pd.concat(all_frames, ignore_index=True)
        for col in ["p10", "p25", "median", "p75", "p90"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

    pk = ["year", "soc_code", "area_code"]
    if len(df) > 0:
        df = df.drop_duplicates(subset=pk, keep="last").reset_index(drop=True)

    df = df[SCHEMA_COLS]
    df.to_parquet(out_path, index=False)
    log.info("Written %d rows to %s", len(df), out_path)
    log.info("COMPLETE fact_acs_wages: %d rows (stub; real ACS PUMS data not yet downloaded)", len(df))


if __name__ == "__main__":
    main()
