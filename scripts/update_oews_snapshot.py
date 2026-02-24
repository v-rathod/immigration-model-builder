#!/usr/bin/env python3
"""Section A: Update fact_oews snapshot JSON after OEWS 2024 ingest."""
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"
METRICS = ROOT / "artifacts" / "metrics"


def main() -> None:
    ts = datetime.now(timezone.utc).isoformat()

    # Count rows per ref_year partition
    oews_dir = TABLES / "fact_oews"
    partitions: dict = {}
    for pf in sorted(oews_dir.rglob("*.parquet")):
        ref_year = None
        for part in pf.parts:
            if part.startswith("ref_year="):
                ref_year = part.split("=")[1]
        if ref_year:
            partitions[ref_year] = partitions.get(ref_year, 0) + pq.read_metadata(pf).num_rows

    # Flat file
    flat = TABLES / "fact_oews.parquet"
    flat_rows = pq.read_metadata(flat).num_rows if flat.exists() else 0

    snap = {
        "table": "fact_oews",
        "snapshot_ts": ts,
        "flat_rows": flat_rows,
        "partitions": partitions,
        "ref_years_present": sorted(partitions.keys()),
        "coverage_note": (
            "ref_year=2023: official BLS OEWS data. "
            "ref_year=2024: synthetic fallback (copied from 2023) — "
            "official BLS 2024 data was unavailable (corrupt zip). "
            "See artifacts/metrics/fetch_oews.log for fetch details."
            if "2024" in partitions else
            "ref_year=2023: official BLS OEWS data only."
        ),
    }

    snap_path = TABLES / "fact_oews" / "_snapshot.json"
    snap_path.parent.mkdir(parents=True, exist_ok=True)
    snap_path.write_text(json.dumps(snap, indent=2))
    print(f"Snapshot updated: {snap_path}")
    print(f"  ref_years: {snap['ref_years_present']}")
    print(f"  flat_rows: {flat_rows:,}")


if __name__ == "__main__":
    main()
