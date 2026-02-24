#!/usr/bin/env python3
"""
STEP 2 — Authoritative VB snapshot (loader-owned).

Enumerates:  artifacts/tables/fact_cutoffs/bulletin_year=*/bulletin_month=*/
Writes:      artifacts/tables/fact_cutoffs/_snapshot.json

Per-leaf entry: {"bulletin_year":YYYY, "bulletin_month":MM, "row_count":N, "files":[...]}
Totals: rows, leaves, years_span, distinct_years, file_list_checksum
"""
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Global controls ──────────────────────────────────────────────────────────
EXCLUDE_PATTERNS: tuple[str, ...] = (
    "_backup", "_quarantine", ".tmp_", "/tmp_",
)

ROOT = Path(__file__).resolve().parent.parent
FACT_CUTOFFS_DIR = ROOT / "artifacts" / "tables" / "fact_cutoffs"
SNAPSHOT_PATH = ROOT / "artifacts" / "tables" / "fact_cutoffs" / "_snapshot.json"
PRESENTATION_PATH = ROOT / "artifacts" / "tables" / "fact_cutoffs_all.parquet"


def _is_excluded(path: Path) -> bool:
    return any(pat in str(path) for pat in EXCLUDE_PATTERNS)


def _leaf_meta(leaf: Path) -> tuple[int, int]:
    parts = leaf.parts
    year_part = next((p for p in parts if p.startswith("bulletin_year=")), None)
    month_part = next((p for p in parts if p.startswith("bulletin_month=")), None)
    if year_part is None or month_part is None:
        raise ValueError(f"Cannot parse leaf path: {leaf}")
    return int(year_part.split("=", 1)[1]), int(month_part.split("=", 1)[1])


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build VB authoritative snapshot JSON")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("MAKE VB SNAPSHOT")
    print("=" * 60)

    if not FACT_CUTOFFS_DIR.exists():
        print(f"ERROR: {FACT_CUTOFFS_DIR} not found", file=sys.stderr)
        sys.exit(1)

    # Enumerate all leaf parquet files
    all_leaves = sorted(
        [f for f in FACT_CUTOFFS_DIR.rglob("*.parquet") if not _is_excluded(f)]
    )
    print(f"Total leaf files:   {len(all_leaves)}")

    # Group by (bulletin_year, bulletin_month)
    groups: dict[tuple[int, int], list[Path]] = {}
    for leaf in all_leaves:
        key = _leaf_meta(leaf)
        groups.setdefault(key, []).append(leaf)

    partitions_count = len(groups)
    all_years = sorted({yr for yr, _ in groups})
    years_span = f"{all_years[0]}-{all_years[-1]}" if all_years else "?"
    distinct_years = len(all_years)

    print(f"Partitions (yr×mo): {partitions_count}")
    print(f"Years span:         {years_span}")
    print(f"Distinct years:     {distinct_years}")
    print()

    # Per-leaf records
    leaf_records: list[dict] = []
    total_rows = 0
    all_file_names: list[str] = []

    for (yr, mo), files in sorted(groups.items()):
        row_count = 0
        for f in files:
            try:
                import pyarrow.parquet as pq
                row_count += pq.read_metadata(f).num_rows
            except Exception:
                row_count += len(pd.read_parquet(f))
        file_names = sorted(str(f.relative_to(ROOT)) for f in files)
        leaf_records.append(
            {
                "bulletin_year": yr,
                "bulletin_month": mo,
                "row_count": row_count,
                "files": file_names,
            }
        )
        total_rows += row_count
        all_file_names.extend(file_names)
        print(f"  {yr}-{mo:02d}: {row_count:>5} rows  ({len(files)} file(s))")

    print()

    # Checksum of file list (sorted)
    checksum = hashlib.sha256("\n".join(sorted(all_file_names)).encode()).hexdigest()[:16]

    # Also capture presentation (deduped) row count if available
    presentation_rows: int | None = None
    if PRESENTATION_PATH.exists():
        try:
            presentation_rows = len(pd.read_parquet(PRESENTATION_PATH))
        except Exception:
            pass

    summary: dict = {
        "leaves": partitions_count,
        "total_rows": total_rows,
        "years_span": years_span,
        "distinct_years": distinct_years,
        "file_list_checksum": checksum,
    }
    if presentation_rows is not None:
        summary["presentation_rows"] = presentation_rows

    snapshot = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "summary": summary,
        "partitions": leaf_records,
    }

    print(f"Summary:")
    print(f"  leaves:         {partitions_count}")
    print(f"  total_rows:     {total_rows:,}")
    if presentation_rows is not None:
        print(f"  presentation_rows: {presentation_rows:,}")
    print(f"  years_span:     {years_span}")
    print(f"  distinct_years: {distinct_years}")
    print(f"  checksum:       {checksum}")
    print()

    if args.dry_run:
        print("[dry-run] Skipping write.")
        return

    SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Atomic write
    tmp_path = SNAPSHOT_PATH.parent / f".tmp_snapshot_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    with open(tmp_path, "w") as f:
        json.dump(snapshot, f, indent=2)
    if SNAPSHOT_PATH.exists():
        SNAPSHOT_PATH.unlink()
    tmp_path.rename(SNAPSHOT_PATH)
    print(f"✓ Snapshot written: {SNAPSHOT_PATH}")


if __name__ == "__main__":
    main()
