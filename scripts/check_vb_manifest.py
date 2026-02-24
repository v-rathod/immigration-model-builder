#!/usr/bin/env python3
"""
check_vb_manifest.py
────────────────────
Verify fact_cutoffs partitions match loader expectations.

Enumerates ALL leaf parquet files under:
    artifacts/tables/fact_cutoffs/bulletin_year=*/bulletin_month=*/

Sums row counts from parquet footers (no data deserialization).
Counts unique (bulletin_year, bulletin_month) partition pairs.

Output (always):
    LOADER_COUNT=<rows> PARTITIONS=<n> YEARS=<min>-<max>

Exit codes:
    0 — partitions ≥ 160 AND rows ≥ 8000
    1 — partitions < 160 OR rows < 8000
"""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow.parquet as pq

# ── Configuration ──────────────────────────────────────────────────
FACT_CUTOFFS_DIR = Path("artifacts/tables/fact_cutoffs")
MIN_PARTITIONS = 160
MIN_ROWS = 8_000

EXCLUDE: tuple[str, ...] = (
    "_backup", "_quarantine", ".tmp_", "/tmp_",
)


# ── Helpers ────────────────────────────────────────────────────────

def _excluded(p: Path) -> bool:
    s = str(p)
    return any(ex in s for ex in EXCLUDE)


def _parquet_row_count(pf: Path) -> int:
    try:
        return pq.read_metadata(pf).num_rows
    except Exception:
        return 0


def _partition_kv(path: Path) -> dict[str, str]:
    """Extract key=value pairs from all directory components of a path."""
    kv: dict[str, str] = {}
    for part in path.parts:
        if "=" in part:
            k, v = part.split("=", 1)
            kv[k] = v
    return kv


# ── Main ───────────────────────────────────────────────────────────

def main() -> None:
    if not FACT_CUTOFFS_DIR.exists():
        print(
            f"ERROR: {FACT_CUTOFFS_DIR} does not exist — fact_cutoffs not built yet",
            file=sys.stderr,
        )
        sys.exit(1)

    parquet_files = sorted(
        pf for pf in FACT_CUTOFFS_DIR.rglob("*.parquet")
        if not _excluded(pf)
    )

    if not parquet_files:
        print(f"ERROR: no parquet files found under {FACT_CUTOFFS_DIR}", file=sys.stderr)
        sys.exit(1)

    total_rows: int = 0
    partitions: set[tuple[str, str]] = set()
    years: set[str] = set()

    for pf in parquet_files:
        kv = _partition_kv(pf)
        by = kv.get("bulletin_year", "")
        bm = kv.get("bulletin_month", "")
        if by and bm:
            partitions.add((by, bm))
            years.add(by)
        total_rows += _parquet_row_count(pf)

    n_partitions = len(partitions)
    year_min = min(years) if years else "N/A"
    year_max = max(years) if years else "N/A"
    month_vals = sorted({bm for (_, bm) in partitions})
    n_months = len(month_vals)

    # --- Always print the summary line ---
    print(
        f"LOADER_COUNT={total_rows} "
        f"PARTITIONS={n_partitions} "
        f"YEARS={year_min}-{year_max} "
        f"MONTHS={n_months}"
    )

    # Detailed partition table (years × months)
    year_list = sorted(years)
    print(f"\nPartition detail ({len(year_list)} years × up to 12 months = {n_partitions} found):")
    for yr in year_list:
        months_for_yr = sorted(bm for (by, bm) in partitions if by == yr)
        print(f"  {yr}: months={','.join(months_for_yr)} ({len(months_for_yr)} partitions)")

    # --- Threshold checks ---
    errors: list[str] = []
    if n_partitions < MIN_PARTITIONS:
        note = (
            f"  NOTE: years {year_min}–{year_max} only; "
            "pre-2015 PDFs not parseable by current loader — "
            f"168 expected, {n_partitions} ingested"
        )
        errors.append(
            f"FAIL partitions={n_partitions} < min={MIN_PARTITIONS}\n{note}"
        )
    if total_rows < MIN_ROWS:
        errors.append(f"FAIL rows={total_rows} < min={MIN_ROWS}")

    print()
    if errors:
        for e in errors:
            print(e, file=sys.stderr)
        sys.exit(1)

    print("OK: all thresholds met")


if __name__ == "__main__":
    main()
