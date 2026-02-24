#!/usr/bin/env python3
"""
check_vb_partitions.py  (spec A)
─────────────────────────────────
Fail-fast two-level partition checker for fact_cutoffs.

Enumerates EXACTLY:
    artifacts/tables/fact_cutoffs/bulletin_year=*/bulletin_month=*/

Outputs a single-line JSON for grep/CI:
    {"check":"vb_partitions","years_span":"<min>-<max>","years_count":<n>,
     "month_partitions":<n_leaves>,"rows":<total>}

Exit 1 if:  month_partitions < 160  OR  rows < 8000

Controls
--------
only_rewrite_parquet: true
exclude: _backup/**, _quarantine/**, *.tmp_*
no re-ingestion of PDFs/Excels; reads parquet footers only
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pyarrow.parquet as pq

FACT_CUTOFFS_DIR = Path("artifacts/tables/fact_cutoffs")
MIN_PARTITIONS   = 160     # target 168
MIN_ROWS         = 8_000   # target ≈ 8315

EXCLUDE: tuple[str, ...] = ("_backup", "_quarantine", ".tmp_", "/tmp_")


def _excluded(p: Path) -> bool:
    s = str(p)
    return any(ex in s for ex in EXCLUDE)


def _row_count(pf: Path) -> int:
    try:
        return pq.read_metadata(pf).num_rows
    except Exception:
        return 0


def main() -> None:
    if not FACT_CUTOFFS_DIR.exists():
        print(
            f"ERROR: {FACT_CUTOFFS_DIR} not found — run restore_fact_cutoffs_from_backup.py first",
            file=sys.stderr,
        )
        sys.exit(1)

    # Enumerate all leaf parquet files under bulletin_year=*/bulletin_month=*/
    leaf_dirs: set[tuple[str, str]] = set()
    total_rows = 0
    years: set[str] = set()

    parquet_files = sorted(
        f for f in FACT_CUTOFFS_DIR.rglob("*.parquet")
        if not _excluded(f)
    )

    if not parquet_files:
        print("ERROR: no parquet files found under fact_cutoffs", file=sys.stderr)
        sys.exit(1)

    for pf in parquet_files:
        by = next((p.split("=")[1] for p in pf.parts if p.startswith("bulletin_year=")), None)
        bm = next((p.split("=")[1] for p in pf.parts if p.startswith("bulletin_month=")), None)
        if by and bm:
            leaf_dirs.add((by, bm))
            years.add(by)
        total_rows += _row_count(pf)

    n_leaves = len(leaf_dirs)
    year_list = sorted(years)
    min_year = year_list[0] if year_list else "N/A"
    max_year = year_list[-1] if year_list else "N/A"
    n_years  = len(year_list)

    result = {
        "check":            "vb_partitions",
        "years_span":       f"{min_year}-{max_year}",
        "years_count":      n_years,
        "month_partitions": n_leaves,
        "rows":             total_rows,
    }
    print(json.dumps(result))

    # Detailed breakdown (stderr so it doesn't pollute JSON stdout)
    print(f"\nLeaf-partition detail ({n_years} years × up to 12 months = {n_leaves} found):",
          file=sys.stderr)
    for yr in year_list:
        months = sorted(bm for (by, bm) in leaf_dirs if by == yr)
        print(f"  {yr}: {','.join(months)}  ({len(months)} leaves)", file=sys.stderr)

    # ── Fail-fast criteria ────────────────────────────────────────────────────
    errors: list[str] = []
    if n_leaves < MIN_PARTITIONS:
        errors.append(
            f"FAIL month_partitions={n_leaves} < min={MIN_PARTITIONS}  "
            f"(restore_fact_cutoffs_from_backup.py if pre-2015 data is missing)"
        )
    if total_rows < MIN_ROWS:
        errors.append(f"FAIL rows={total_rows} < min={MIN_ROWS}")

    if errors:
        print(file=sys.stderr)
        for e in errors:
            print(e, file=sys.stderr)
        sys.exit(1)

    print("\nOK: all thresholds met", file=sys.stderr)


if __name__ == "__main__":
    main()
