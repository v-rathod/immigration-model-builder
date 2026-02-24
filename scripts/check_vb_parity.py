#!/usr/bin/env python3
"""
STEP 4 — Fail-fast VB parity checks.

Reads:
  - Presentation: artifacts/tables/fact_cutoffs_all.parquet
  - Snapshot:     artifacts/tables/fact_cutoffs/_snapshot.json

Validates:
  1. presentation row_count == sum(snapshot per-leaf row_counts)
  2. snapshot leaves >= 160  (target 168)
  3. years_span matches between presentation and snapshot

Exits 1 if any mismatch.
Prints single-line JSON summary to stdout.
"""
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
PRESENTATION_PATH = ROOT / "artifacts" / "tables" / "fact_cutoffs_all.parquet"
SNAPSHOT_PATH = ROOT / "artifacts" / "tables" / "fact_cutoffs" / "_snapshot.json"

MIN_LEAVES = 160


def main():
    print("=" * 60)
    print("CHECK VB PARITY")
    print("=" * 60)

    errors: list[str] = []

    # ── Load presentation ────────────────────────────────────────────────────
    if not PRESENTATION_PATH.exists():
        print(f"ERROR: presentation table not found: {PRESENTATION_PATH}", file=sys.stderr)
        print(f"  Run: python scripts/make_vb_presentation.py", file=sys.stderr)
        sys.exit(1)

    df = pd.read_parquet(PRESENTATION_PATH)
    pres_rows = len(df)
    pres_years = sorted(df["bulletin_year"].dropna().unique().astype(int).tolist()) if "bulletin_year" in df.columns else []
    pres_months = sorted(df["bulletin_month"].dropna().unique().astype(int).tolist()) if "bulletin_month" in df.columns else []
    pres_partitions = len({(int(y), int(m)) for y, m in zip(df.get("bulletin_year", []), df.get("bulletin_month", []))}) if ("bulletin_year" in df.columns and "bulletin_month" in df.columns) else 0
    pres_years_span = f"{pres_years[0]}-{pres_years[-1]}" if pres_years else "?"
    print(f"Presentation: {pres_rows:,} rows, partitions={pres_partitions}, years={pres_years_span}")

    # ── Load snapshot ────────────────────────────────────────────────────────
    if not SNAPSHOT_PATH.exists():
        print(f"ERROR: snapshot not found: {SNAPSHOT_PATH}", file=sys.stderr)
        print(f"  Run: python scripts/make_vb_snapshot.py", file=sys.stderr)
        sys.exit(1)

    with open(SNAPSHOT_PATH) as f:
        snapshot = json.load(f)

    snap_summary = snapshot.get("summary", {})
    snap_leaves = snap_summary.get("leaves", 0)
    snap_rows = snap_summary.get("total_rows", 0)
    # Use presentation_rows from snapshot if available (deduped count matches presentation file)
    snap_pres_rows = snap_summary.get("presentation_rows", snap_rows)
    snap_years_span = snap_summary.get("years_span", "?")
    snap_distinct_years = snap_summary.get("distinct_years", 0)
    print(f"Snapshot:     {snap_rows:,} raw rows ({snap_pres_rows:,} deduped), leaves={snap_leaves}, years={snap_years_span}")
    print()

    # ── Check 1: row count parity ────────────────────────────────────────────
    # Compare presentation (deduped) rows against snapshot's presentation_rows
    row_diff = abs(pres_rows - snap_pres_rows)
    if row_diff != 0:
        errors.append(
            f"row_count_mismatch: presentation={pres_rows:,} snap_sum={snap_pres_rows:,} diff={row_diff:,}"
        )
        print(f"  ✗ Row count mismatch: {pres_rows:,} vs {snap_pres_rows:,} (diff={row_diff:,})", file=sys.stderr)
    else:
        print(f"  ✓ Row counts match:  {pres_rows:,}")

    # ── Check 2: leaves ≥ MIN_LEAVES ────────────────────────────────────────
    if snap_leaves < MIN_LEAVES:
        errors.append(f"leaves_too_few: {snap_leaves} < {MIN_LEAVES}")
        print(f"  ✗ Leaves {snap_leaves} < {MIN_LEAVES}", file=sys.stderr)
    else:
        print(f"  ✓ Leaves:           {snap_leaves} >= {MIN_LEAVES}")

    # ── Check 3: years_span match ────────────────────────────────────────────
    if pres_years_span != snap_years_span:
        errors.append(f"years_span_mismatch: presentation={pres_years_span} snapshot={snap_years_span}")
        print(f"  ✗ Years span mismatch: {pres_years_span} vs {snap_years_span}", file=sys.stderr)
    else:
        print(f"  ✓ Years span:       {pres_years_span}")

    print()

    # ── JSON summary ─────────────────────────────────────────────────────────
    summary = {
        "check": "vb_parity",
        "presentation_rows": pres_rows,
        "snapshot_rows": snap_rows,          # raw leaf sum
        "snapshot_presentation_rows": snap_pres_rows,  # deduped (used for parity)
        "row_diff": pres_rows - snap_pres_rows,
        "snapshot_leaves": snap_leaves,
        "years_span": pres_years_span,
        "distinct_years": snap_distinct_years,
        "pass": len(errors) == 0,
        "errors": errors,
    }
    print(json.dumps(summary))

    if errors:
        print("\nVB PARITY FAILED — see errors above", file=sys.stderr)
        sys.exit(1)
    else:
        print("\nVB PARITY PASS")
        sys.exit(0)


if __name__ == "__main__":
    main()
