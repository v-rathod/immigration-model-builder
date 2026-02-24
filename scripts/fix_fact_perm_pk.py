#!/usr/bin/env python3
"""
fix_fact_perm_pk.py — Enforce within-partition case_number uniqueness in fact_perm.

fact_perm is stored as a Hive-partitioned directory:
    artifacts/tables/fact_perm/fiscal_year=XXXX/*.parquet

The fiscal_year column lives ONLY in the directory path, not inside the parquet files.
PK is (case_number, fiscal_year); within a fiscal_year partition, case_number must be unique.

Cross-partition duplicate case_numbers (same case appearing in multiple annual files) are
intentional — DOL publishes pending cases in successive annual disclosure files.
This script ONLY deduplicates within each individual partition.

Dedup priority (keep first):
  1. Latest decision_date (most recent processing record)
  2. Most non-null values across all columns (most complete record)
  3. Lexicographically smallest source_file (deterministic tiebreak)

Writes are atomic: tmp file → unlink original → rename.

Usage:
    python3 scripts/fix_fact_perm_pk.py [--dry-run] [--report artifacts/metrics/perm_pk_report.md]
"""
import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
PERM_DIR = TABLES / "fact_perm"
EXCL = ("_backup", "_quarantine", ".tmp_", "/tmp_")

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def _excl(p: Path) -> bool:
    return any(x in str(p) for x in EXCL)


def _partition_key(leaf: Path) -> str | None:
    """Extract fiscal_year value from a path like .../fiscal_year=2024/file.parquet"""
    for part in leaf.parts:
        if part.startswith("fiscal_year="):
            return part.split("=", 1)[1]
    return None


def dedup_partition(pf: Path, fy: str, dry_run: bool) -> dict:
    """Deduplicate a single parquet file on case_number. Returns stats dict."""
    df = pd.read_parquet(pf)
    before = len(df)

    if "case_number" not in df.columns:
        return {"file": str(pf), "fy": fy, "before": before, "after": before,
                "removed": 0, "note": "SKIP: no case_number column"}

    if before == 0:
        return {"file": str(pf), "fy": fy, "before": 0, "after": 0,
                "removed": 0, "note": "SKIP: empty file"}

    # Sort priority: latest decision_date first, then most non-nulls, then source_file asc
    if "decision_date" in df.columns:
        df["decision_date"] = pd.to_datetime(df["decision_date"], errors="coerce")
        df["_dd_sort"] = df["decision_date"].apply(
            lambda x: x.timestamp() if pd.notna(x) else -1e18
        )
    else:
        df["_dd_sort"] = 0.0

    df["_nonnull_count"] = df.notna().sum(axis=1)
    df["_src"] = df["source_file"].fillna("") if "source_file" in df.columns else ""

    df.sort_values(
        ["_dd_sort", "_nonnull_count", "_src"],
        ascending=[False, False, True],
        inplace=True,
    )
    df.drop_duplicates(subset=["case_number"], keep="first", inplace=True)
    df.drop(columns=["_dd_sort", "_nonnull_count", "_src"], inplace=True)
    df.reset_index(drop=True, inplace=True)

    after = len(df)
    removed = before - after

    if removed > 0 and not dry_run:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        tmp = pf.parent / f".tmp_{ts}_{pf.name}"
        df.to_parquet(tmp, index=False)
        pf.unlink()
        tmp.rename(pf)
        log.info(f"  [{fy}] {pf.name}: {before:,} → {after:,} rows (removed {removed:,})")
    elif removed > 0:
        log.info(f"  [{fy}] DRY-RUN {pf.name}: would remove {removed:,} dups")
    else:
        log.info(f"  [{fy}] {pf.name}: CLEAN ({before:,} rows, 0 dups)")

    return {
        "file": pf.name,
        "fy": fy,
        "before": before,
        "after": after,
        "removed": removed,
        "note": "OK",
    }


def main():
    parser = argparse.ArgumentParser(description="Enforce within-partition case_number uniqueness in fact_perm")
    parser.add_argument("--dry-run", action="store_true", help="Analyse only; do not write files")
    parser.add_argument(
        "--report",
        default="artifacts/metrics/perm_pk_report.md",
        help="Output markdown report path",
    )
    args = parser.parse_args()

    ts = datetime.now(timezone.utc).isoformat()
    log.info(f"=== fix_fact_perm_pk  {'DRY-RUN' if args.dry_run else 'LIVE'}  {ts} ===")

    if not PERM_DIR.exists():
        log.error(f"fact_perm directory not found: {PERM_DIR}")
        sys.exit(1)

    leaves = sorted(
        f for f in PERM_DIR.rglob("*.parquet") if not _excl(f)
    )
    log.info(f"Found {len(leaves)} partition file(s)")

    results = []
    total_before = total_after = 0

    for pf in leaves:
        fy = _partition_key(pf) or "unknown"
        stats = dedup_partition(pf, fy, dry_run=args.dry_run)
        results.append(stats)
        total_before += stats["before"]
        total_after += stats["after"]

    total_removed = total_before - total_after
    pass_flag = "PASS" if total_removed == 0 else f"FIXED ({total_removed:,} dups removed)"
    if args.dry_run and total_removed > 0:
        pass_flag = f"DRY-RUN: {total_removed:,} dups would be removed"

    log.info(f"\nTotal rows  before: {total_before:,}")
    log.info(f"Total rows  after:  {total_after:,}")
    log.info(f"Dups removed:       {total_removed:,}")
    log.info(f"Status: {pass_flag}")

    # Write markdown report
    report_path = ROOT / args.report
    METRICS.mkdir(parents=True, exist_ok=True)
    lines = [
        "# fact_perm PK Uniqueness Report",
        f"Generated: {ts}  |  Mode: {'DRY-RUN' if args.dry_run else 'LIVE'}",
        "",
        f"**Status:** {pass_flag}",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Partitions checked | {len(results)} |",
        f"| Total rows before  | {total_before:,} |",
        f"| Total rows after   | {total_after:,} |",
        f"| Dups removed       | {total_removed:,} |",
        "",
        "## Per-File Detail",
        "",
        "| FY | File | Before | After | Removed | Note |",
        "|----|------|--------|-------|---------|------|",
    ]
    for r in results:
        lines.append(
            f"| {r['fy']} | {r['file']} | {r['before']:,} | {r['after']:,} | {r['removed']:,} | {r['note']} |"
        )
    lines += [
        "",
        "## Design Notes",
        "",
        "- PK enforced: **(case_number)** within each `fiscal_year=XXXX` partition.",
        "- Cross-FY duplicate case_numbers are **intentional** (DOL annual disclosure overlap).",
        "- Dedup priority: latest `decision_date` > most non-null values > smallest `source_file`.",
        "- Writes are atomic (tmp → unlink → rename).",
    ]
    report_path.write_text("\n".join(lines) + "\n")
    log.info(f"Report written: {report_path}")

    if not args.dry_run and total_removed > 0:
        print(f"fix_fact_perm_pk: FIXED — removed {total_removed:,} within-partition dups")
    elif total_removed == 0:
        print(f"fix_fact_perm_pk: CLEAN — 0 within-partition dups across {len(results)} files")
    else:
        print(f"fix_fact_perm_pk: DRY-RUN — {total_removed:,} dups would be removed from {len(results)} files")


if __name__ == "__main__":
    main()
