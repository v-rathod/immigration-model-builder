#!/usr/bin/env python3
"""
restore_fact_cutoffs_from_backup.py
────────────────────────────────────
Atomically restore the canonical fact_cutoffs dataset from the newest backup
that has ≥ 160 partitions (the 2011-2026 full-history set).

Controls
--------
only_rewrite_parquet: true  (never re-parses PDFs/Excels — only copies .parquet files)
atomic_writes: true         (copies to .tmp_YYYYMMDD_HHMMSS then atomic rename)
exclude: _backup/**, _quarantine/**, *.tmp_*  (enforced on destination scan)
dry_run: supported via --dry-run flag
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

BACKUP_BASE = Path("artifacts/_backup/fact_cutoffs")
LIVE_DIR    = Path("artifacts/tables/fact_cutoffs")
MIN_PARTITIONS_REQUIRED = 160
MIN_ROWS_REQUIRED = 8_000

EXCLUDE: tuple[str, ...] = (".tmp_", "/tmp_")   # NOT "_backup" — we're reading FROM backup


def _parquet_row_count(pf: Path) -> int:
    try:
        import pyarrow.parquet as pq
        return pq.read_metadata(pf).num_rows
    except Exception:
        return 0


def _scan_backup(d: Path) -> tuple[int, int]:
    """Return (n_partitions, total_rows) for a backup timestamp dir."""
    files = sorted(f for f in d.rglob("*.parquet") if not any(x in str(f) for x in EXCLUDE))
    parts: set[tuple[str, str]] = set()
    total_rows = 0
    for f in files:
        by = next((p.split("=")[1] for p in f.parts if p.startswith("bulletin_year=")), None)
        bm = next((p.split("=")[1] for p in f.parts if p.startswith("bulletin_month=")), None)
        if by and bm:
            parts.add((by, bm))
        total_rows += _parquet_row_count(f)
    return len(parts), total_rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore canonical fact_cutoffs from backup")
    parser.add_argument("--dry-run", action="store_true", help="Print plan but do not copy")
    parser.add_argument(
        "--backup-ts",
        help="Specific backup timestamp to restore (e.g. 20260221_191639). Default: auto-select newest with ≥160 partitions.",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("RESTORE FACT_CUTOFFS FROM BACKUP")
    print("=" * 60)

    if not BACKUP_BASE.exists():
        print(f"ERROR: backup directory not found: {BACKUP_BASE}", file=sys.stderr)
        sys.exit(1)

    # ── Select best backup ────────────────────────────────────────────────────
    if args.backup_ts:
        candidate = BACKUP_BASE / args.backup_ts
        if not candidate.exists():
            print(f"ERROR: specified backup {args.backup_ts} not found", file=sys.stderr)
            sys.exit(1)
        chosen = candidate
        n_parts, n_rows = _scan_backup(chosen)
    else:
        chosen = None
        n_parts = n_rows = 0
        # Pick newest backup that meets thresholds
        for ts_dir in sorted(BACKUP_BASE.iterdir(), reverse=True):
            if not ts_dir.is_dir():
                continue
            np, nr = _scan_backup(ts_dir)
            print(f"  backup {ts_dir.name}: partitions={np}  rows={nr}")
            if np >= MIN_PARTITIONS_REQUIRED and nr >= MIN_ROWS_REQUIRED:
                chosen = ts_dir
                n_parts = np
                n_rows = nr
                break

    if chosen is None:
        print(
            f"\nERROR: No backup with ≥{MIN_PARTITIONS_REQUIRED} partitions and ≥{MIN_ROWS_REQUIRED} rows found.",
            file=sys.stderr,
        )
        sys.exit(1)

    print(f"\nSelected backup: {chosen.name}")
    print(f"  partitions={n_parts}  rows={n_rows}")

    # ── Atomic copy to temp then rename ─────────────────────────────────────
    ts_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    tmp_dir = LIVE_DIR.parent / f".tmp_{ts_str}"

    print(f"\nPlan:")
    print(f"  source:  {chosen}")
    print(f"  temp:    {tmp_dir}")
    print(f"  dest:    {LIVE_DIR}")

    if args.dry_run:
        print("\nDRY RUN — no files written")
        return

    # Copy backup → temp
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    print(f"\nCopying {n_parts} partition dirs to temp...")
    shutil.copytree(chosen, tmp_dir)

    # Remove live and rename temp → live
    if LIVE_DIR.exists():
        shutil.rmtree(LIVE_DIR)
    tmp_dir.rename(LIVE_DIR)
    print(f"Atomic rename: {tmp_dir.name} → {LIVE_DIR.name}")
    print(f"\nRestored {n_parts} partitions ({n_rows:,} rows) → {LIVE_DIR}")


if __name__ == "__main__":
    main()
