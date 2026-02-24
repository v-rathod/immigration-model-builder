#!/usr/bin/env python3
"""
cleanup_transcripts.py  –  Enforce rolling retention policy for chat transcripts.

Keeps the 10 most-recent rotated `chat_transcript_YYYYMMDD*.md` files.
Deletes any older rotated transcripts.
NEVER deletes `chat_transcript_latest.md`.

One-time cleanup mode (--days N): delete rotated files older than N days.
Default: enforce keep-last-10 rule only.

Usage:
    python scripts/cleanup_transcripts.py            # keep-last-10 (default)
    python scripts/cleanup_transcripts.py --days 14  # one-time: delete >14 days first, then keep-last-10
    python scripts/cleanup_transcripts.py --dry-run  # preview only
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT     = Path(__file__).resolve().parent.parent
METRICS  = ROOT / "artifacts" / "metrics"
LATEST   = METRICS / "chat_transcript_latest.md"
GLOB     = "chat_transcript_*.md"
KEEP_N   = 10


def _rotated_files() -> list[Path]:
    """Rotated archives sorted newest first. Excludes chat_transcript_latest.md."""
    return sorted(
        [p for p in METRICS.glob(GLOB) if p.name != LATEST.name],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )


def run(days_threshold: int | None, dry_run: bool) -> None:
    if not METRICS.exists():
        print("metrics directory not found — nothing to clean")
        return

    archived = _rotated_files()
    print(f"Found {len(archived)} rotated transcript(s)")

    deleted = 0
    kept    = 0

    # Phase 1: delete files older than --days, if specified
    if days_threshold is not None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_threshold)
        for p in list(archived):
            mtime = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            if mtime < cutoff:
                if dry_run:
                    print(f"  DRY-RUN would delete (>{days_threshold}d): {p.name}")
                else:
                    p.unlink()
                    print(f"  deleted (>{days_threshold}d): {p.name}")
                deleted += 1
                archived.remove(p)

    # Phase 2: enforce keep-N rule on whatever remains
    for i, p in enumerate(archived):
        if i < KEEP_N:
            print(f"  keep [{i+1:2d}]: {p.name}")
            kept += 1
        else:
            if dry_run:
                print(f"  DRY-RUN would delete (>keep-{KEEP_N}): {p.name}")
            else:
                p.unlink()
                print(f"  deleted (>keep-{KEEP_N}): {p.name}")
            deleted += 1

    # Summary
    if LATEST.exists():
        size = LATEST.stat().st_size
        print(f"\n  chat_transcript_latest.md: {size:,} bytes (never deleted)")

    mode = "DRY-RUN" if dry_run else "LIVE"
    print(f"\n[{mode}] kept={kept}  deleted={deleted}  total_before={kept+deleted}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Enforce transcript retention policy")
    parser.add_argument(
        "--days", type=int, default=None, metavar="N",
        help="Also delete rotated files older than N days before applying keep-last-10",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview deletions without removing any files",
    )
    args = parser.parse_args()

    print(f"=== cleanup_transcripts  {'DRY-RUN' if args.dry_run else 'LIVE'}  "
          f"{datetime.now(timezone.utc).isoformat()} ===")
    run(days_threshold=args.days, dry_run=args.dry_run)
    print("TRANSCRIPT_CLEANUP_COMPLETE")


if __name__ == "__main__":
    main()
