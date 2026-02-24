#!/usr/bin/env python3
"""
fix_fact_cutoffs_dedup.py — Atomic deduplication of fact_cutoffs partition data.

Problem: double-ingestion produced 2 part-*.parquet files per partition dir,
resulting in 14,290 rows with PK duplicates. Expected: 8,315 unique rows.

Fix: read all files, reconstruct partition columns from path, dedup on PK
keeping the row with the latest ingested_at, then rewrite each partition
as a single part-0.parquet file. Extra files are removed.

Safety: dry-run by default. Pass --write to apply changes.
"""

import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Primary key for fact_cutoffs
PK_COLS = ["bulletin_year", "bulletin_month", "chart", "category", "country"]
FACT_DIR = Path("artifacts/tables/fact_cutoffs")
LOG_PATH = Path("artifacts/metrics/audit_outputs_fix.log")


def load_all_cutoffs(fact_dir: Path) -> pd.DataFrame:
    """Read all parquet files, reconstructing partition columns from dir names."""
    files = sorted(fact_dir.rglob("*.parquet"))
    dfs = []
    for pf in files:
        part_df = pd.read_parquet(pf)
        for part in pf.parts:
            if "=" in part:
                col_name, col_val = part.split("=", 1)
                if col_name not in part_df.columns:
                    part_df[col_name] = col_val
        part_df["_src_file"] = str(pf)
        dfs.append(part_df)
    return pd.concat(dfs, ignore_index=True)


def rewrite_partition(partition_dir: Path, part_df: pd.DataFrame, dry_run: bool) -> int:
    """Rewrite partition directory: remove all files, write single part-0.parquet."""
    # Drop partition columns (they live in the dir name, not the file)
    partition_cols = []
    for part in partition_dir.parts:
        if "=" in part:
            partition_cols.append(part.split("=", 1)[0])

    write_df = part_df.drop(columns=partition_cols, errors="ignore")
    write_df = write_df.drop(columns=["_src_file"], errors="ignore")

    out_file = partition_dir / "part-0.parquet"
    existing_files = list(partition_dir.glob("*.parquet"))

    if dry_run:
        print(f"  [dry-run] {partition_dir.name}: {len(existing_files)} files → 1 file, {len(write_df)} rows")
        return len(write_df)

    # Remove all existing parquet files
    for f in existing_files:
        f.unlink()

    # Write deduplicated single file
    write_df.to_parquet(out_file, index=False, engine="pyarrow")
    return len(write_df)


def main():
    parser = argparse.ArgumentParser(description="Deduplicate fact_cutoffs partitions")
    parser.add_argument("--write", action="store_true", help="Apply changes (default: dry-run)")
    args = parser.parse_args()

    dry_run = not args.write
    mode = "DRY-RUN" if dry_run else "WRITE"
    started = datetime.now(timezone.utc)

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    log_lines = [
        f"fix_fact_cutoffs_dedup — {started.isoformat()}",
        f"Mode: {mode}",
        "",
    ]

    print("=" * 60)
    print(f"FIX FACT_CUTOFFS DEDUP [{mode}]")
    print("=" * 60)

    if not FACT_DIR.exists():
        print(f"ERROR: {FACT_DIR} not found")
        return 1

    # ── Load all data ──────────────────────────────────────────────────────────
    print("Loading all fact_cutoffs partitions...")
    df = load_all_cutoffs(FACT_DIR)
    df["bulletin_year"] = df["bulletin_year"].astype(int)
    df["bulletin_month"] = df["bulletin_month"].astype(int)

    total_before = len(df)
    files_before = len(list(FACT_DIR.rglob("*.parquet")))
    print(f"  Files:    {files_before}")
    print(f"  Rows:     {total_before:,}")

    pk_dupes = df.duplicated(subset=PK_COLS).sum()
    print(f"  PK dupes: {pk_dupes:,}")

    log_lines += [
        f"Before: {total_before:,} rows, {files_before} files, {pk_dupes:,} PK dupes",
    ]

    # ── Deduplicate: keep row with latest ingested_at per PK ──────────────────
    if "ingested_at" in df.columns:
        df_sorted = df.sort_values("ingested_at", ascending=False)
    else:
        df_sorted = df

    df_deduped = df_sorted.drop_duplicates(subset=PK_COLS, keep="first").copy()
    total_after = len(df_deduped)
    print(f"\nAfter dedup: {total_after:,} rows  (removed {total_before - total_after:,})")
    log_lines.append(f"After dedup: {total_after:,} rows (removed {total_before - total_after:,})")

    # ── Rewrite partitions ────────────────────────────────────────────────────
    print("\nRewriting partitions...")
    partition_dirs = sorted({
        Path(r["_src_file"]).parent
        for _, r in df_deduped.iterrows()
    })
    # Also cover partitions that might only have duplicate rows (rare, but cover it)
    all_partition_dirs = sorted({
        pf.parent for pf in FACT_DIR.rglob("*.parquet")
    })

    total_written = 0
    changed_count = 0

    for pdir in all_partition_dirs:
        # Filter rows for this partition
        by = {}
        for part in pdir.parts:
            if "=" in part:
                col, val = part.split("=", 1)
                by[col] = int(val)  # bulletin_year and bulletin_month are int
        if not by:
            continue

        mask = pd.Series([True] * len(df_deduped))
        for col, val in by.items():
            if col in df_deduped.columns:
                mask = mask & (df_deduped[col] == val)

        part_rows = df_deduped[mask].copy()
        n_files = len(list(pdir.glob("*.parquet")))

        if n_files > 1 or len(part_rows) == 0:
            rewrite_partition(pdir, part_rows, dry_run)
            changed_count += 1

        total_written += len(part_rows)

    print(f"\nPartition dirs processed: {len(all_partition_dirs)}")
    print(f"Dirs rewritten (had >1 file or 0 rows): {changed_count}")
    print(f"Total rows written: {total_written:,}")

    log_lines += [
        f"Partition dirs: {len(all_partition_dirs)}, rewritten: {changed_count}",
        f"Total rows written: {total_written:,}",
        "",
        "STATUS: " + ("DRY-RUN (no changes)" if dry_run else "APPLIED"),
    ]

    with open(LOG_PATH, "w") as f:
        f.write("\n".join(log_lines) + "\n")

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    print(f"\nElapsed: {elapsed:.1f}s")
    print(f"Log: {LOG_PATH}")
    print("=" * 60)

    if dry_run:
        print("✓ Dry-run complete. Run with --write to apply.")
    else:
        print("✓ Deduplication applied.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
