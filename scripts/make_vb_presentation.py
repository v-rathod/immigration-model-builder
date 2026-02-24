#!/usr/bin/env python3
"""
STEP 1 — Build VB presentation table (parquet-only, no PDF re-ingestion).

Scans:  artifacts/tables/fact_cutoffs/bulletin_year=*/bulletin_month=*/
Writes: artifacts/tables/fact_cutoffs_all.parquet  (single canonical flat file)

Dedupe key: (bulletin_year, bulletin_month, chart, category, country)
Preference: status_flag D > C > U; then non-null cutoff_date; then lex-smallest source_file.
"""
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# ── Global controls ──────────────────────────────────────────────────────────
CHUNK_SIZE: int = int(os.environ.get("CHUNK_SIZE", 250_000))
CONCURRENCY: int = int(os.environ.get("CONCURRENCY", 3))
ONLY_REWRITE_PARQUET: bool = True
ATOMIC_WRITES: bool = True

EXCLUDE_PATTERNS: tuple[str, ...] = (
    "_backup", "_quarantine", ".tmp_", "/tmp_",
)

ROOT = Path(__file__).resolve().parent.parent
FACT_CUTOFFS_DIR = ROOT / "artifacts" / "tables" / "fact_cutoffs"
OUT_PATH = ROOT / "artifacts" / "tables" / "fact_cutoffs_all.parquet"

# Preference order for status_flag during dedupe
STATUS_PREF = {"D": 0, "C": 1, "U": 2}

PK_COLS = ["bulletin_year", "bulletin_month", "chart", "category", "country"]


def _is_excluded(path: Path) -> bool:
    s = str(path)
    return any(pat in s for pat in EXCLUDE_PATTERNS)


def _leaf_meta(leaf: Path) -> tuple[int, int]:
    """Extract (bulletin_year, bulletin_month) from leaf directory path."""
    # …/bulletin_year=2015/bulletin_month=3/data.parquet
    parts = leaf.parts
    year_part = next((p for p in parts if p.startswith("bulletin_year=")), None)
    month_part = next((p for p in parts if p.startswith("bulletin_month=")), None)
    if year_part is None or month_part is None:
        raise ValueError(f"Cannot parse leaf path: {leaf}")
    return int(year_part.split("=", 1)[1]), int(month_part.split("=", 1)[1])


def scan_leaves() -> list[Path]:
    leaves = sorted(
        [
            f
            for f in FACT_CUTOFFS_DIR.rglob("*.parquet")
            if not _is_excluded(f)
        ]
    )
    return leaves


def read_all_leaves(leaves: list[Path]) -> pd.DataFrame:
    dfs: list[pd.DataFrame] = []
    for leaf in leaves:
        yr, mo = _leaf_meta(leaf)
        df = pd.read_parquet(leaf)
        df["bulletin_year"] = yr
        df["bulletin_month"] = mo
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _status_rank(s: pd.Series) -> pd.Series:
    return s.map(STATUS_PREF).fillna(99).astype(int)


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Dedupe on PK_COLS with preference:
      1. status_flag D > C > U (lower rank = preferred)
      2. non-null cutoff_date (nulls last)
      3. lexicographically smallest source_file
    """
    df = df.copy()
    df["_rank_status"] = _status_rank(df["status_flag"].fillna("U"))
    df["_rank_cutoff_null"] = df["cutoff_date"].isna().astype(int)  # 0=has date, 1=null
    df["_rank_source"] = df["source_file"].fillna("zzz")

    df.sort_values(
        ["_rank_status", "_rank_cutoff_null", "_rank_source"],
        inplace=True,
    )
    df.drop_duplicates(subset=PK_COLS, keep="first", inplace=True)
    df.drop(columns=["_rank_status", "_rank_cutoff_null", "_rank_source"], inplace=True)
    return df.reset_index(drop=True)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build VB presentation table")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 60)
    print("MAKE VB PRESENTATION TABLE")
    print("=" * 60)

    if not FACT_CUTOFFS_DIR.exists():
        print(f"ERROR: fact_cutoffs directory not found: {FACT_CUTOFFS_DIR}", file=sys.stderr)
        sys.exit(1)

    # Scan leaves
    leaves = scan_leaves()
    print(f"Leaves seen:        {len(leaves)}")
    if len(leaves) == 0:
        print("ERROR: no parquet leaves found", file=sys.stderr)
        sys.exit(1)

    # Collect year×month partitions
    partitions: set[tuple[int, int]] = set()
    for leaf in leaves:
        yr, mo = _leaf_meta(leaf)
        partitions.add((yr, mo))
    partitions_count = len(partitions)
    if partitions:
        years = sorted({yr for yr, _ in partitions})
        years_span = f"{years[0]}–{years[-1]}"
        distinct_years = len(years)
    else:
        years_span = "?"
        distinct_years = 0

    print(f"Partitions (yr×mo): {partitions_count}")
    print(f"Years span:         {years_span}")
    print(f"Distinct years:     {distinct_years}")
    print()

    # Read all leaves
    print("Reading all leaves...")
    df_all = read_all_leaves(leaves)
    total_rows_before = len(df_all)
    print(f"Total rows before dedupe: {total_rows_before:,}")

    # Dedupe
    df_deduped = dedupe(df_all)
    rows_after = len(df_deduped)
    print(f"Rows after dedupe:        {rows_after:,}  (removed {total_rows_before - rows_after:,})")
    print()

    # Summary
    print(f"leaves_seen={len(leaves)}")
    print(f"total_rows_before={total_rows_before:,}")
    print(f"rows_after_dedupe={rows_after:,}")
    print(f"years_span={years_span}")
    print(f"distinct_years={distinct_years}")
    print(f"partitions_count={partitions_count}")
    print()

    if args.dry_run:
        print("[dry-run] Skipping write.")
        return

    # Atomic write
    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    tmp_path = OUT_PATH.parent / f".tmp_{ts}_fact_cutoffs_all.parquet"
    print(f"Writing → {OUT_PATH}")
    df_deduped.to_parquet(tmp_path, index=False)
    if OUT_PATH.exists():
        OUT_PATH.unlink()
    tmp_path.rename(OUT_PATH)
    print(f"✓ Written: {OUT_PATH}  ({rows_after:,} rows)")


if __name__ == "__main__":
    main()
