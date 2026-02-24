#!/usr/bin/env python3
"""
Build fact_perm_unique_case — one row per case_number (P3 consumer view).

Reads existing fact_perm parquet partitions (no raw data re-ingestion).

Deduplication strategy (for non-null case_number):
  1. If a case appears in only one fiscal year → keep that row.
  2. If a case appears in multiple fiscal years (cross-FY duplicate):
     a. Keep the row with the LATEST decision_date.
     b. Tie-break: most non-null values across all columns.
     c. Final tie-break: lexicographically smallest source_file.
  is_crossfy_duplicate = True on the kept row when multiple FY files had it.

Null case_number rows: pass through as-is with is_crossfy_duplicate = False.

Output: artifacts/tables/fact_perm_unique_case/part-0.parquet (single file).
Log:    artifacts/metrics/fact_perm_unique_case.log

Global controls:
  CHUNK_SIZE         = 250_000 (env CHUNK_SIZE)
  ONLY_REWRITE_PARQUET = True  (read existing parquet only)
  exclude: _backup/, _quarantine/, *.tmp_*
  atomic_write: write to .tmp_<name>, then rename
  dry_run: --dry-run flag
"""

from __future__ import annotations

import argparse
import os
import sys
import shutil
import tempfile
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import psutil  # graceful fallback if not installed

# ── Global performance knobs ──────────────────────────────────────────────────
_DEFAULT_CHUNK_SIZE = 250_000
_HIGH_RAM_THRESHOLD_GB = 48


def _get_chunk_size() -> int:
    base = int(os.environ.get("CHUNK_SIZE", _DEFAULT_CHUNK_SIZE))
    try:
        ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        if ram_gb > _HIGH_RAM_THRESHOLD_GB:
            base = int(base * 0.75)
    except Exception:
        pass
    return base


EXCLUDE_PATTERNS: tuple[str, ...] = (
    "_backup", "_quarantine", ".tmp_", "/tmp_",
)

ONLY_REWRITE_PARQUET: bool = True  # never touch raw Excel/PDF


# ── Helpers ───────────────────────────────────────────────────────────────────

def _excluded(p: Path) -> bool:
    s = str(p)
    return any(ex in s for ex in EXCLUDE_PATTERNS)


def _non_null_count(row: pd.Series) -> int:
    return row.notna().sum()


def _load_all_partitions(perm_dir: Path) -> pd.DataFrame:
    """
    Load all fact_perm partitions into a single DataFrame.
    Restores fiscal_year from directory name if not in file.
    """
    files = sorted(
        pf for pf in perm_dir.rglob("*.parquet")
        if not _excluded(pf)
    )
    if not files:
        raise FileNotFoundError(f"No parquet files found in {perm_dir}")

    dfs: list[pd.DataFrame] = []
    for pf in files:
        df = pd.read_parquet(pf)
        # Restore partition columns from directory names if missing
        for part in pf.parts:
            if "=" in part:
                col, val = part.split("=", 1)
                if col not in df.columns:
                    df[col] = val
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)


def _select_best_row(group: pd.DataFrame) -> pd.Series:
    """
    Given a group of rows sharing the same case_number, select the best one.
    Priority: latest decision_date → most non-nulls → smallest source_file.
    """
    g = group.copy()

    # Sort key 1: latest decision_date (NaT goes last)
    if "decision_date" in g.columns:
        g["_sort_date"] = pd.to_datetime(g["decision_date"], errors="coerce")
        g = g.sort_values("_sort_date", ascending=False, na_position="last")

    # Sort key 2: most non-null columns (after dropping helper col)
    non_sort_cols = [c for c in g.columns if not c.startswith("_")]
    g["_non_null_cnt"] = g[non_sort_cols].apply(_non_null_count, axis=1)
    g = g.sort_values(
        ["_sort_date", "_non_null_cnt"],
        ascending=[False, False],
        na_position="last",
    )

    # Sort key 3: smallest source_file as final tiebreak
    if "source_file" in g.columns:
        g = g.sort_values(
            ["_sort_date", "_non_null_cnt", "source_file"],
            ascending=[False, False, True],
            na_position="last",
        )

    # Keep first row, drop helper columns
    best = g.iloc[0].drop(labels=["_sort_date", "_non_null_cnt"], errors="ignore")
    return best


def build_unique_case(
    perm_dir: Path,
    out_dir: Path,
    log_path: Path,
    dry_run: bool = False,
    chunk_size: int = _DEFAULT_CHUNK_SIZE,
) -> None:
    t0 = time.perf_counter()
    log_lines: list[str] = [
        f"fact_perm_unique_case build — {datetime.now().isoformat()}",
        f"dry_run={dry_run}  chunk_size={chunk_size}",
        "",
    ]

    print(f"Loading fact_perm partitions from {perm_dir} ...")
    df = _load_all_partitions(perm_dir)
    base_count = len(df)
    log_lines.append(f"base_count = {base_count:,}")
    print(f"  Loaded {base_count:,} rows")

    # ── Null case_number rows pass through unchanged ──────────────────────────
    null_mask = df["case_number"].isna()
    df_null = df[null_mask].copy()
    df_notnull = df[~null_mask].copy()
    null_count = len(df_null)
    log_lines.append(f"null_case_number_rows = {null_count:,}  (passed through)")
    print(f"  Null case_number rows: {null_count:,}  (pass-through)")

    # ── Detect cross-FY duplicates ────────────────────────────────────────────
    if "fiscal_year" in df_notnull.columns:
        fy_per_case = df_notnull.groupby("case_number")["fiscal_year"].nunique()
        crossfy_cases = set(fy_per_case[fy_per_case > 1].index.tolist())
    else:
        crossfy_cases = set()

    crossfy_dup_rows = len(df_notnull[df_notnull["case_number"].isin(crossfy_cases)])
    log_lines.append(f"cross_fy_cases = {len(crossfy_cases):,}  ({crossfy_dup_rows:,} total rows)")
    print(f"  Cross-FY duplicate case_numbers: {len(crossfy_cases):,}  ({crossfy_dup_rows:,} rows)")

    # Stamp is_crossfy_duplicate onto df_notnull NOW (before split) using
    # string-normalised comparison to survive ArrowBacked vs object dtype mismatches.
    crossfy_str: set[str] = {str(x) for x in crossfy_cases}
    df_notnull["is_crossfy_duplicate"] = (
        df_notnull["case_number"].astype(str).isin(crossfy_str)
    )

    # ── Deduplicate non-null case_numbers ─────────────────────────────────────
    cn_counts = df_notnull["case_number"].value_counts()
    single_cases = cn_counts[cn_counts == 1].index
    multi_cases = cn_counts[cn_counts > 1].index

    df_single = df_notnull[df_notnull["case_number"].isin(single_cases)].copy()
    df_multi = df_notnull[df_notnull["case_number"].isin(multi_cases)].copy()

    print(f"  Single-occurrence cases: {len(df_single):,}  (no dedup needed)")
    print(f"  Multi-occurrence cases: {len(df_multi):,} rows → selecting best ...")

    # Dedup multi-occurrence cases
    if len(df_multi) > 0:
        best_rows = (
            df_multi.groupby("case_number", group_keys=False, sort=False)
            .apply(_select_best_row)
            .reset_index(drop=True)
        )
    else:
        best_rows = df_multi.copy()

    removed_count = len(df_multi) - len(best_rows)
    log_lines.append(f"removed_count (multi-oc dedup) = {removed_count:,}")
    print(f"  Removed {removed_count:,} duplicate rows via best-row selection")

    # ── Assemble final output ─────────────────────────────────────────────────
    df_deduped = pd.concat([df_single, best_rows], ignore_index=True)
    # is_crossfy_duplicate already set on each row above

    # Null pass-through rows
    df_null["is_crossfy_duplicate"] = False

    df_final = pd.concat([df_deduped, df_null], ignore_index=True)
    unique_case_count = len(df_final)
    total_removed = base_count - unique_case_count
    log_lines += [
        f"unique_case_count = {unique_case_count:,}",
        f"total_removed = {total_removed:,}",
        f"  (= {removed_count:,} dedup + {null_count:,} kept null rows; "
        f"net rows removed = {total_removed:,})",
        "",
        f"is_crossfy_duplicate=True count: {df_final['is_crossfy_duplicate'].sum():,}",
        f"columns: {list(df_final.columns)}",
        f"elapsed_sec: {time.perf_counter() - t0:.1f}",
    ]

    print(f"\nFinal: {unique_case_count:,} rows  (removed {total_removed:,})")
    print(f"  is_crossfy_duplicate=True: {df_final['is_crossfy_duplicate'].sum():,}")

    if dry_run:
        print("\nDRY RUN — output NOT written")
        log_lines.append("DRY RUN — no output written")
        _write_log(log_path, log_lines)
        return

    # ── Atomic write ──────────────────────────────────────────────────────────
    out_dir.parent.mkdir(parents=True, exist_ok=True)
    tmp_dir = out_dir.parent / f".tmp_{out_dir.name}"
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)
    tmp_file = tmp_dir / "part-0.parquet"

    print(f"\nWriting → {out_dir}/part-0.parquet ...")
    df_final.to_parquet(tmp_file, index=False)

    # Atomic rename: remove existing then rename tmp
    if out_dir.exists():
        shutil.rmtree(out_dir)
    tmp_dir.rename(out_dir)
    final_file = out_dir / "part-0.parquet"
    print(f"  Written ({final_file.stat().st_size / 1024 / 1024:.1f} MB) [atomic]")
    log_lines.append(f"output: {out_dir}/part-0.parquet")

    _write_log(log_path, log_lines)
    print(f"\nLog → {log_path}")


def _write_log(log_path: Path, lines: list[str]) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text("\n".join(lines) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build fact_perm_unique_case derived table"
    )
    parser.add_argument(
        "--perm-dir", default="artifacts/tables/fact_perm",
        help="Path to fact_perm partitioned directory",
    )
    parser.add_argument(
        "--out-dir", default="artifacts/tables/fact_perm_unique_case",
        help="Output directory",
    )
    parser.add_argument(
        "--log", default="artifacts/metrics/fact_perm_unique_case.log",
        help="Log file path",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=0,
                        help="Override chunk_size (0 = auto)")
    args = parser.parse_args()

    chunk_size = args.chunk_size if args.chunk_size > 0 else _get_chunk_size()

    print("=" * 60)
    print("BUILD FACT_PERM_UNIQUE_CASE")
    print("=" * 60)
    print(f"perm_dir : {args.perm_dir}")
    print(f"out_dir  : {args.out_dir}")
    print(f"chunk_size : {chunk_size:,}")
    print(f"dry_run  : {args.dry_run}")
    print()

    build_unique_case(
        perm_dir=Path(args.perm_dir),
        out_dir=Path(args.out_dir),
        log_path=Path(args.log),
        dry_run=args.dry_run,
        chunk_size=chunk_size,
    )


if __name__ == "__main__":
    main()
