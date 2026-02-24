#!/usr/bin/env python3
"""
Build Manifest Generator — single source of truth for all curated artifacts.

Reads canonical artifact roots ONLY (no data_root / downloads scanning).
Excludes: _backup/, _quarantine/, and *.tmp_* paths.

Outputs: artifacts/metrics/build_manifest.json

Schema per table entry:
  type          : "dim" | "fact"
  files         : list of absolute file paths
  file_counts   : {rel_path: row_count}   (from parquet footer metadata)
  row_count     : int  (sum)
  columns       : list[str]
  partition_keys: list[str]          (derived from directory names)
  partition_values: {key: [val,...]}
  partition_row_counts: {key=val: row_count}
  source_files  : list[str]          (unique source_file values, facts only)
  pk_columns    : list[str]          (from schemas.yml)
  pk_unique     : bool | null        (checked using column projection)
  pk_null_rows  : int                (rows with any PK column null)
"""

from __future__ import annotations

import argparse
import json
import sys
import os
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq
import pandas as pd
import yaml

# ── Performance knobs ─────────────────────────────────────────────────────────
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 250_000))
DRY_RUN = False  # overridden by CLI

EXCLUDE_PATTERNS: tuple[str, ...] = (
    "_backup", "_quarantine", ".tmp_", "/tmp_",
)

DIM_TABLES = [
    "dim_country", "dim_soc", "dim_area",
    "dim_employer", "dim_visa_class",
]

FACT_TABLES = [
    "fact_cutoffs", "fact_perm", "fact_oews", "fact_lca",
]

# Explicit canonical partition key order for compound-partitioned tables.
# Auto-detection from directory names is correct but may vary in dict ordering;
# this enforces a stable, documented ordering in the manifest.
FACT_PARTITION_KEYS: dict[str, list[str]] = {
    "fact_cutoffs": ["bulletin_year", "bulletin_month"],
    "fact_perm":    ["fiscal_year"],
    "fact_lca":     ["fiscal_year"],
    "fact_oews":    ["ref_year"],
}

# Derived tables: read-only consumer views built by separate scripts.
# They are not in schemas.yml; audited non-blocking.
DERIVED_TABLES = [
    "fact_perm_unique_case",
]


def _excluded(path: Path) -> bool:
    s = str(path)
    return any(ex in s for ex in EXCLUDE_PATTERNS)


def _parquet_row_count(pf: Path) -> int:
    """Read row count from parquet footer (no data deserialization)."""
    try:
        meta = pq.read_metadata(pf)
        return meta.num_rows
    except Exception:
        return 0


def _parquet_columns(pf: Path) -> list[str]:
    """Read column names from parquet schema (no data deserialization)."""
    try:
        schema = pq.read_schema(pf)
        return schema.names
    except Exception:
        return []


def _partition_kv(path: Path) -> dict[str, str]:
    """Parse key=value pairs out of all directory components above the file."""
    kv = {}
    for part in path.parts:
        if "=" in part:
            k, v = part.split("=", 1)
            kv[k] = v
    return kv


def build_dim_entry(
    table_name: str,
    artifacts_root: Path,
    pk_columns: list[str],
) -> dict:
    pf = artifacts_root / "tables" / f"{table_name}.parquet"
    entry: dict = {
        "type": "dim",
        "files": [],
        "file_counts": {},
        "row_count": 0,
        "columns": [],
        "partition_keys": [],
        "partition_values": {},
        "partition_row_counts": {},
        "source_files": [],
        "pk_columns": pk_columns,
        "pk_unique": None,
        "pk_null_rows": 0,
        "error": None,
    }

    if not pf.exists():
        entry["error"] = "file not found"
        return entry

    abs_str = str(pf.resolve())
    rc = _parquet_row_count(pf)
    cols = _parquet_columns(pf)

    entry["files"] = [abs_str]
    entry["file_counts"] = {abs_str: rc}
    entry["row_count"] = rc
    entry["columns"] = cols

    # PK uniqueness check — dims are small, safe to read
    if pk_columns:
        try:
            pk_df = pd.read_parquet(pf, columns=pk_columns)
            null_mask = pk_df.isnull().any(axis=1)
            entry["pk_null_rows"] = int(null_mask.sum())
            clean = pk_df[~null_mask]
            entry["pk_unique"] = len(clean.drop_duplicates()) == len(clean)
        except Exception as e:
            entry["error"] = f"pk_check failed: {e}"

    return entry


def build_fact_entry(
    table_name: str,
    artifacts_root: Path,
    pk_columns: list[str],
) -> dict:
    table_dir = artifacts_root / "tables" / table_name
    entry: dict = {
        "type": "fact",
        "files": [],
        "file_counts": {},
        "row_count": 0,
        "columns": [],
        "partition_keys": [],
        "partition_values": {},
        "partition_row_counts": {},
        "partitions": [],
        "source_files": [],
        "pk_columns": pk_columns,
        "pk_unique": None,
        "pk_null_rows": 0,
        "error": None,
    }

    if not table_dir.exists():
        entry["error"] = "directory not found"
        return entry

    parquet_files = sorted(
        pf for pf in table_dir.rglob("*.parquet")
        if not _excluded(pf)
    )

    if not parquet_files:
        entry["error"] = "no parquet files found"
        return entry

    all_columns: list[str] | None = None
    total_rc = 0
    file_counts: dict[str, int] = {}
    partition_row_counts: dict[str, int] = {}
    partition_kv_accum: dict[str, set] = {}

    for pf in parquet_files:
        abs_str = str(pf.resolve())
        rc = _parquet_row_count(pf)
        total_rc += rc
        file_counts[abs_str] = rc

        if all_columns is None:
            all_columns = _parquet_columns(pf)

        # Partition key/value from directory names
        kv = _partition_kv(pf)
        for k, v in kv.items():
            partition_kv_accum.setdefault(k, set()).add(v)
        # Use compound key for multi-level partitions so counts aren't conflated
        # e.g. "bulletin_year=2015/bulletin_month=10" not two flat keys.
        # Order: preserve directory-path order (outer key first) by sorting by
        # the position of each key component in the path string.
        if kv:
            path_str = str(pf)
            def _key_pos(item: tuple) -> int:
                return path_str.find(f"{item[0]}=")
            compound_label = "/".join(f"{k}={v}" for k, v in sorted(kv.items(), key=_key_pos))
            partition_row_counts[compound_label] = partition_row_counts.get(compound_label, 0) + rc

    entry["files"] = [str(pf.resolve()) for pf in parquet_files]
    entry["file_counts"] = file_counts
    entry["row_count"] = total_rc
    # Include partition-key columns in the schema even though they are not stored
    # inside the parquet files (they're encoded in the directory names).
    file_columns = all_columns or []
    partition_only_cols = [k for k in partition_kv_accum if k not in file_columns]
    entry["columns"] = file_columns + partition_only_cols
    entry["partition_keys"] = list(partition_kv_accum.keys())
    entry["partition_values"] = {
        k: sorted(v) for k, v in partition_kv_accum.items()
    }
    entry["partition_row_counts"] = partition_row_counts

    # Per-leaf-partition list: [{key: val, ..., row_count: N, files: [abs_path]}]
    partition_entries: list[dict] = []
    for pf in parquet_files:
        kv = _partition_kv(pf)
        abs_str = str(pf.resolve())
        pentry: dict = {k: v for k, v in kv.items()}
        pentry["row_count"] = file_counts[abs_str]
        pentry["files"] = [abs_str]
        partition_entries.append(pentry)
    entry["partitions"] = partition_entries

    # source_file extraction (column projection — fast)
    if "source_file" in (all_columns or []):
        source_files_set: set[str] = set()
        for pf in parquet_files:
            try:
                sf_df = pd.read_parquet(pf, columns=["source_file"])
                source_files_set.update(sf_df["source_file"].dropna().astype(str))
            except Exception:
                pass
        entry["source_files"] = sorted(source_files_set)

    # PK uniqueness check (column projection on PK columns only)
    if pk_columns:
        try:
            pk_dfs = []
            for pf in parquet_files:
                avail = [c for c in pk_columns if c in (all_columns or [])]
                missing_from_file = [c for c in pk_columns if c not in (all_columns or [])]
                if avail:
                    df_pk = pd.read_parquet(pf, columns=avail)
                    # Add partition-derived columns if missing from file
                    kv = _partition_kv(pf)
                    for mc in missing_from_file:
                        if mc in kv:
                            df_pk[mc] = kv[mc]
                    pk_dfs.append(df_pk)

            if pk_dfs:
                df_all_pk = pd.concat(pk_dfs, ignore_index=True)
                null_mask = df_all_pk.isnull().any(axis=1)
                entry["pk_null_rows"] = int(null_mask.sum())
                clean = df_all_pk[~null_mask]
                entry["pk_unique"] = len(clean.drop_duplicates()) == len(clean)
        except Exception as e:
            entry["pk_unique"] = None
            entry["error"] = f"pk_check failed: {e}"

    return entry


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate build manifest JSON")
    parser.add_argument(
        "--artifacts", default="artifacts",
        help="Artifacts root directory (default: artifacts)",
    )
    parser.add_argument(
        "--schemas", default="configs/schemas.yml",
        help="Path to schemas.yml (default: configs/schemas.yml)",
    )
    parser.add_argument(
        "--out", default="artifacts/metrics/build_manifest.json",
        help="Output manifest path",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    artifacts_root = Path(args.artifacts)
    out_path = Path(args.out)

    # Load schemas for PK info
    with open(args.schemas) as f:
        schemas: dict = yaml.safe_load(f)

    print("=" * 60)
    print("BUILD MANIFEST GENERATOR")
    print("=" * 60)
    print(f"Artifacts root : {artifacts_root.resolve()}")
    print(f"Output         : {out_path}")
    print()

    manifest: dict = {
        "build_id": datetime.now(timezone.utc).isoformat(),
        "artifacts_root": str(artifacts_root.resolve()),
        "tables": {},
    }

    # ── Dims ─────────────────────────────────────────────────────────────────
    print("Building dim entries...")
    for tname in DIM_TABLES:
        pk_cols = schemas.get(tname, {}).get("primary_key", [])
        entry = build_dim_entry(tname, artifacts_root, pk_cols)
        manifest["tables"][tname] = entry
        status = "✓" if entry["error"] is None else f"✗ {entry['error']}"
        print(
            f"  {tname}: {entry['row_count']:,} rows  "
            f"pk_unique={entry['pk_unique']}  {status}"
        )

    print()

    # ── Facts ─────────────────────────────────────────────────────────────────
    print("Building fact entries...")
    for tname in FACT_TABLES:
        pk_cols = schemas.get(tname, {}).get("primary_key", [])
        entry = build_fact_entry(tname, artifacts_root, pk_cols)
        # Enforce canonical partition key ordering
        if tname in FACT_PARTITION_KEYS:
            canonical = FACT_PARTITION_KEYS[tname]
            # Keep only keys actually present; preserve canonical order
            present = set(entry.get("partition_keys", []))
            entry["partition_keys"] = [k for k in canonical if k in present]
        manifest["tables"][tname] = entry
        nfiles = len(entry["files"])
        nsf = len(entry["source_files"])
        status = "✓" if entry["error"] is None else f"✗ {entry['error']}"
        print(
            f"  {tname}: {entry['row_count']:,} rows  "
            f"files={nfiles}  source_files={nsf}  "
            f"pk_unique={entry['pk_unique']}  {status}"
        )

    print()

    # ── Derived tables ─────────────────────────────────────────────────
    print("Building derived table entries...")
    for tname in DERIVED_TABLES:
        entry = build_fact_entry(tname, artifacts_root, pk_columns=[])
        entry["type"] = "derived"  # override type
        manifest["tables"][tname] = entry
        nfiles = len(entry["files"])
        status = "✓" if entry["error"] is None else f"(not yet built: {entry['error']})"
        print(
            f"  {tname}: {entry['row_count']:,} rows  files={nfiles}  {status}"
        )

    print()

    if args.dry_run:
        print("DRY RUN — manifest NOT saved")
        return

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(manifest, f, indent=2)
    print(f"Manifest written → {out_path}")


if __name__ == "__main__":
    main()
