#!/usr/bin/env python3
"""
Section A – Presentation & Snapshot Artifacts
============================================
Produces:
  1. fact_cutoffs_all.parquet   – VB presentation (rebuilt if missing/outdated)
  2. fact_cutoffs/_snapshot.json
  3. fact_perm_all.parquet      – optional concat of PERM partitions
  4. fact_perm/_snapshot.json
  5. fact_lca_all.parquet       – optional concat of LCA partitions
  6. fact_lca/_snapshot.json
  7. fact_oews/_snapshot.json
  8. artifacts/metrics/presentations_and_snapshots.json  – summary log

All parquet writes are atomic (write tmp → rename).
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"
METRICS = ROOT / "artifacts" / "metrics"
METRICS.mkdir(parents=True, exist_ok=True)

LOG_LINES: list[str] = []

# ── Logging ──────────────────────────────────────────────────────────────────

def log(msg: str = "") -> None:
    print(msg)
    LOG_LINES.append(msg)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def _atomic_write_parquet(df: pd.DataFrame, dest: Path, **kwargs) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(f".tmp_{os.getpid()}.parquet")
    df.to_parquet(tmp, index=False, engine="pyarrow", **kwargs)
    shutil.move(str(tmp), str(dest))


def _read_partitioned(dir_path: Path, restore_partition_cols: bool = True) -> pd.DataFrame:
    """Concat all parquet files under dir_path, restoring partition key cols."""
    files = sorted(dir_path.rglob("*.parquet"))
    dfs = []
    for pf in files:
        df = pd.read_parquet(pf)
        if restore_partition_cols:
            for part in pf.parts:
                if "=" in part:
                    col, val = part.split("=", 1)
                    if col not in df.columns:
                        df[col] = val
        dfs.append(df)
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _snapshot(
    name: str,
    dir_path: Path,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a snapshot dict for a partitioned directory."""
    files = sorted(dir_path.rglob("*.parquet"))
    partitions: dict[str, int] = {}
    total_rows = 0
    file_list = []
    for pf in files:
        try:
            meta = pq.read_metadata(pf)
            nrows = meta.num_rows
        except Exception:
            nrows = 0
        total_rows += nrows
        rel = str(pf.relative_to(dir_path))
        # parse partition keys from path parts
        part_key = "/".join(p for p in pf.parts if "=" in p)
        partitions[part_key] = nrows
        file_list.append({"file": rel, "rows": nrows, "sha256": _sha256(pf)})
    snap: dict[str, Any] = {
        "name": name,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_rows": total_rows,
        "num_files": len(files),
        "partitions": partitions,
        "files": file_list,
    }
    if extra:
        snap.update(extra)
    return snap


def _write_snapshot(snap: dict[str, Any], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as fh:
        json.dump(snap, fh, indent=2)
    log(f"  snapshot → {out_path.relative_to(ROOT)}")


# ── SECTION 1: Visa Bulletin (fact_cutoffs) ──────────────────────────────────

def build_vb_presentation() -> dict[str, Any]:
    log("\n" + "=" * 60)
    log("SECTION 1: Visa Bulletin Presentation + Snapshot")
    log("=" * 60)

    all_path = TABLES / "fact_cutoffs_all.parquet"
    cutoffs_dir = TABLES / "fact_cutoffs"

    if not cutoffs_dir.exists():
        log("  ERROR: fact_cutoffs/ not found – cannot build presentation")
        return {"error": "fact_cutoffs directory missing"}

    leaves = sorted(cutoffs_dir.rglob("*.parquet"))
    log(f"  Leaves found: {len(leaves)}")

    # Decide whether to rebuild
    rebuild = True
    if all_path.exists():
        try:
            existing_meta = pq.read_metadata(all_path)
            existing_rows = existing_meta.num_rows
            total_leaf_rows = sum(pq.read_metadata(lf).num_rows for lf in leaves)
            if existing_rows == total_leaf_rows and existing_rows > 0:
                rebuild = False
                log(f"  fact_cutoffs_all.parquet up-to-date ({existing_rows:,} rows) – skipping rebuild")
        except Exception:
            pass

    if rebuild:
        log("  Building fact_cutoffs_all.parquet …")
        df = _read_partitioned(cutoffs_dir)
        log(f"  Raw union: {len(df):,} rows")

        # Apply VB dedupe key: PK=(bulletin_year, bulletin_month, chart, category, country)
        # Preference: D > C > U → non-null cutoff_date → lex smallest source_file
        pk = ["bulletin_year", "bulletin_month", "chart", "category", "country"]
        chart_order = {"D": 0, "C": 1, "U": 2}
        if "chart" in df.columns:
            df["_chart_ord"] = df["chart"].map(chart_order).fillna(9)
        else:
            df["_chart_ord"] = 9
        df["_cutoff_null"] = df["cutoff_date"].isna().astype(int) if "cutoff_date" in df.columns else 1
        df["_src"] = df["source_file"] if "source_file" in df.columns else ""
        df = df.sort_values(by=["_chart_ord", "_cutoff_null", "_src"])
        df = df.drop_duplicates(subset=pk, keep="first")
        df = df.drop(columns=["_chart_ord", "_cutoff_null", "_src"], errors="ignore")
        log(f"  After PK dedupe: {len(df):,} rows")

        _atomic_write_parquet(df, all_path)
        log(f"  Written: {all_path.relative_to(ROOT)} ({len(df):,} rows)")
        rows = len(df)
    else:
        rows = existing_rows  # type: ignore[possibly-undefined]

    # Snapshot
    snap = _snapshot("fact_cutoffs", cutoffs_dir)
    snap["presentation_rows"] = rows
    # Year span from partition directory names
    bulletin_years = sorted({
        int(p.split("=")[1])
        for pf in leaves
        for p in pf.parts
        if p.startswith("bulletin_year=") and p.split("=")[1].isdigit()
    })
    if bulletin_years:
        snap["years_span"] = f"{bulletin_years[0]}–{bulletin_years[-1]}"
        snap["distinct_years"] = len(bulletin_years)
    _write_snapshot(snap, cutoffs_dir / "_snapshot.json")

    log(f"  PASS: fact_cutoffs presentation {rows:,} rows")
    return {"rows": rows, "leaves": len(leaves), "years": snap.get("distinct_years", 0)}


# ── SECTION 2: PERM ──────────────────────────────────────────────────────────

def build_perm_snapshot(build_all_parquet: bool = True) -> dict[str, Any]:
    log("\n" + "=" * 60)
    log("SECTION 2: PERM Presentation + Snapshot")
    log("=" * 60)

    perm_dir = TABLES / "fact_perm"
    if not perm_dir.exists():
        log("  ERROR: fact_perm/ not found")
        return {"error": "fact_perm directory missing"}

    files = sorted(perm_dir.rglob("*.parquet"))
    log(f"  Partition files: {len(files)}")

    snap = _snapshot("fact_perm", perm_dir)
    log(f"  Total rows: {snap['total_rows']:,}")

    # Fiscal years span
    fiscal_years = sorted({
        int(p.split("=")[1])
        for pf in files
        for p in pf.parts
        if p.startswith("fiscal_year=")
    })
    if fiscal_years:
        snap["fiscal_years"] = f"{fiscal_years[0]}–{fiscal_years[-1]}"
        snap["distinct_fiscal_years"] = len(fiscal_years)

    _write_snapshot(snap, perm_dir / "_snapshot.json")

    if build_all_parquet:
        all_path = TABLES / "fact_perm_all.parquet"
        rebuild = True
        if all_path.exists():
            try:
                meta = pq.read_metadata(all_path)
                if meta.num_rows == snap["total_rows"]:
                    rebuild = False
                    log(f"  fact_perm_all.parquet up-to-date ({meta.num_rows:,} rows) – skipping")
            except Exception:
                pass
        if rebuild:
            log("  Building fact_perm_all.parquet (concat of partitions) …")
            df = _read_partitioned(perm_dir)
            _atomic_write_parquet(df, all_path)
            log(f"  Written: {all_path.relative_to(ROOT)} ({len(df):,} rows)")
    else:
        log("  Skipping fact_perm_all.parquet (build_all_parquet=False)")

    return {"total_rows": snap["total_rows"], "files": len(files)}


# ── SECTION 3: LCA ───────────────────────────────────────────────────────────

def build_lca_snapshot(build_all_parquet: bool = False) -> dict[str, Any]:
    """build_all_parquet=False by default (9.5M rows; skip unless explicitly asked)."""
    log("\n" + "=" * 60)
    log("SECTION 3: LCA Presentation + Snapshot")
    log("=" * 60)

    lca_dir = TABLES / "fact_lca"
    if not lca_dir.exists():
        log("  ERROR: fact_lca/ not found")
        return {"error": "fact_lca directory missing"}

    files = sorted(lca_dir.rglob("*.parquet"))
    log(f"  Partition files: {len(files)}")

    snap = _snapshot("fact_lca", lca_dir)
    log(f"  Total rows: {snap['total_rows']:,}")

    fiscal_years = sorted({
        int(p.split("=")[1])
        for pf in files
        for p in pf.parts
        if p.startswith("fiscal_year=")
    })
    if fiscal_years:
        snap["fiscal_years"] = f"{fiscal_years[0]}–{fiscal_years[-1]}"
        snap["distinct_fiscal_years"] = len(fiscal_years)

    _write_snapshot(snap, lca_dir / "_snapshot.json")

    if build_all_parquet:
        all_path = TABLES / "fact_lca_all.parquet"
        log("  Building fact_lca_all.parquet …")
        df = _read_partitioned(lca_dir)
        _atomic_write_parquet(df, all_path)
        log(f"  Written: {all_path.relative_to(ROOT)} ({len(df):,} rows)")
    else:
        log("  Skipping fact_lca_all.parquet (large; pass --lca-all to generate)")

    return {"total_rows": snap["total_rows"], "files": len(files)}


# ── SECTION 4: OEWS ──────────────────────────────────────────────────────────

def build_oews_snapshot() -> dict[str, Any]:
    log("\n" + "=" * 60)
    log("SECTION 4: OEWS Snapshot")
    log("=" * 60)

    oews_dir = TABLES / "fact_oews"
    oews_single = TABLES / "fact_oews.parquet"

    if oews_dir.exists() and oews_dir.is_dir():
        files = sorted(oews_dir.rglob("*.parquet"))
        log(f"  Partition files: {len(files)}")
        snap = _snapshot("fact_oews", oews_dir)

        # ref_year breakdown
        ref_years: dict[str, int] = {}
        for pf in files:
            for p in pf.parts:
                if p.startswith("ref_year="):
                    ry = p.split("=")[1]
                    ref_years[ry] = ref_years.get(ry, 0) + (pq.read_metadata(pf).num_rows if pf.exists() else 0)
        if ref_years:
            snap["ref_years"] = ref_years

        _write_snapshot(snap, oews_dir / "_snapshot.json")
        log(f"  Total rows: {snap['total_rows']:,}")
        return {"total_rows": snap["total_rows"]}
    elif oews_single.exists():
        meta = pq.read_metadata(oews_single)
        snap = {
            "name": "fact_oews",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_rows": meta.num_rows,
            "num_files": 1,
            "sha256": _sha256(oews_single),
        }
        snap_path = TABLES / "fact_oews_snapshot.json"
        with open(snap_path, "w") as fh:
            json.dump(snap, fh, indent=2)
        log(f"  Single file: {oews_single.name} ({meta.num_rows:,} rows)")
        log(f"  snapshot → {snap_path.relative_to(ROOT)}")
        return {"total_rows": meta.num_rows}
    else:
        log("  ERROR: fact_oews not found (dir or single file)")
        return {"error": "fact_oews not found"}


# ── MAIN ─────────────────────────────────────────────────────────────────────

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Build presentation & snapshot artifacts")
    parser.add_argument("--lca-all", action="store_true", help="Also build fact_lca_all.parquet")
    parser.add_argument("--no-perm-all", action="store_true", help="Skip fact_perm_all.parquet")
    args = parser.parse_args()

    t0 = time.time()
    log("=" * 60)
    log("PRESENTATION & SNAPSHOT ARTIFACTS")
    log(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log("=" * 60)

    summary: dict[str, Any] = {}

    summary["fact_cutoffs"] = build_vb_presentation()
    summary["fact_perm"] = build_perm_snapshot(build_all_parquet=not args.no_perm_all)
    summary["fact_lca"] = build_lca_snapshot(build_all_parquet=args.lca_all)
    summary["fact_oews"] = build_oews_snapshot()

    elapsed = time.time() - t0
    summary["elapsed_seconds"] = round(elapsed, 1)
    summary["generated_at"] = datetime.now(timezone.utc).isoformat()

    # Write summary JSON
    out_json = METRICS / "presentations_and_snapshots.json"
    with open(out_json, "w") as fh:
        json.dump(summary, fh, indent=2)

    log("\n" + "=" * 60)
    log("DONE")
    log(f"Elapsed: {elapsed:.1f}s")
    log(f"Summary: {out_json.relative_to(ROOT)}")
    log("=" * 60)

    # Write log
    log_path = METRICS / "presentation_and_snapshot.log"
    with open(log_path, "w") as fh:
        fh.write("\n".join(LOG_LINES))

    # Check for errors
    errors = [k for k, v in summary.items() if isinstance(v, dict) and "error" in v]
    if errors:
        print(f"\nERROR: Snapshot failures for: {errors}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
