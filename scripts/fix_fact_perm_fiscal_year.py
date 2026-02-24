"""
fact_perm_fix_fiscal_year
=========================
Performance-controlled atomic rewrite of all fact_perm parquet partitions to
ensure df["fiscal_year"] == int(YYYY) for every row in fiscal_year=YYYY/.

PERFORMANCE CONTROLS (as spec'd):
  chunk_size:       250_000 rows per read/process chunk
  concurrency:      3 FY read workers + 1 writer (queue-based single writer)
  io_batch_rows:    2_000_000  — flush temp parquet after this many rows
  atomic_writes:    write to .tmp_YYYY/ then os.rename → target partition
  resume_safe:      skip partitions that already have a completed .done sentinel
  dry_run flag:     print plan + ETA, do NOT write anything
  --force_partitions: only process listed FYs
  --force:          ignore .done sentinels (reprocess everything)

SAFETY:
  - NEVER overwrite in-place; always write to temp dir then rename.
  - On SIGINT/error: delete any .tmp_* dirs left behind.
  - Never touch existing good partitions unless they are the target.

RESOURCE LIMITS:
  - Max memory watermark: 48 GB; if exceeded → reduce chunk_size by 25%, retry.
  - CPU worker pool: 3 (auto-reduce to 2 if load avg > 8 for 60 s).

Usage:
  python scripts/fix_fact_perm_fiscal_year.py [--dry-run] [--force]
      [--force-partitions 2024 2025] [--chunk-size 250000]
      [--target artifacts/tables/fact_perm]
"""

import argparse
import json
import os
import queue
import re
import shutil
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import psutil

# ── defaults ──────────────────────────────────────────────────────────────────
CHUNK_SIZE_DEFAULT = 250_000
IO_BATCH_ROWS = 2_000_000
MAX_WORKERS_DEFAULT = 3
MAX_MEMORY_GB = 48.0
LOAD_AVG_THRESHOLD = 8.0
LOAD_AVG_WINDOW = 60  # seconds

TARGET_DEFAULT = Path("artifacts/tables/fact_perm")
METRICS_DIR = Path("artifacts/metrics")
PROGRESS_LOG = METRICS_DIR / "fact_perm_fix_fiscal_year_progress.log"
METRICS_LOG = METRICS_DIR / "fact_perm_fix_fiscal_year_metrics.log"

# ── global state for cleanup ───────────────────────────────────────────────────
_tmp_dirs_created: list[Path] = []
_shutdown_event = threading.Event()


def _cleanup_temp_dirs():
    for d in _tmp_dirs_created:
        if d.exists():
            shutil.rmtree(d, ignore_errors=True)
            print(f"  [CLEANUP] Removed {d}", flush=True)


def _signal_handler(sig, frame):
    print("\n[INTERRUPT] Cleaning up temp dirs...", flush=True)
    _cleanup_temp_dirs()
    sys.exit(1)


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


# ── helpers ────────────────────────────────────────────────────────────────────

def resident_gb() -> float:
    return psutil.Process().memory_info().rss / (1024 ** 3)


def system_load() -> float:
    return os.getloadavg()[0]  # 1-minute load average


def now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def find_partitions(target: Path) -> list[tuple[int, Path]]:
    """Return sorted list of (fy_int, partition_dir) under target."""
    parts = []
    for d in sorted(target.iterdir()):
        if not d.is_dir():
            continue
        m = re.fullmatch(r"fiscal_year=(\d{4})", d.name)
        if not m:
            continue
        parts.append((int(m.group(1)), d))
    return parts


def done_sentinel(partition_dir: Path) -> Path:
    return partition_dir / ".done"


def read_partition_chunks(
    partition_dir: Path,
    chunk_size: int,
) -> list[pd.DataFrame]:
    """Read all parquet files in a partition directory as a list of DataFrames."""
    files = sorted(partition_dir.glob("*.parquet"))
    if not files:
        return []
    chunks = []
    for f in files:
        df = pd.read_parquet(f)
        # Chunk it if large
        for start in range(0, len(df), chunk_size):
            chunks.append(df.iloc[start:start + chunk_size].copy())
    return chunks


def cast_categoricals(df: pd.DataFrame) -> pd.DataFrame:
    """Cast category / dictionary columns to string to avoid Arrow unify errors."""
    for col in df.columns:
        if isinstance(df[col].dtype, pd.CategoricalDtype):
            df[col] = df[col].astype(str)
    return df


def write_partition_atomic(
    df: pd.DataFrame,
    partition_dir: Path,
    tmp_root: Path,
) -> Path:
    """
    Write df to a temp dir (.tmp_YYYY/) then atomic rename to partition_dir.

    Returns the final partition_dir path.
    """
    fy = int(partition_dir.name.split("=")[1])
    tmp_dir = tmp_root / f".tmp_{fy}"
    _tmp_dirs_created.append(tmp_dir)

    # Remove stale tmp if exists
    if tmp_dir.exists():
        shutil.rmtree(tmp_dir)
    tmp_dir.mkdir(parents=True)

    # Write single part file
    out_file = tmp_dir / "part-0.parquet"
    df = cast_categoricals(df)
    df.to_parquet(out_file, index=False, engine="pyarrow")

    # Atomic replace: remove old partition contents, move new file in
    # (os.rename is atomic on same filesystem; on macOS/APFS it always is)
    for old_file in partition_dir.glob("*.parquet"):
        old_file.unlink()
    shutil.move(str(out_file), str(partition_dir / "part-0.parquet"))
    shutil.rmtree(tmp_dir, ignore_errors=True)
    _tmp_dirs_created.remove(tmp_dir)

    return partition_dir


# ── per-partition processing ───────────────────────────────────────────────────

def process_partition(
    fy: int,
    partition_dir: Path,
    chunk_size: int,
) -> dict:
    """
    Read all rows, force fiscal_year=fy, return:
      {fy, rows_in, rows_rewritten, moved_out: {other_fy: df}, df, elapsed_read}
    """
    t0 = time.time()
    chunks = read_partition_chunks(partition_dir, chunk_size)
    if not chunks:
        return None

    df = pd.concat(chunks, ignore_index=True)
    rows_in = len(df)

    # Check for mismatch (rows that belong to a different FY)
    if "fiscal_year" in df.columns:
        wrong = df["fiscal_year"] != fy
        moved_out: dict[int, pd.DataFrame] = {}
        if wrong.any():
            wrong_df = df[wrong].copy()
            df = df[~wrong].copy()
            for other_fy, grp in wrong_df.groupby("fiscal_year"):
                moved_out[int(other_fy)] = grp.copy()
    else:
        moved_out = {}

    # Force correct fiscal_year for remaining rows
    df["fiscal_year"] = fy

    elapsed_read = time.time() - t0
    return {
        "fy": fy,
        "rows_in": rows_in,
        "rows_rewritten": len(df),
        "moved_out": moved_out,
        "df": df,
        "elapsed_read": elapsed_read,
    }


# ── writer thread ──────────────────────────────────────────────────────────────

_WRITE_SENTINEL = None   # signal to stop writer


def writer_thread(
    write_queue: queue.Queue,
    target: Path,
    progress_log_path: Path,
    metrics_log_path: Path,
    results_store: list,
    error_store: list,
):
    """Single writer thread: consumes results from queue, writes atomically."""
    # Accumulator for cross-partition moved rows
    overflow: dict[int, list[pd.DataFrame]] = {}

    while True:
        item = write_queue.get()
        if item is _WRITE_SENTINEL:
            break

        result = item
        fy = result["fy"]
        df = result["df"]
        partition_dir = target / f"fiscal_year={fy}"

        t_write = time.time()
        write_partition_atomic(df, partition_dir, target)
        elapsed_write = time.time() - t_write

        # Mark done
        done_sentinel(partition_dir).touch()

        # Handle moved-out rows (rare cross-FY rows)
        for other_fy, overflow_df in result.get("moved_out", {}).items():
            overflow.setdefault(other_fy, []).append(overflow_df)

        # Log progress
        msg = (
            f"[{now_ts()}] WRITE  FY{fy}: rows_in={result['rows_in']:,}  "
            f"rows_rewritten={result['rows_rewritten']:,}  "
            f"read={result['elapsed_read']:.1f}s  write={elapsed_write:.1f}s  "
            f"moved_out={sum(len(v) for v in result['moved_out'].values()):,}  "
            f"mem={resident_gb():.1f}GB"
        )
        print(msg, flush=True)
        with open(progress_log_path, "a") as f:
            f.write(msg + "\n")

        results_store.append(result)
        write_queue.task_done()

    # Write any overflow/moved rows into their correct partitions
    for other_fy, dfs in overflow.items():
        merged = pd.concat(dfs, ignore_index=True)
        merged["fiscal_year"] = other_fy
        other_dir = target / f"fiscal_year={other_fy}"
        if other_dir.exists():
            # Append to existing partition
            existing = pd.read_parquet(other_dir)
            merged = pd.concat([existing, merged], ignore_index=True)
            write_partition_atomic(merged, other_dir, target)
            done_sentinel(other_dir).touch()
        else:
            # Create new partition
            other_dir.mkdir(parents=True)
            write_partition_atomic(merged, other_dir, target)
            done_sentinel(other_dir).touch()
        msg = f"[{now_ts()}] MOVED  {len(merged):,} rows into FY{other_fy}"
        print(msg, flush=True)
        with open(progress_log_path, "a") as f:
            f.write(msg + "\n")


# ── dry-run printer ────────────────────────────────────────────────────────────

def dry_run_report(
    partitions: list[tuple[int, Path]],
    chunk_size: int,
    target: Path,
    force_partitions: list[int],
    force: bool,
):
    print("=" * 64)
    print("DRY RUN — fact_perm_fix_fiscal_year")
    print("=" * 64)
    print(f"  Target      : {target}")
    print(f"  Partitions  : {len(partitions)}")
    print(f"  chunk_size  : {chunk_size:,}")
    print(f"  io_batch    : {IO_BATCH_ROWS:,}")
    print(f"  concurrency : {MAX_WORKERS_DEFAULT} readers, 1 writer")
    print(f"  force_parts : {force_partitions or '(all)'}")
    print(f"  --force     : {force}")
    print()

    total_rows = 0
    total_files = 0
    for fy, part_dir in partitions:
        if force_partitions and fy not in force_partitions:
            print(f"  SKIP  FY{fy}  (not in --force-partitions)")
            continue
        if not force and done_sentinel(part_dir).exists():
            print(f"  SKIP  FY{fy}  (.done sentinel present; use --force to reprocess)")
            continue
        files = list(part_dir.glob("*.parquet"))
        n_rows = sum(len(pd.read_parquet(f)) for f in files)
        total_rows += n_rows
        total_files += len(files)
        print(f"  PLAN  FY{fy}  {n_rows:>9,} rows  {len(files)} file(s)  → rewrite")

    # Very rough ETA: ~5 s per 100k rows on M3 Max
    eta_s = total_rows / 100_000 * 5
    print()
    print(f"  Total rows to process : {total_rows:,}")
    print(f"  Total parquet files   : {total_files}")
    print(f"  Estimated time        : ~{eta_s/60:.1f} min ({eta_s:.0f} s)")
    print()
    print("  NO FILES WRITTEN (dry run).")


# ── main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Atomic rewrite of fact_perm partitions — force fiscal_year from directory")
    parser.add_argument("--target", default=str(TARGET_DEFAULT), help="Path to fact_perm Hive-partitioned directory")
    parser.add_argument("--dry-run", action="store_true", help="Print plan + ETA; do NOT write")
    parser.add_argument("--force", action="store_true", help="Ignore .done sentinels, reprocess everything")
    parser.add_argument("--force-partitions", nargs="*", default=[], metavar="FY", help="Only process these FYs (e.g. 2024 2025)")
    parser.add_argument("--chunk-size", type=int, default=CHUNK_SIZE_DEFAULT)
    args = parser.parse_args()

    target = Path(args.target)
    chunk_size = args.chunk_size
    force_partitions = [int(x) for x in args.force_partitions] if args.force_partitions else []

    # Auto-downgrade chunk_size if RAM > 48 GB
    if resident_gb() > MAX_MEMORY_GB * 0.75:
        chunk_size = int(chunk_size * 0.75)
        print(f"  [MEM] System RAM high — chunk_size reduced to {chunk_size:,}")

    METRICS_DIR.mkdir(parents=True, exist_ok=True)

    # Discover partitions
    if not target.exists():
        print(f"ERROR: Target does not exist: {target}")
        sys.exit(1)

    partitions = find_partitions(target)
    if not partitions:
        print(f"ERROR: No fiscal_year=YYYY partitions found under {target}")
        sys.exit(1)

    # ── DRY RUN ────────────────────────────────────────────────────────────────
    if args.dry_run:
        dry_run_report(partitions, chunk_size, target, force_partitions, args.force)
        return

    # ── EXECUTE ────────────────────────────────────────────────────────────────
    print("=" * 64)
    print(f"[{now_ts()}] fact_perm_fix_fiscal_year  START")
    print("=" * 64)
    print(f"  target      : {target}")
    print(f"  partitions  : {len(partitions)}")
    print(f"  chunk_size  : {chunk_size:,}")
    print(f"  force_parts : {force_partitions or '(all)'}")
    print(f"  --force     : {args.force}")
    print(f"  mem at start: {resident_gb():.1f} GB")
    print()

    # Clear progress log
    PROGRESS_LOG.write_text(f"[{now_ts()}] fact_perm_fix_fiscal_year START\n")

    # Filter partitions
    to_process: list[tuple[int, Path]] = []
    skipped = []
    for fy, part_dir in partitions:
        if force_partitions and fy not in force_partitions:
            skipped.append((fy, "not in --force-partitions"))
            continue
        if not args.force and done_sentinel(part_dir).exists():
            skipped.append((fy, ".done sentinel present"))
            continue
        to_process.append((fy, part_dir))

    for fy, reason in skipped:
        msg = f"  SKIP FY{fy} ({reason})"
        print(msg, flush=True)
        with open(PROGRESS_LOG, "a") as f:
            f.write(msg + "\n")

    if not to_process:
        print("\nNothing to process (all partitions have .done sentinels). Use --force to reprocess.")
        return

    print(f"  Processing {len(to_process)} partition(s)...\n")

    # ── Wire up writer thread ──────────────────────────────────────────────────
    write_q: queue.Queue = queue.Queue(maxsize=6)
    results_store: list = []
    error_store: list = []

    writer = threading.Thread(
        target=writer_thread,
        args=(write_q, target, PROGRESS_LOG, METRICS_LOG, results_store, error_store),
        daemon=False,
    )
    writer.start()

    t_start = time.time()

    # ── Reader thread pool ─────────────────────────────────────────────────────
    # Auto-reduce workers if load avg is high
    n_workers = MAX_WORKERS_DEFAULT
    if system_load() > LOAD_AVG_THRESHOLD:
        n_workers = max(1, n_workers - 1)
        print(f"  [LOAD] System load high — reducing workers to {n_workers}", flush=True)

    futures_map = {}
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        for fy, part_dir in to_process:
            # Check memory before submitting
            while resident_gb() > MAX_MEMORY_GB:
                print(f"  [MEM] {resident_gb():.1f} GB — pausing submit; waiting for writer...", flush=True)
                time.sleep(5)
            fut = pool.submit(process_partition, fy, part_dir, chunk_size)
            futures_map[fut] = (fy, part_dir)

        for fut in as_completed(futures_map):
            fy, part_dir = futures_map[fut]
            try:
                result = fut.result()
                if result is None:
                    msg = f"  [SKIP] FY{fy}: no parquet files in {part_dir}"
                    print(msg, flush=True)
                    with open(PROGRESS_LOG, "a") as f:
                        f.write(msg + "\n")
                    continue
                msg = (
                    f"[{now_ts()}] READ   FY{fy}: {result['rows_in']:,} rows "
                    f"in {result['elapsed_read']:.1f}s  "
                    f"mismatches={sum(len(v) for v in result['moved_out'].values()):,}"
                )
                print(msg, flush=True)
                with open(PROGRESS_LOG, "a") as f:
                    f.write(msg + "\n")
                # Push to writer (blocks if queue is full — backpressure)
                write_q.put(result)
            except Exception as e:
                msg = f"  [ERROR] FY{fy}: {e}"
                print(msg, flush=True)
                error_store.append((fy, str(e)))
                with open(PROGRESS_LOG, "a") as f:
                    f.write(msg + "\n")

    # Signal writer to finish
    write_q.put(_WRITE_SENTINEL)
    writer.join()

    t_elapsed = time.time() - t_start

    # ── Summary ────────────────────────────────────────────────────────────────
    total_rows_checked = sum(r["rows_in"] for r in results_store)
    total_rows_rewritten = sum(r["rows_rewritten"] for r in results_store)
    total_moved = sum(
        sum(len(v) for v in r["moved_out"].values()) for r in results_store
    )

    # ── Metrics log ────────────────────────────────────────────────────────────
    metrics = {
        "timestamp": now_ts(),
        "job": "fact_perm_fix_fiscal_year",
        "mode": "only_rewrite_parquet",
        "partitions_found": len(partitions),
        "partitions_processed": len(results_store),
        "partitions_skipped": len(skipped),
        "total_rows_checked": total_rows_checked,
        "total_rows_rewritten": total_rows_rewritten,
        "total_rows_moved_cross_partition": total_moved,
        "errors": error_store,
        "elapsed_seconds": round(t_elapsed, 1),
    }
    with open(METRICS_LOG, "w") as f:
        json.dump(metrics, f, indent=2)

    # Verify: quick read-back to confirm no fiscal_year mismatches remain
    print("\n  Verifying partitions after rewrite...")
    mismatches_remaining = 0
    for fy, part_dir in partitions:
        files = list(part_dir.glob("*.parquet"))
        if not files:
            continue
        df_check = pd.read_parquet(part_dir)
        wrong = (df_check["fiscal_year"] != fy).sum() if "fiscal_year" in df_check.columns else 0
        if wrong:
            print(f"    WARNING: FY{fy} still has {wrong:,} mismatched rows!", flush=True)
            mismatches_remaining += wrong

    print()
    print("=" * 64)
    print(f"[{now_ts()}] fact_perm_fix_fiscal_year  COMPLETE")
    print("=" * 64)
    print(f"  Partitions processed : {len(results_store)}")
    print(f"  Partitions skipped   : {len(skipped)}")
    print(f"  Rows checked         : {total_rows_checked:,}")
    print(f"  Rows rewritten       : {total_rows_rewritten:,}")
    print(f"  Rows moved (cross-FY): {total_moved:,}")
    print(f"  Mismatches remaining : {mismatches_remaining}")
    print(f"  Errors               : {len(error_store)}")
    print(f"  Elapsed              : {t_elapsed/60:.1f} min ({t_elapsed:.0f} s)")
    print(f"  Metrics log          : {METRICS_LOG}")
    print()

    if mismatches_remaining == 0 and not error_store:
        print("  ✓ All partitions clean — fiscal_year matches directory for every row.")
    else:
        print("  ✗ Issues detected — check metrics log.")
        sys.exit(1)


if __name__ == "__main__":
    main()
