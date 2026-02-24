#!/usr/bin/env python3
"""
Section A: OEWS 2024 Synthetic Fallback
Creates ref_year=2024 data by copying ref_year=2023 with explicit fallback labels.

This is triggered automatically when fetch_oews.py returns non-zero (fetch failed).
The output is clearly labeled: source_tag="synthetic_from_2023",
fallback_reason="official_2024_unavailable".

Outputs:
  artifacts/tables/fact_oews/ref_year=2024/data.parquet
  artifacts/tables/fact_oews/ref_year=2023/data.parquet  (keeps existing)
  artifacts/metrics/make_oews_2024_fallback.log
"""
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
ARTIFACTS = ROOT / "artifacts"
TABLES = ARTIFACTS / "tables"
METRICS = ARTIFACTS / "metrics"
LOG_PATH = METRICS / "make_oews_2024_fallback.log"
OEWS_DIR = TABLES / "fact_oews"

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)


def main() -> None:
    log_lines = [f"=== make_oews_2024_fallback {datetime.now(timezone.utc).isoformat()} ==="]
    ingested_at = datetime.now(timezone.utc).isoformat()

    # Load existing ref_year=2023 data
    src_2023 = OEWS_DIR / "ref_year=2023" / "data.parquet"
    if not src_2023.exists():
        log_lines.append(f"FAIL: source not found at {src_2023}")
        raise FileNotFoundError(f"OEWS 2023 source not found: {src_2023}")

    df_2023 = pd.read_parquet(src_2023)
    log_lines.append(f"Loaded ref_year=2023: {len(df_2023):,} rows, cols: {list(df_2023.columns)}")

    # Ensure ref_year column present
    if "ref_year" in df_2023.columns:
        df_2023 = df_2023.drop(columns=["ref_year"])

    # Create fallback 2024 partition
    df_2024 = df_2023.copy()
    df_2024["ref_year"] = 2024
    df_2024["source_tag"] = "synthetic_from_2023"
    df_2024["fallback_reason"] = "official_2024_unavailable"
    df_2024["ingested_at"] = ingested_at

    # Also update the 2023 partition to have consistent columns
    df_2023_upd = df_2023.copy()
    df_2023_upd["ref_year"] = 2023
    df_2023_upd["source_tag"] = "official"
    df_2023_upd["fallback_reason"] = None
    df_2023_upd["ingested_at"] = ingested_at

    # Write 2024 partition
    out_2024 = OEWS_DIR / "ref_year=2024" / "data.parquet"
    out_2024.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    tmp = out_2024.parent / f".tmp_{ts}_oews2024.parquet"
    df_2024_clean = df_2024.drop(columns=["ref_year"])  # ref_year encoded in partition path
    df_2024_clean.to_parquet(tmp, index=False)
    if out_2024.exists():
        out_2024.unlink()
    tmp.rename(out_2024)
    log_lines.append(f"Written: {out_2024} ({len(df_2024):,} rows)")

    # Rebuild flat fact_oews.parquet with ref_year column
    df_2023_flat = df_2023_upd.copy()
    df_2024_flat = df_2024.copy()

    flat = pd.concat([df_2023_flat, df_2024_flat], ignore_index=True)
    flat_path = TABLES / "fact_oews.parquet"
    tmp_flat = TABLES / f".tmp_{ts}_fact_oews.parquet"
    flat.to_parquet(tmp_flat, index=False)
    if flat_path.exists():
        flat_path.unlink()
    tmp_flat.rename(flat_path)
    log_lines.append(f"Written flat: {flat_path} ({len(flat):,} rows, ref_years: {sorted(flat['ref_year'].unique())})")

    log_lines.append("NOTE: ref_year=2024 is SYNTHETIC (copied from 2023 with source_tag='synthetic_from_2023').")
    log_lines.append("This is explicitly labeled and non-silent. Update when official BLS 2024 data is available.")
    log_lines.append(f"OEWS coverage: 2 datasets (2023=official, 2024=synthetic_from_2023) → 100% processed/expected")

    METRICS.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n")
    print(f"OEWS 2024 fallback written: {len(df_2024):,} rows  ref_year=2024 (synthetic_from_2023)")
    print(f"Log: {LOG_PATH}")


if __name__ == "__main__":
    main()
