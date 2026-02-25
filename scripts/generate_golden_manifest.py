#!/usr/bin/env python3
"""
Generate a golden manifest of all P2 artifacts for regression detection.
=========================================================================
Reads every Parquet artifact in artifacts/tables/ and records:
  - row count, column count, column names, dtypes, sorted dtype signature
  - hash of first N rows (deterministic sample for drift detection)
  - min/max of key numeric columns

Output: artifacts/metrics/golden_manifest.json
Run after every validated build to update the baseline.

Usage:
    python3 scripts/generate_golden_manifest.py              # generate manifest
    python3 scripts/generate_golden_manifest.py --update      # same (explicit)
"""
from __future__ import annotations

import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"
MANIFEST_PATH = ROOT / "artifacts" / "metrics" / "golden_manifest.json"

# Artifacts to snapshot — name maps to parquet file or partitioned directory
ARTIFACTS = [
    # Dimensions
    "dim_employer", "dim_soc", "dim_country", "dim_area",
    "dim_visa_class", "dim_visa_ceiling",
    # Facts (flat)
    "fact_perm_all", "fact_cutoffs_all", "fact_oews",
    "fact_niv_issuance", "fact_visa_issuance", "fact_visa_applications",
    "fact_dhs_admissions", "fact_uscis_approvals", "fact_warn_events",
    "fact_waiting_list",
    # Facts (partitioned dirs) — read as directory
    "fact_perm", "fact_cutoffs", "fact_oews",
    # Features
    "employer_features", "salary_benchmarks", "employer_monthly_metrics",
    "employer_risk_features", "soc_demand_metrics", "visa_demand_metrics",
    "worksite_geo_metrics", "category_movement_metrics",
    "backlog_estimates", "fact_cutoff_trends", "processing_times_trends",
    # Models
    "employer_friendliness_scores", "employer_friendliness_scores_ml",
    "pd_forecasts",
    # Stubs (may be 0 rows)
    "employer_scores", "oews_wages", "visa_bulletin",
    "fact_trac_adjudications", "fact_acs_wages",
]

# Remove duplicates (fact_oews appears both flat and partitioned)
ARTIFACTS = list(dict.fromkeys(ARTIFACTS))

SAMPLE_ROWS = 100  # rows to hash for drift detection


def _load(name: str) -> pd.DataFrame | None:
    """Load a parquet file or partitioned directory.  Returns None if missing."""
    flat = TABLES / f"{name}.parquet"
    partitioned = TABLES / name

    if flat.is_file():
        return pd.read_parquet(flat)
    elif partitioned.is_dir():
        parts = sorted(partitioned.glob("**/*.parquet"))
        if not parts:
            return None
        frames = []
        for p in parts:
            try:
                frames.append(pd.read_parquet(p))
            except Exception:
                continue
        if not frames:
            return None
        return pd.concat(frames, ignore_index=True)
    return None


def _dtype_signature(df: pd.DataFrame) -> str:
    """Sorted string of col:dtype pairs — detects schema changes."""
    pairs = sorted(f"{c}:{df[c].dtype}" for c in df.columns)
    return "|".join(pairs)


def _sample_hash(df: pd.DataFrame, n: int = SAMPLE_ROWS) -> str:
    """SHA-256 of the first N rows sorted by all columns, for drift detection."""
    if len(df) == 0:
        return "empty"
    sample = df.head(n)
    try:
        sample = sample.sort_values(list(sample.columns)).reset_index(drop=True)
    except TypeError:
        pass  # unhashable columns — skip sort
    raw = sample.to_csv(index=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def _numeric_bounds(df: pd.DataFrame) -> dict:
    """Min/max of numeric columns (for quick range drift)."""
    bounds = {}
    for col in df.select_dtypes(include="number").columns:
        series = df[col].dropna()
        if len(series) > 0:
            bounds[col] = {
                "min": float(series.min()),
                "max": float(series.max()),
            }
    return bounds


def generate_manifest() -> dict:
    """Build the golden manifest dict."""
    entries = {}
    for name in ARTIFACTS:
        df = _load(name)
        if df is None:
            entries[name] = {"status": "missing"}
            continue

        entries[name] = {
            "status": "ok",
            "rows": len(df),
            "cols": len(df.columns),
            "columns": sorted(df.columns.tolist()),
            "dtype_signature": _dtype_signature(df),
            "sample_hash": _sample_hash(df),
            "numeric_bounds": _numeric_bounds(df),
        }

    manifest = {
        "program": "NorthStar",
        "project": "Meridian (P2)",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "artifact_count": len(entries),
        "ok_count": sum(1 for v in entries.values() if v.get("status") == "ok"),
        "missing_count": sum(1 for v in entries.values() if v.get("status") == "missing"),
        "artifacts": entries,
    }
    return manifest


def main():
    print("Generating golden manifest …")
    manifest = generate_manifest()

    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST_PATH, "w") as f:
        json.dump(manifest, f, indent=2)

    ok = manifest["ok_count"]
    miss = manifest["missing_count"]
    print(f"  {ok} artifacts snapshotted, {miss} missing")
    print(f"  Written to {MANIFEST_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
