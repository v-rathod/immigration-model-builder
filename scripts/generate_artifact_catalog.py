#!/usr/bin/env python3
"""
generate_artifact_catalog.py
============================
Writes artifacts/metrics/p3_artifact_catalog.json with:
  {name, path, schema (field:type), row_count, partition_keys,
   last_updated, intended_charts}

Run:  python scripts/generate_artifact_catalog.py
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"
METRICS = ROOT / "artifacts" / "metrics"
METRICS.mkdir(parents=True, exist_ok=True)


# Declared intended charts for each dataset
INTENDED_CHARTS: dict[str, list[str]] = {
    "fact_cutoffs_all": [
        "Visa Bulletin timeline heatmap",
        "Priority date cutoff progression line chart",
        "Category comparison bar chart",
    ],
    "fact_cutoff_trends": [
        "Monthly advancement trend line",
        "Velocity 3m / 6m rolling chart",
        "Retrogression flag timeline",
        "Cumulative retrogression count chart",
    ],
    "category_movement_metrics": [
        "Category-level volatility score bar chart",
        "Retrogression events 12m heatmap",
        "Next movement prediction indicator",
    ],
    "employer_monthly_metrics": [
        "Employer approval rate trend line",
        "Monthly filings/approvals stacked bar",
        "Employer 36m approval rate scatter",
    ],
    "worksite_geo_metrics": [
        "State-level choropleth map of filings",
        "SOC×area competitiveness ratio heatmap",
        "Wage ratio vs OEWS benchmark scatter",
    ],
    "salary_benchmarks": [
        "SOC percentile salary range chart (box/candlestick)",
        "ref_year salary trend line",
        "Wage level distribution bar chart",
    ],
    "soc_demand_metrics": [
        "SOC demand ranking table",
        "Competitiveness percentile bar chart",
        "Top employer per SOC treemap",
    ],
    "processing_times_trends": [
        "Processing time trend line (if data available)",
    ],
    "backlog_estimates": [
        "Backlog months to clear estimate line chart",
        "Inflow vs advancement comparison chart",
    ],
    "employer_features": [
        "Employer feature distribution histograms",
        "Approval rate trend scatter",
    ],
    "employer_friendliness_scores": [
        "EFS tier distribution pie chart",
        "Top N employers by EFS bar chart",
        "EFS vs approval rate scatter",
    ],
}

# Partition keys per dataset
PARTITION_KEYS: dict[str, list[str]] = {
    "fact_cutoffs": ["bulletin_year", "bulletin_month"],
    "fact_perm": ["fiscal_year"],
    "fact_lca": ["fiscal_year"],
    "fact_oews": ["ref_year"],
    "fact_perm_unique_case": ["fiscal_year"],
}


def _get_schema(path: Path) -> dict[str, str]:
    """Get field:type dict from a parquet file."""
    try:
        meta = pq.read_schema(path)
        return {field.name: str(field.type) for field in meta}
    except Exception:
        return {}


def _get_row_count(path: Path) -> int:
    try:
        if path.is_dir():
            total = 0
            for pf in sorted(path.rglob("*.parquet")):
                total += pq.read_metadata(pf).num_rows
            return total
        return pq.read_metadata(path).num_rows
    except Exception:
        return -1


def _get_last_modified(path: Path) -> str:
    try:
        if path.is_dir():
            mtime = max(pf.stat().st_mtime for pf in path.rglob("*.parquet"))
        else:
            mtime = path.stat().st_mtime
        return datetime.fromtimestamp(mtime, tz=timezone.utc).isoformat()
    except Exception:
        return "unknown"


def main() -> None:
    t0 = time.time()
    print("=" * 60)
    print("GENERATE ARTIFACT CATALOG")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    catalog: list[dict] = []

    # Collect all parquet tables and directories
    all_items: list[tuple[str, Path]] = []

    for item in sorted(TABLES.iterdir()):
        name = item.stem if item.suffix == ".parquet" else item.name
        if item.name.startswith("_") or item.suffix in (".json", ".log", ".tmp"):
            continue
        if item.suffix == ".parquet" or item.is_dir():
            all_items.append((name, item))

    for name, path in all_items:
        print(f"  Cataloging: {name} …")

        # Schema: use first file found for dirs
        schema_path = path
        if path.is_dir():
            files = sorted(path.rglob("*.parquet"))
            if files:
                schema_path = files[0]
            else:
                continue

        schema = _get_schema(schema_path)
        row_count = _get_row_count(path)
        last_updated = _get_last_modified(path)

        # Partition keys: from directory structure
        part_keys = PARTITION_KEYS.get(name, [])
        if path.is_dir() and not part_keys:
            # Auto-detect from directory names
            for sub in sorted(path.iterdir()):
                if sub.is_dir() and "=" in sub.name:
                    part_keys.append(sub.name.split("=")[0])
                    break

        entry = {
            "name": name,
            "path": str(path.relative_to(ROOT)),
            "type": "partitioned_directory" if path.is_dir() else "single_file",
            "schema": schema,
            "row_count": row_count,
            "num_columns": len(schema),
            "partition_keys": part_keys,
            "last_updated": last_updated,
            "intended_charts": INTENDED_CHARTS.get(name, []),
        }
        catalog.append(entry)

    # Write catalog
    out_path = METRICS / "p3_artifact_catalog.json"
    with open(out_path, "w") as fh:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_artifacts": len(catalog),
            "artifacts": catalog,
        }, fh, indent=2)

    elapsed = time.time() - t0
    print(f"\n✓ Catalog written: {out_path.relative_to(ROOT)}")
    print(f"  Artifacts cataloged: {len(catalog)}")
    print(f"  Elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
