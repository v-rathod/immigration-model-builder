"""
Builds artifacts/tables/visa_demand_metrics.parquet by aggregating from
fact_visa_issuance, fact_visa_applications, and fact_niv_issuance.
Read-only on source tables; writes one new derived parquet.

Usage: python scripts/make_visa_demand_metrics.py [--light]
"""
from __future__ import annotations

import argparse
import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.utils.usage_registry import begin_task, end_task

TABLES = ROOT / "artifacts" / "tables"
OUTPUT = TABLES / "visa_demand_metrics.parquet"

SOURCES = {
    "fact_visa_issuance": TABLES / "fact_visa_issuance.parquet",
    "fact_visa_applications": TABLES / "fact_visa_applications.parquet",
    "fact_niv_issuance": TABLES / "fact_niv_issuance.parquet",
    "fact_iv_post": TABLES / "fact_iv_post.parquet",
}


def main(light: bool = False) -> None:
    import pandas as pd

    present = {name: path for name, path in SOURCES.items() if path.exists()}
    if not present:
        print("WARN: no source tables found; skipping visa_demand_metrics build")
        return

    present_inputs = [str(p) for p in present.values()]
    begin_task(
        task="visa_demand_metrics",
        inputs=present_inputs,
        outputs=[str(OUTPUT)],
    )

    frames: list[pd.DataFrame] = []

    # --- fact_visa_issuance: country × category × fiscal_year → total issued ---
    if "fact_visa_issuance" in present:
        df_iv = pd.read_parquet(present["fact_visa_issuance"])
        agg_iv = (
            df_iv.groupby(["fiscal_year", "category", "country"], dropna=False)["issued"]
            .sum()
            .reset_index()
            .rename(columns={"issued": "iv_issued"})
        )
        agg_iv["source"] = "visa_issuance"
        frames.append(agg_iv.rename(columns={"iv_issued": "count_issued"}))

    # --- fact_visa_applications: country × visa_class × fiscal_year → applications ---
    if "fact_visa_applications" in present:
        df_va = pd.read_parquet(present["fact_visa_applications"])
        agg_va = (
            df_va.groupby(["fiscal_year", "category", "country"], dropna=False)["applications"]
            .sum()
            .reset_index()
            .rename(columns={"applications": "count_issued"})
        )
        agg_va["source"] = "visa_applications"
        frames.append(agg_va)

    # --- fact_niv_issuance: country × visa_class × fiscal_year → issued ---
    if "fact_niv_issuance" in present:
        df_niv = pd.read_parquet(present["fact_niv_issuance"])
        # summarise to fiscal_year × country level (NIV has visa_class not category)
        agg_niv = (
            df_niv.groupby(["fiscal_year", "visa_class", "country"], dropna=False)["issued"]
            .sum()
            .reset_index()
            .rename(columns={"visa_class": "category", "issued": "count_issued"})
        )
        agg_niv["source"] = "niv_issuance"
        frames.append(agg_niv)

    # --- fact_iv_post: post × visa_class × fiscal_year → issued (monthly) ---
    # Aggregates monthly post-level IV issuances up to fiscal_year × visa_class × post
    if "fact_iv_post" in present:
        df_post = pd.read_parquet(present["fact_iv_post"])
        agg_post = (
            df_post.groupby(["fiscal_year", "visa_class", "post"], dropna=False)["issued"]
            .sum()
            .reset_index()
            .rename(columns={"visa_class": "category", "post": "country",
                              "issued": "count_issued"})
        )
        agg_post["source"] = "iv_post"
        frames.append(agg_post)

    if not frames:
        print("WARN: no usable frames built; skipping output write")
        end_task("visa_demand_metrics", {"row_count": 0, "sources_used": 0})
        return

    result = pd.concat(frames, ignore_index=True)

    # Ensure key columns exist; fill missing with defaults
    for col in ["fiscal_year", "category", "country", "count_issued", "source"]:
        if col not in result.columns:
            result[col] = None

    result = result[["fiscal_year", "category", "country", "count_issued", "source"]]
    result["count_issued"] = pd.to_numeric(result["count_issued"], errors="coerce").fillna(0).astype("int64")

    TABLES.mkdir(parents=True, exist_ok=True)
    result.to_parquet(OUTPUT, index=False)

    metrics = {
        "row_count": len(result),
        "sources_used": len(present),
        "source_names": list(present.keys()),
        "distinct_fiscal_years": int(result["fiscal_year"].nunique()),
        "distinct_countries": int(result["country"].nunique()),
        "distinct_categories": int(result["category"].nunique()),
    }
    end_task("visa_demand_metrics", metrics=metrics)
    print(f"OK: visa_demand_metrics.parquet written — {len(result):,} rows "
          f"from {len(present)} sources: {list(present.keys())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--light", action="store_true", help="lightweight mode (same as default)")
    args = parser.parse_args()
    main(light=args.light)
