"""
Log usage of dim_visa_ceiling and fact_waiting_list as inputs to backlog_estimates.
Read-only — does not modify any parquet files.
"""
from __future__ import annotations

import pathlib
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.utils.usage_registry import begin_task, end_task, log_stub

TABLES = ROOT / "artifacts" / "tables"

INPUTS = [
    str(TABLES / "dim_visa_ceiling.parquet"),
    str(TABLES / "fact_waiting_list.parquet"),
    str(TABLES / "fact_cutoffs_all.parquet"),
]
OUTPUT = str(TABLES / "backlog_estimates.parquet")


def main() -> None:
    missing = [p for p in INPUTS if not pathlib.Path(p).exists()]
    out_path = pathlib.Path(OUTPUT)

    if not out_path.exists():
        log_stub(
            "backlog_usage",
            reason="backlog_estimates.parquet missing",
            inputs=INPUTS,
            outputs=[OUTPUT],
        )
        print("WARN: backlog_estimates.parquet not found; stub logged.")
        return

    try:
        import pandas as pd
        df = pd.read_parquet(out_path)
        row_count = len(df)
        distinct_categories = df["category"].nunique() if "category" in df.columns else 0
        distinct_countries = df["country"].nunique() if "country" in df.columns else 0
    except Exception as exc:
        log_stub("backlog_usage", reason=f"read error: {exc}", inputs=INPUTS, outputs=[OUTPUT])
        print(f"WARN: could not read backlog_estimates: {exc}")
        return

    begin_task(
        task="backlog_usage",
        inputs=[str(p) for p in INPUTS if pathlib.Path(p).exists()],
        outputs=[OUTPUT],
    )
    metrics = {
        "row_count": row_count,
        "distinct_categories": int(distinct_categories),
        "distinct_countries": int(distinct_countries),
        "missing_inputs": missing,
    }
    end_task("backlog_usage", metrics=metrics)
    print(f"OK: backlog_usage logged — {row_count:,} rows, "
          f"{distinct_categories} categories, {distinct_countries} countries")


if __name__ == "__main__":
    main()
