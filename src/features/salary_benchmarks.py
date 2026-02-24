"""Salary benchmark feature engineering.

NOTE: The authoritative salary_benchmarks.parquet is built by
scripts/make_salary_benchmarks.py (OEWS-based percentile table with
p10/p25/median/p75/p90 per SOC × ref_year × area).

This function is a no-op shim: if the output already exists with real data
(rows > 0), it is left untouched. Only if the file is missing does it write
a schema-only placeholder so the pipeline doesn't crash.
"""

from pathlib import Path
import pandas as pd


def build_salary_benchmarks(in_tables: Path, out_path: Path) -> None:
    """Skip if real data exists; otherwise write schema-only placeholder."""
    print(f"[SALARY BENCHMARKS]")

    # If existing file has real data, do not overwrite
    if out_path.exists():
        try:
            import pyarrow.parquet as pq
            meta = pq.read_metadata(out_path)
            if meta.num_rows > 0:
                print(f"  Existing salary_benchmarks.parquet has {meta.num_rows:,} rows — skipping (use make_salary_benchmarks.py to rebuild)")
                return
        except Exception:
            pass  # if we can't read it, proceed to create placeholder

    print(f"  No real data found; writing schema placeholder")
    # Schema matches make_salary_benchmarks.py output
    df_placeholder = pd.DataFrame(columns=[
        "soc_code", "area_code", "ref_year", "p10", "p25", "median", "p75", "p90",
    ])
    df_placeholder.to_parquet(out_path, index=False)
    print(f"  Created placeholder: {out_path}")
    print(f"  Run 'python scripts/make_salary_benchmarks.py' for real data")

