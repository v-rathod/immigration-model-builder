"""
Builds artifacts/tables/employer_risk_features.parquet by LEFT-joining
fact_warn_events to dim_employer via normalized employer_name key.
Read-only on source tables; writes one new derived parquet.

Usage: python scripts/make_employer_risk_features.py [--light]
"""
from __future__ import annotations

import argparse
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.utils.usage_registry import begin_task, end_task, log_stub

TABLES = ROOT / "artifacts" / "tables"
WARN_PATH = TABLES / "fact_warn_events.parquet"
DIM_EMP_PATH = TABLES / "dim_employer.parquet"
OUTPUT = TABLES / "employer_risk_features.parquet"


def _norm(s: str) -> str:
    """Normalize employer name: lowercase, strip punctuation, collapse spaces."""
    s = str(s).lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def main(light: bool = False) -> None:
    import pandas as pd

    if not WARN_PATH.exists():
        log_stub(
            "employer_risk_features",
            reason="fact_warn_events.parquet missing",
            inputs=[str(WARN_PATH), str(DIM_EMP_PATH)],
            outputs=[str(OUTPUT)],
        )
        print("WARN: fact_warn_events.parquet not found; stub logged")
        return

    df_warn = pd.read_parquet(WARN_PATH)
    if len(df_warn) == 0:
        log_stub(
            "employer_risk_features",
            reason="fact_warn_events is empty (0 rows)",
            inputs=[str(WARN_PATH)],
            outputs=[str(OUTPUT)],
        )
        print("WARN: fact_warn_events has 0 rows; stub logged")
        return

    begin_task(
        task="employer_risk_features",
        inputs=[str(WARN_PATH), str(DIM_EMP_PATH)],
        outputs=[str(OUTPUT)],
    )

    # Aggregate WARN: one row per employer (normalized key)
    df_warn = df_warn.copy()
    df_warn["employer_key"] = df_warn["employer_name_raw"].fillna("").apply(_norm)
    warn_agg = (
        df_warn.groupby("employer_key", dropna=False)
        .agg(
            total_warn_events=("employer_name_raw", "count"),
            total_employees_affected=("employees_affected", "sum"),
            states=("state", lambda x: sorted(x.dropna().unique().tolist())),
            employer_name_raw=("employer_name_raw", "first"),
        )
        .reset_index()
    )

    # JOIN to dim_employer if available
    join_rate: float = 0.0
    joined_count: int = 0
    if DIM_EMP_PATH.exists():
        df_dim = pd.read_parquet(DIM_EMP_PATH, columns=["employer_id", "employer_name"])
        df_dim["employer_key"] = df_dim["employer_name"].fillna("").apply(_norm)
        merged = warn_agg.merge(
            df_dim[["employer_key", "employer_id"]],
            on="employer_key",
            how="left",
        )
        joined_count = merged["employer_id"].notna().sum()
        join_rate = joined_count / len(merged) if len(merged) > 0 else 0.0
    else:
        merged = warn_agg.copy()
        merged["employer_id"] = None
        print("WARN: dim_employer.parquet not found; employer_id will be null")

    # Add risk flag
    merged["is_warn_flagged"] = True

    TABLES.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(OUTPUT, index=False)

    metrics = {
        "warn_rows_raw": len(df_warn),
        "warn_employers": len(warn_agg),
        "join_rate": round(join_rate, 4),
        "joined_count": int(joined_count),
        "states": sorted(df_warn["state"].dropna().unique().tolist()),
    }
    end_task("employer_risk_features", metrics=metrics)
    print(
        f"OK: employer_risk_features.parquet written — {len(merged):,} employers, "
        f"join_rate={join_rate:.1%} ({joined_count}/{len(merged)})"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--light", action="store_true")
    args = parser.parse_args()
    main(light=args.light)
