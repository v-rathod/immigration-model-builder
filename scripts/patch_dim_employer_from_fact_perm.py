"""
patch_dim_employer_from_fact_perm.py
────────────────────────────────────
Expand dim_employer by adding stub rows for every employer found in
**fact_perm/** partitions that is not already in dim_employer.

Root cause of the gap
---------------------
build_dim_employer reads raw Excel files for the last 2 FYs (max_years=2)
and only 50K rows per file.  fact_perm/ contains all 20 FYs with 226K+
unique employers.  This patch reads employer_id + employer_name directly
from every fact_perm partition file and adds stub rows for all missing
employers.
"""
from __future__ import annotations

import pathlib
import sys

import pandas as pd

ROOT   = pathlib.Path(__file__).resolve().parents[1]
TABLES = ROOT / "artifacts" / "tables"

FACT_PERM_DIR = TABLES / "fact_perm"
FEAT_PATH     = TABLES / "employer_features.parquet"
DIM_EMP_PATH  = TABLES / "dim_employer.parquet"


def main() -> None:
    print("[PATCH DIM_EMPLOYER FROM FACT_PERM]")

    # ── 1. Load current dim_employer ─────────────────────────────────────────
    if not DIM_EMP_PATH.exists():
        print("  ERROR: dim_employer.parquet not found")
        sys.exit(1)

    df_dim = pd.read_parquet(DIM_EMP_PATH)
    existing_ids = set(df_dim["employer_id"].dropna())
    print(f"  existing dim_employer: {len(df_dim):,} rows, {len(existing_ids):,} unique IDs")

    # ── 2. Load employer_id + employer_name from all fact_perm partitions ────
    if not FACT_PERM_DIR.is_dir():
        print("  ERROR: fact_perm/ directory not found")
        sys.exit(1)

    perm_files = sorted(FACT_PERM_DIR.rglob("*.parquet"))
    print(f"  reading {len(perm_files)} fact_perm partition files...")
    chunks = []
    for pf in perm_files:
        try:
            df_part = pd.read_parquet(pf, columns=["employer_id", "employer_name"])
            df_part = df_part.dropna(subset=["employer_id"])
            chunks.append(df_part)
        except Exception as e:
            print(f"    WARN: {pf.name}: {e}")
    if not chunks:
        print("  ERROR: no fact_perm data read")
        sys.exit(1)
    df_all = pd.concat(chunks, ignore_index=True)
    # Deduplicate to one row per employer_id (keep most common name)
    perm_emp = (
        df_all.groupby("employer_id")["employer_name"]
        .agg(lambda x: x.mode().iloc[0] if len(x) > 0 else None)
        .reset_index()
    )
    print(f"  fact_perm: {len(perm_emp):,} unique employers")

    # ── 3. Find missing employers ─────────────────────────────────────────────
    missing = perm_emp[~perm_emp["employer_id"].isin(existing_ids)].copy()
    print(f"  missing from dim_employer: {len(missing):,}")

    if len(missing) == 0:
        print("  Nothing to add – dim_employer already complete.")
        return

    # ── 4. Build stub rows ────────────────────────────────────────────────────
    now_ts = pd.Timestamp.now(tz="UTC")

    # Match aliases dtype: if original stores as list objects use empty list, else use '[]' string
    _first_alias = df_dim["aliases"].dropna().iloc[0] if "aliases" in df_dim.columns and df_dim["aliases"].notna().any() else None
    _alias_val = [] if isinstance(_first_alias, list) else ('[]' if isinstance(_first_alias, str) else None)
    _first_src = df_dim["source_files"].dropna().iloc[0] if "source_files" in df_dim.columns and df_dim["source_files"].notna().any() else None
    _src_val = ["fact_perm_patch"] if isinstance(_first_src, list) else "fact_perm_patch_FY2022_2026"

    stubs = pd.DataFrame({
        "employer_id":   missing["employer_id"].values,
        "employer_name": missing["employer_name"].values,
        "aliases":       [_alias_val] * len(missing),
        "domain":        [None] * len(missing),
        "source_files":  [_src_val] * len(missing),
        "ingested_at":   [now_ts] * len(missing),
    })

    # Align schema to existing dim_employer (handle any extra columns)
    for col in df_dim.columns:
        if col not in stubs.columns:
            stubs[col] = None
    stubs = stubs[df_dim.columns]

    # ── 5. Concat and write ───────────────────────────────────────────────────
    df_out = pd.concat([df_dim, stubs], ignore_index=True)
    df_out = df_out.drop_duplicates(subset=["employer_id"], keep="first")
    df_out.to_parquet(DIM_EMP_PATH, index=False)
    print(f"  dim_employer written: {len(df_out):,} rows (+{len(stubs):,} stubs)")

    # ── 6. Validate coverage ──────────────────────────────────────────────────
    df_new_dim = pd.read_parquet(DIM_EMP_PATH)
    new_set = set(df_new_dim["employer_id"].dropna())
    print(f"  post-patch dim_employer: {len(df_new_dim):,} rows, {len(new_set):,} unique IDs")

    # Check coverage vs employer_features
    if FEAT_PATH.exists():
        all_feat_ids = pd.read_parquet(FEAT_PATH, columns=["employer_id"])["employer_id"].dropna()
        uniq_feat = set(all_feat_ids.unique())
        row_pct  = all_feat_ids.isin(new_set).sum() / len(all_feat_ids) if len(all_feat_ids) > 0 else 1.0
        uniq_pct = len(uniq_feat & new_set) / len(uniq_feat) if uniq_feat else 1.0
        print(f"  vs employer_features row-level coverage: {row_pct:.1%}")
        print(f"  vs employer_features unique-emp coverage: {uniq_pct:.1%}")
        assert row_pct >= 0.40, f"Coverage still below 40%: {row_pct:.1%}"
    else:
        print("  employer_features.parquet not found, skipping coverage check")

    print("  PASS: dim_employer expanded successfully")


if __name__ == "__main__":
    main()
