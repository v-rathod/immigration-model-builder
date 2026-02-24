#!/usr/bin/env python3
"""
Drop duplicate (case_number, fiscal_year) rows in fact_perm partition files.
Keeps the LAST occurrence; skips rows where case_number is null (those are simply
kept as-is — nulls don't violate the non-null part of the PK constraint).
"""
import pandas as pd
from pathlib import Path
import sys

DRY_RUN = "--write" not in sys.argv
perm_dir = Path("artifacts/tables/fact_perm")

total_before = total_after = 0
files_modified = 0

for pf in sorted(perm_dir.rglob("*.parquet")):
    df = pd.read_parquet(pf)
    # Get fiscal_year from path if not in file
    path_fy = None
    for part in pf.parts:
        if part.startswith("fiscal_year="):
            path_fy = part.split("=", 1)[1]

    if "fiscal_year" not in df.columns and path_fy:
        df["fiscal_year"] = path_fy

    # Split: rows with non-null case_number vs null
    mask_notnull = df["case_number"].notna()
    df_notnull = df[mask_notnull].copy()
    df_null = df[~mask_notnull].copy()

    df_deduped = df_notnull.drop_duplicates(subset=["case_number", "fiscal_year"], keep="last")
    dropped = len(df_notnull) - len(df_deduped)

    total_before += len(df)
    total_after += len(df_deduped) + len(df_null)

    if dropped > 0:
        print(f"  {pf.parent.name}: {len(df):,} → {len(df_deduped) + len(df_null):,} rows (dropped {dropped})")
        files_modified += 1
        if not DRY_RUN:
            # Reconstruct: deduped non-null rows + original null rows
            df_out = pd.concat([df_deduped, df_null], ignore_index=True)
            # Remove partition columns added from path (not stored in file)
            if path_fy and "fiscal_year" not in pd.read_parquet(pf).columns:
                df_out = df_out.drop(columns=["fiscal_year"])
            df_out.to_parquet(pf, index=False)

print(f"\nTotal: {total_before:,} → {total_after:,} rows ({total_before - total_after:,} dropped)")
print(f"Files affected: {files_modified}")
if DRY_RUN:
    print("\nDRY RUN — pass --write to apply")
