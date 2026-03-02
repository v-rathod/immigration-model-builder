"""Audit employer normalization state across all P2 artifacts."""
import pandas as pd
from pathlib import Path

artifacts = Path("artifacts/tables")

print("=== ALL PARQUET FILES WITH employer_name column ===")
for f in sorted(artifacts.rglob("*.parquet")):
    try:
        df = pd.read_parquet(f)
        if "employer_name" not in df.columns:
            continue
        n = df["employer_name"].nunique()
        sample = df[
            df["employer_name"].str.lower().str.contains("google", na=False)
        ]["employer_name"].unique()[:4]
        has_eid = "employer_id" in df.columns
        print(f"  {f.name:50s}  unique={n:,}  has_eid={has_eid}  google={list(sample)}")
    except Exception as e:
        print(f"  ERROR reading {f.name}: {e}")

print()
print("=== NORMALIZATION QUALITY: raw names with case/punct variants ===")
for f in sorted(artifacts.rglob("*.parquet")):
    try:
        df = pd.read_parquet(f)
        if "employer_name" not in df.columns:
            continue
        names = df["employer_name"].dropna().unique()
        # Find names that differ only in case
        lowered = {}
        for nm in names:
            key = str(nm).lower().strip().rstrip(".,;")
            lowered.setdefault(key, []).append(nm)
        dupes = {k: v for k, v in lowered.items() if len(v) > 1}
        if dupes:
            sample = list(dupes.items())[:3]
            print(f"  {f.name}: {len(dupes):,} case/punct dupe groups, e.g. {sample}")
        else:
            print(f"  {f.name}: OK (no case/punct dupes)")
    except Exception as e:
        print(f"  ERROR {f.name}: {e}")

print()
print("=== employer_salary_profiles: check employer_name normalization ===")
sal_path = artifacts / "employer_salary_profiles.parquet"
if sal_path.exists():
    sal = pd.read_parquet(sal_path)
    print(f"  Columns: {list(sal.columns)}")
    print(f"  Unique employer_names: {sal['employer_name'].nunique():,}")
    # Show google variants
    g = sal[sal["employer_name"].str.lower().str.contains("google", na=False)]["employer_name"].unique()
    print(f"  Google variants: {list(g)}")
