"""Check fact_lca column coverage after alias fix."""
import pandas as pd
from pathlib import Path

lca_dir = Path("artifacts/tables/fact_lca")
target_cols = ["is_fulltime", "job_title", "naics_code"]

print("=== fact_lca Column Coverage Check ===\n")

for fy_dir in sorted(lca_dir.iterdir()):
    if not fy_dir.is_dir():
        continue
    try:
        df = pd.read_parquet(fy_dir / "part-0.parquet")
        parts = []
        for col in target_cols:
            if col in df.columns:
                non_null = df[col].notna() & (df[col].astype(str) != "") & (df[col].astype(str) != "nan")
                pct = non_null.mean() * 100
                parts.append(f"{col}={pct:.0f}%")
            else:
                parts.append(f"{col}=MISSING")
        print(f"  {fy_dir.name} ({len(df):>8,} rows)  {', '.join(parts)}")
    except Exception as e:
        print(f"  {fy_dir.name}  ERROR: {e}")

# Overall
print("\n=== Overall ===")
all_dfs = []
for fy_dir in sorted(lca_dir.iterdir()):
    if not fy_dir.is_dir():
        continue
    try:
        df = pd.read_parquet(fy_dir / "part-0.parquet")
        all_dfs.append(df)
    except:
        pass

if all_dfs:
    total = pd.concat(all_dfs, ignore_index=True)
    print(f"Total rows: {len(total):,}")
    for col in target_cols:
        if col in total.columns:
            non_null = total[col].notna() & (total[col].astype(str) != "") & (total[col].astype(str) != "nan")
            print(f"  {col}: {non_null.sum():,} / {len(total):,} ({non_null.mean()*100:.1f}%)")
