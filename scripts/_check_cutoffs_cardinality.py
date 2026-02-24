#!/usr/bin/env python3
"""Check fact_cutoffs_all structure and cardinality."""
import pandas as pd
from pathlib import Path

TABLES = Path("artifacts/tables")
df = pd.read_parquet(TABLES / "fact_cutoffs_all.parquet")
print(f"fact_cutoffs_all: {len(df):,} rows")
print(f"  columns: {list(df.columns)}")
pk4 = ["bulletin_year","bulletin_month","category","country"]
pk5 = ["bulletin_year","bulletin_month","category","country","chart"]
if "chart" in df.columns:
    print(f"  chart values: {sorted(df['chart'].unique())}")
    dups4 = df.duplicated(subset=pk4).sum()
    dups5 = df.duplicated(subset=pk5).sum()
    print(f"  dups on PK4 (yr,mo,cat,cty): {dups4}")
    print(f"  dups on PK5 (+chart): {dups5}")
    # how many have both DFF and FAD?
    if "DFF" in df["chart"].values and "FAD" in df["chart"].values:
        both = (
            df.groupby(pk4)["chart"]
            .nunique()
            .eq(2)
            .sum()
        )
        print(f"  PK4 combos with both DFF+FAD charts: {both}")
else:
    print("  no 'chart' column")
    dups4 = df.duplicated(subset=pk4).sum()
    print(f"  dups on PK4: {dups4}")

print(f"  yr range: {df['bulletin_year'].min()}-{df['bulletin_year'].max()}")
print(f"  distinct (yr,mo): {df[['bulletin_year','bulletin_month']].drop_duplicates().shape[0]}")
