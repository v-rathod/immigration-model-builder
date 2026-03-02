#!/usr/bin/env python3
"""Deep-dive investigation of audit findings."""
import pandas as pd
import numpy as np

# 1) fact_cutoffs_all country values
print("=" * 60)
print("1) CUTOFFS: Country distribution")
print("=" * 60)
df = pd.read_parquet('artifacts/tables/fact_cutoffs_all.parquet')
print(df['country'].value_counts())
print()

els = df[df['country'] == 'EL SALVADOR GUATEMALA HONDURAS']
print(f'EL SALVADOR GUATEMALA HONDURAS rows: {len(els)}')
if len(els) > 0:
    print('  Categories:', els['category'].unique().tolist())
    print('  Charts:', els['chart'].unique().tolist())
    print('  Year range:', els['bulletin_year'].min(), '-', els['bulletin_year'].max())
print()

# 2) fact_perm columns & soc_code nulls
print("=" * 60)
print("2) PERM: Columns and soc_code nulls by FY")
print("=" * 60)
fp = pd.read_parquet('artifacts/tables/fact_perm')
print('Columns:', list(fp.columns))
print()

fp['fy'] = fp['fiscal_year'].astype(int)
null_by_fy = fp.groupby('fy').agg(
    total=('soc_code', 'size'),
    nulls=('soc_code', lambda x: x.isna().sum()),
)
null_by_fy['null_pct'] = (null_by_fy['nulls'] / null_by_fy['total'] * 100).round(1)
print(null_by_fy.to_string())
print()

# Check if wage column exists under different name
wage_cols = [c for c in fp.columns if 'wage' in c.lower() or 'pw_' in c.lower() or 'salary' in c.lower()]
print(f'Wage-related columns: {wage_cols}')
print()

# 3) fact_lca columns
print("=" * 60)
print("3) LCA: Columns")
print("=" * 60)
fl = pd.read_parquet('artifacts/tables/fact_lca')
print('Columns:', list(fl.columns))
wage_cols_lca = [c for c in fl.columns if 'wage' in c.lower() or 'pay' in c.lower() or 'salary' in c.lower()]
emp_cols = [c for c in fl.columns if 'employer' in c.lower() or 'company' in c.lower()]
print(f'Wage columns: {wage_cols_lca}')
print(f'Employer columns: {emp_cols}')
print()

# 4) Check cutoff jumps — are they real retrogression or parsing errors?
print("=" * 60)
print("4) CUTOFF JUMPS: EB1 India (biggest jump flagged)")
print("=" * 60)
eb1_ind = df[(df['category'] == 'EB1') & (df['country'] == 'IND') & (df['chart'] == 'FAD')]
eb1_ind = eb1_ind.sort_values(['bulletin_year', 'bulletin_month'])
print(eb1_ind[['bulletin_year', 'bulletin_month', 'status_flag', 'cutoff_date']].to_string(index=False))
print()

# 5) Check EB3-Other jumps (Oct 2025 flagged for CHN, IND, MEX, PHL)
print("=" * 60)
print("5) EB3-Other: All countries Sep-Oct 2025")
print("=" * 60)
eb3o = df[(df['category'] == 'EB3-Other') & (df['chart'] == 'FAD')]
eb3o = eb3o.sort_values(['country', 'bulletin_year', 'bulletin_month'])
# Show last 6 months per country
for ctry in eb3o['country'].unique():
    sub = eb3o[eb3o['country'] == ctry].tail(8)
    print(f"\n  {ctry}:")
    for _, row in sub.iterrows():
        print(f"    {row['bulletin_year']}-{row['bulletin_month']:02d}  {row['status_flag']}  {row['cutoff_date']}")

# 6) dim_soc null titles
print()
print("=" * 60)
print("6) dim_soc: null soc_title analysis")
print("=" * 60)
ds = pd.read_parquet('artifacts/tables/dim_soc.parquet')
nulls = ds[ds['soc_title'].isna()]
non_nulls = ds[~ds['soc_title'].isna()]
print(f'Total: {len(ds)}, With title: {len(non_nulls)}, Without: {len(nulls)}')
print(f'Sample null-title codes: {nulls["soc_code"].head(10).tolist()}')
if 'soc_system' in ds.columns:
    print(f'Null titles by soc_system: {nulls["soc_system"].value_counts().to_dict()}')
