#!/usr/bin/env python3
"""Verify PHL and VIETNAM fix."""
import pandas as pd
df = pd.read_parquet('artifacts/tables/fact_cutoffs')
for c in ['bulletin_year','bulletin_month','fiscal_year']:
    if c in df.columns:
        df[c] = df[c].astype(int)

# Country coverage by year
pivot = df.groupby(['bulletin_year', 'country']).size().unstack(fill_value=0)
print("=== Country rows by year ===")
print(pivot.to_string())
print()

# PHL check
phl = df[df['country'] == 'PHL']
print(f"PHL total rows: {len(phl)}")
print(f"PHL year range: {phl['bulletin_year'].min()}-{phl['bulletin_year'].max()}")
print()

# VIETNAM check
vnm = df[df['country'] == 'VIETNAM']
print(f"VIETNAM total rows: {len(vnm)}")
if len(vnm) > 0:
    print(f"VIETNAM year range: {vnm['bulletin_year'].min()}-{vnm['bulletin_year'].max()}")
print()

# EB2/IND FAD still correct?
eb2_ind = df[(df['category']=='EB2') & (df['country']=='IND') & (df['chart']=='FAD')]
current = (eb2_ind['status_flag'] == 'C').sum()
print(f"EB2/IND FAD 'Current': {current} (should be 0)")

# Spot check PHL 2018
phl_2018 = df[(df['bulletin_year']==2018) & (df['country']=='PHL')]
print(f"\nPHL 2018 rows: {len(phl_2018)} (was 0 before)")

# EB2/PHL FAD now
eb2_phl = df[(df['category']=='EB2') & (df['country']=='PHL') & (df['chart']=='FAD')]
eb2_phl = eb2_phl.sort_values(['bulletin_year','bulletin_month'])
print("\n=== EB2/PHL FAD (should now have continuous data) ===")
for _, r in eb2_phl.iterrows():
    print(f"  {r['bulletin_year']}-{r['bulletin_month']:02d}  {r['status_flag']}  {r['cutoff_date']}")
