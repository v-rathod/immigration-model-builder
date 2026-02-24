"""Debug SOC/area code format mismatch between PERM and OEWS."""
import pandas as pd
from pathlib import Path

perm_dir = Path('artifacts/tables/fact_perm')
pfiles = sorted(perm_dir.rglob('*.parquet'))[:2]
perm = pd.concat([pd.read_parquet(pf) for pf in pfiles])
print('PERM soc_code samples:', perm['soc_code'].dropna().unique()[:10])
print('PERM soc_code dtype:', perm['soc_code'].dtype)
print('PERM soc_code length mode:')
print(perm['soc_code'].dropna().str.len().value_counts().head())

oews_dir = Path('artifacts/tables/fact_oews')
ofiles = sorted(oews_dir.rglob('*.parquet'))[:2]
oews = pd.concat([pd.read_parquet(of) for of in ofiles])
print()
print('OEWS soc_code samples:', oews['soc_code'].dropna().unique()[:10])
print('OEWS soc_code dtype:', oews['soc_code'].dtype)
print('OEWS soc_code length mode:')
print(oews['soc_code'].dropna().str.len().value_counts().head())

print()
print('PERM area_code samples:', perm['area_code'].dropna().unique()[:10])
print('OEWS area_code samples:', oews['area_code'].dropna().unique()[:10])

# How many PERM soc_codes match OEWS?
perm_socs = set(perm['soc_code'].dropna().unique())
oews_socs = set(oews['soc_code'].dropna().unique())
overlap = perm_socs & oews_socs
print(f'\nPERM unique SOC codes: {len(perm_socs)}')
print(f'OEWS unique SOC codes: {len(oews_socs)}')
print(f'Intersection: {len(overlap)}')
if len(overlap) < 10:
    print('Sample overlap:', list(overlap)[:10])
# Try trimming PERM soc to 7 chars (XX-XXXX)
perm_socs_trim = set(s[:7] for s in perm_socs if len(s) >= 7)
overlap2 = perm_socs_trim & oews_socs
print(f'\nTrimmed PERM SOC (7 chars) intersection: {len(overlap2)}')
# Try zero-padded
oews_socs_detail = set(s for s in oews_socs if '.' not in s)
print(f'OEWS SOC w/o dots: {len(oews_socs_detail)} (sample: {list(oews_socs_detail)[:5]})')
