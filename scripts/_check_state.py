#!/usr/bin/env python3
"""Quick state check for migration planning."""
import pandas as pd
import os

data_root = "/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads"

# Check codebook
df = pd.read_csv(f'{data_root}/Codebooks/country_codes_iso.csv')
print('country_codes cols:', list(df.columns))
print('country_codes rows:', len(df))
print(df.head(2).to_string())
print('---')

# Check crosswalk
df2 = pd.read_csv(f'{data_root}/Codebooks/soc_crosswalk_2010_to_2018.csv')
print('crosswalk cols:', list(df2.columns))
print('crosswalk rows:', len(df2))
print(df2.head(2).to_string())
print('---')

# Check current fact_perm partition
fp_dir = 'artifacts/tables/fact_perm'
parts = sorted([d for d in os.listdir(fp_dir) if d.startswith('fiscal_year')])
print('fact_perm partitions:', parts)
print('HIVE DEFAULT?:', '__HIVE_DEFAULT_PARTITION__' in os.listdir(fp_dir))

# Check one partition  
p1 = os.path.join(fp_dir, parts[0])
files = os.listdir(p1)
df3 = pd.read_parquet(os.path.join(p1, files[0]))
print(f'fact_perm cols ({parts[0]}):', list(df3.columns))
print(f'fact_perm rows ({parts[0]}):', len(df3))
print('---')

# Check existing fact_perm.parquet single file
fp_single = 'artifacts/tables/fact_perm_single_file_backup.parquet'
if os.path.exists(fp_single):
    print('fact_perm single file backup exists:', fp_single)

# Check fact_cutoffs
fc_dir = 'artifacts/tables/fact_cutoffs'
fc_parts = sorted([d for d in os.listdir(fc_dir) if d.startswith('bulletin')])
print('fact_cutoffs year dirs:', fc_parts)

# Check current dim_soc
ds = pd.read_parquet('artifacts/tables/dim_soc.parquet')
print('dim_soc rows:', len(ds), 'cols:', list(ds.columns))
print('---')

# Check dim_country
dc = pd.read_parquet('artifacts/tables/dim_country.parquet')
print('dim_country rows:', len(dc), 'cols:', list(dc.columns))
print('---')

# Check OEWS files
oews_dir = f'{data_root}/BLS_OEWS'
for d in sorted(os.listdir(oews_dir)):
    dp = os.path.join(oews_dir, d)
    if os.path.isdir(dp):
        files = os.listdir(dp)
        print(f'OEWS {d}:', files)
