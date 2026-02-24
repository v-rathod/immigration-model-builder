#!/usr/bin/env python3
"""Quick state inspection."""
import pandas as pd, json
from pathlib import Path

# fact_perm
fp = Path('artifacts/tables/fact_perm')
parts = []
for p in sorted(fp.glob('fiscal_year=*/part-0.parquet')):
    fy = int(p.parent.name.split('=')[1])
    df = pd.read_parquet(p)
    parts.append((fy, len(df), list(df.columns)))
total = sum(r for _,r,_ in parts)
print(f'fact_perm: {total:,} rows, {len(parts)} partitions')
print(f'  Columns: {parts[0][2]}')

# PK check
all_cn = pd.concat([pd.read_parquet(p, columns=['case_number']) for p in fp.glob('fiscal_year=*/part-0.parquet')])
pk_dupes = all_cn['case_number'].duplicated().sum()
print(f'  PK dupes (case_number): {pk_dupes}')

# Check schema compliance
schema_cols = ['case_number','case_status','received_date','decision_date','employer_id',
               'soc_code','area_code','employer_country','job_title','wage_offer_from',
               'wage_offer_to','wage_offer_unit','worksite_city','worksite_state',
               'worksite_postal','is_fulltime','fiscal_year','source_file','ingested_at']
actual = set(parts[0][2])
missing_from_schema = [c for c in schema_cols if c not in actual]
extra = [c for c in actual if c not in schema_cols]
print(f'  Missing from schema: {missing_from_schema}')
print(f'  Extra columns: {extra}')

# fact_cutoffs
fc = Path('artifacts/tables/fact_cutoffs')
fc_dfs = []
for p in sorted(fc.glob('bulletin_year=*/bulletin_month=*/part-0.parquet')):
    by = int(p.parent.parent.name.split('=')[1])
    bm = int(p.parent.name.split('=')[1])
    df = pd.read_parquet(p)
    df['bulletin_year'] = by
    df['bulletin_month'] = bm
    fc_dfs.append(df)
fc_all = pd.concat(fc_dfs, ignore_index=True)
pk_cols = ['bulletin_year','bulletin_month','chart','category','country']
fc_pk = fc_all.duplicated(subset=pk_cols).sum()
print(f'\nfact_cutoffs: {len(fc_all):,} rows, {len(fc_dfs)} partitions')
print(f'  Columns: {list(fc_all.columns[:8])}')
print(f'  PK dupes: {fc_pk}')
print(f'  Years: {sorted(fc_all["bulletin_year"].unique())}')

# dims
for name in ['dim_soc','dim_country','dim_area','dim_visa_class','dim_employer']:
    df = pd.read_parquet(f'artifacts/tables/{name}.parquet')
    print(f'\n{name}: {len(df)} rows, cols={list(df.columns)}')

# Audit results
print('\n--- Audit Results ---')
with open('artifacts/metrics/input_coverage_report.json') as f:
    ic = json.load(f)
for ds in ['PERM','OEWS','Visa_Bulletin','LCA']:
    d = ic.get(ds, {})
    pct = d.get('coverage_pct', 0)
    miss = d.get('missing', [])
    print(f'{ds}: {pct*100:.1f}% ({d.get("processed",0)}/{d.get("expected",0)}) missing={len(miss)}')

with open('artifacts/metrics/output_audit_report.json') as f:
    oa = json.load(f)
for tbl, d in oa.items():
    print(f'{tbl}: rows={d.get("rows")}, pk={d.get("pk_unique")}, missing={d.get("required_missing",[])}')
