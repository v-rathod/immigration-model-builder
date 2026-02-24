#!/usr/bin/env python3
import pandas as pd

# Check salary_benchmarks
sb = pd.read_parquet('artifacts/tables/salary_benchmarks.parquet')
print('salary_benchmarks:', len(sb), 'rows')
pct_cols = ['p10','p25','median','p75','p90']
avail = [c for c in pct_cols if c in sb.columns]
print('Available pct cols:', avail)
null_any = sb[avail].isnull().any(axis=1).mean()
print('null_any_pct:', f'{null_any:.1%}')
violations = 0
for lo, hi in zip(avail[:-1], avail[1:]):
    v = (sb[lo] > sb[hi]).sum()
    violations += v
    if v > 0:
        print(f'  {lo}>{hi} violations: {v}')
print('Total monotonicity violations:', violations)

# Check worksite_geo_metrics
wg = pd.read_parquet('artifacts/tables/worksite_geo_metrics.parquet')
print('\nworksite_geo_metrics:', len(wg), 'rows')
soc_cov = wg['soc_code'].notna().mean() if 'soc_code' in wg.columns else 'no col'
print('soc_code coverage:', soc_cov if isinstance(soc_cov, str) else f'{soc_cov:.1%}')
area_cov = wg['area_code'].notna().mean() if 'area_code' in wg.columns else 'no col'
print('area_code coverage:', area_cov if isinstance(area_cov, str) else f'{area_cov:.1%}')
