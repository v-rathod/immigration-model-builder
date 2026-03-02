"""Check data availability for complex free-form queries."""
import pandas as pd

# 1) soc_salary_market — YoY growth data for SW devs
ssm = pd.read_parquet('artifacts/tables/soc_salary_market.parquet')
print('=== soc_salary_market ===')
print(f'Rows: {len(ssm):,}  Cols: {list(ssm.columns)}')
sw = ssm[(ssm['soc_code']=='15-1252') & (ssm['visa_type']=='H-1B')]
print(f'\nSW Devs (15-1252) H-1B rows: {len(sw)}')
print(sw.sort_values('fiscal_year').to_string(index=False))

# Calculate YoY growth
sw_sorted = sw.sort_values('fiscal_year').copy()
sw_sorted['prev_median'] = sw_sorted['market_median'].shift(1)
sw_sorted['yoy_pct'] = ((sw_sorted['market_median'] - sw_sorted['prev_median']) / sw_sorted['prev_median'] * 100).round(1)
print('\nYoY median salary growth for SW Devs (15-1252):')
for _, r in sw_sorted.iterrows():
    yoy = f"{r['yoy_pct']:+.1f}%" if pd.notna(r['yoy_pct']) else 'N/A'
    print(f"  FY{r['fiscal_year']}: ${r['market_median']:,.0f}  ({yoy})")

# 2) employer_salary_profiles — per-employer × SOC breakdown
print('\n=== employer_salary_profiles ===')
esp = pd.read_parquet('artifacts/tables/employer_salary_profiles.parquet')
print(f'Rows: {len(esp):,}  Cols: {list(esp.columns)}')

# Check Google as example
google = esp[esp['employer_name'].str.contains('GOOGLE', case=False, na=False)]
print(f'\nGoogle rows: {len(google)}')
if len(google) > 0:
    top_socs = google.groupby('soc_code')['n_filings'].sum().nlargest(5)
    print('Top SOCs for Google:')
    dim_soc = pd.read_parquet('artifacts/tables/dim_soc.parquet')
    titles = dict(zip(dim_soc['soc_code'], dim_soc['soc_title']))
    for s, n in top_socs.items():
        med = google[google['soc_code']==s]['median_salary'].median()
        mkt_row = ssm[(ssm['soc_code']==s) & (ssm['visa_type']=='H-1B')]
        mkt = mkt_row['market_median'].median() if len(mkt_row) > 0 else 0
        title = titles.get(s, s)
        diff_pct = ((med - mkt) / mkt * 100) if mkt > 0 else 0
        print(f"  {s} ({title}): {n:,} filings")
        print(f"    employer median=${med:,.0f}, market median=${mkt:,.0f}, diff={diff_pct:+.1f}%")

# 3) Check how many employers have enough data for per-position breakdown
emp_soc = esp.groupby(['employer_name', 'soc_code'])['n_filings'].sum().reset_index()
emp_with_multi_soc = emp_soc.groupby('employer_name').size()
print(f'\nEmployers with 1 SOC: {(emp_with_multi_soc == 1).sum():,}')
print(f'Employers with 2-5 SOCs: {((emp_with_multi_soc >= 2) & (emp_with_multi_soc <= 5)).sum():,}')
print(f'Employers with 6+ SOCs: {(emp_with_multi_soc >= 6).sum():,}')

# 4) Top employers by SOC diversity (most positions filed)
top_diverse = emp_with_multi_soc.nlargest(10)
print(f'\nTop 10 employers by # of SOC codes filed:')
for emp, n in top_diverse.items():
    total = esp[esp['employer_name']==emp]['n_filings'].sum()
    print(f'  {emp}: {n} SOC codes, {total:,} total filings')

# 5) Salary yearly trends
esy = pd.read_parquet('artifacts/tables/employer_salary_yearly.parquet')
print(f'\n=== employer_salary_yearly ===')
print(f'Rows: {len(esy):,}  Cols: {list(esy.columns)}')
