"""Salary query: highest/lowest salary employers, min 50 filings."""
import pandas as pd

df = pd.read_parquet('artifacts/tables/employer_salary_profiles.parquet')
soc_df = pd.read_parquet('artifacts/tables/dim_soc.parquet')
soc_titles = soc_df.set_index('soc_code')['soc_title'].to_dict()

def soc_name(code):
    return soc_titles.get(code) or soc_titles.get(str(code)[:7]) or code

MIN = 50

# ========== H-1B FY2024: HIGHEST MEDIAN SALARY (≥50 filings) ==========
h = df[(df['visa_type'] == 'H-1B') & (df['fiscal_year'] == 2024) & (df['n_filings'] >= MIN)]
top = h.nlargest(5, 'median_salary')
print('='*80)
print(f'HIGHEST MEDIAN SALARY — H-1B, FY2024, ≥{MIN} filings')
print('='*80)
for i, (_, r) in enumerate(top.iterrows(), 1):
    prem = ''
    if pd.notna(r.get('oews_national_median')) and r['oews_national_median'] > 0:
        prem = f'  |  vs OEWS: {(r["median_salary"]/r["oews_national_median"]-1)*100:+.0f}%'
    print(f'  #{i}  ${r["median_salary"]:>10,.0f}  {r["employer_name"]}')
    print(f'       {r["job_title_top"]}  ({soc_name(r["soc_code"])})')
    print(f'       SOC: {r["soc_code"]}  |  Filings: {r["n_filings"]}  |  State: {r["worksite_state_top"]}{prem}')
    print()

# ========== H-1B FY2023: LOWEST AVERAGE SALARY (≥50 filings) ==========
h2 = df[(df['visa_type'] == 'H-1B') & (df['fiscal_year'] == 2023) & (df['n_filings'] >= MIN)]
bot = h2.nsmallest(5, 'mean_salary')
print('='*80)
print(f'LOWEST AVERAGE SALARY — H-1B, FY2023, ≥{MIN} filings')
print('='*80)
for i, (_, r) in enumerate(bot.iterrows(), 1):
    prem = ''
    if pd.notna(r.get('oews_national_median')) and r['oews_national_median'] > 0:
        prem = f'  |  vs OEWS: {(r["mean_salary"]/r["oews_national_median"]-1)*100:+.0f}%'
    print(f'  #{i}  ${r["mean_salary"]:>10,.0f}  {r["employer_name"]}')
    print(f'       {r["job_title_top"]}  ({soc_name(r["soc_code"])})')
    print(f'       SOC: {r["soc_code"]}  |  Filings: {r["n_filings"]}  |  State: {r["worksite_state_top"]}{prem}')
    print()
