"""Salary query: meaningful top/bottom with filing thresholds."""
import pandas as pd

df = pd.read_parquet('artifacts/tables/employer_salary_profiles.parquet')

# Load SOC titles
soc_df = pd.read_parquet('artifacts/tables/dim_soc.parquet')
soc_titles = soc_df.set_index('soc_code')['soc_title'].to_dict()

def soc_name(code):
    t = soc_titles.get(code)
    if t: return t
    t = soc_titles.get(str(code)[:7])
    return t or code

# ========== H-1B FY2024: HIGHEST MEDIAN SALARY (≥10 filings) ==========
h1b_2024 = df[(df['visa_type'] == 'H-1B') & (df['fiscal_year'] == 2024) & (df['n_filings'] >= 10)]
top = h1b_2024.nlargest(5, 'median_salary')
print('='*80)
print('HIGHEST MEDIAN SALARY — H-1B, FY2024, ≥10 filings')
print('='*80)
for i, (_, r) in enumerate(top.iterrows(), 1):
    print(f'  #{i}  ${r["median_salary"]:>10,.0f}  {r["employer_name"]}')
    print(f'       Position: {r["job_title_top"]}  ({soc_name(r["soc_code"])})')
    print(f'       SOC: {r["soc_code"]}  |  Filings: {r["n_filings"]}  |  State: {r["worksite_state_top"]}')
    if pd.notna(r.get('oews_national_median')) and r['oews_national_median'] > 0:
        prem = (r['median_salary'] / r['oews_national_median'] - 1) * 100
        print(f'       vs OEWS national median: {prem:+.0f}%')
    print()

# ========== H-1B FY2023: LOWEST AVERAGE SALARY (≥5 filings) ==========
h1b_2023 = df[(df['visa_type'] == 'H-1B') & (df['fiscal_year'] == 2023) & (df['n_filings'] >= 5)]
bot = h1b_2023.nsmallest(5, 'mean_salary')
print('='*80)
print('LOWEST AVERAGE SALARY — H-1B, FY2023, ≥5 filings')
print('='*80)
for i, (_, r) in enumerate(bot.iterrows(), 1):
    print(f'  #{i}  ${r["mean_salary"]:>10,.0f}  {r["employer_name"]}')
    print(f'       Position: {r["job_title_top"]}  ({soc_name(r["soc_code"])})')
    print(f'       SOC: {r["soc_code"]}  |  Filings: {r["n_filings"]}  |  State: {r["worksite_state_top"]}')
    if pd.notna(r.get('oews_national_median')) and r['oews_national_median'] > 0:
        prem = (r['mean_salary'] / r['oews_national_median'] - 1) * 100
        print(f'       vs OEWS national median: {prem:+.0f}%')
    print()
