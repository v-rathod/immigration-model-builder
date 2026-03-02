"""Quick query: highest/lowest salary employers from salary profiles."""
import pandas as pd

df = pd.read_parquet('artifacts/tables/employer_salary_profiles.parquet')
print(f'Total rows: {len(df):,}')
print(f'Columns: {list(df.columns)}')
print(f'visa_type values: {df["visa_type"].unique()}')
print()

# --- Check if we have 'mean_annual_wage' or similar ---
wage_cols = [c for c in df.columns if 'wage' in c.lower() or 'salary' in c.lower()]
print(f'Wage columns: {wage_cols}')
print()

# H-1B filings in FY2024
h1b_2024 = df[(df['visa_type'] == 'H-1B') & (df['fiscal_year'] == 2024)]
print(f'H-1B FY2024 rows: {len(h1b_2024):,}')

# Highest median salary
top = h1b_2024.nlargest(10, 'median_salary')
print()
print('=== TOP 10 HIGHEST MEDIAN SALARY (H-1B, FY2024) ===')
for _, r in top.iterrows():
    soc = r.get('soc_code', 'N/A')
    name = r.get('employer_name', r.get('employer_id', '?'))
    n = r.get('n_filings', 0)
    med = r['median_salary']
    jt = r.get('job_title_top', '')
    print(f'  ${med:>12,.0f}  {name}  |  SOC: {soc}  |  filings: {n}  |  role: {jt}')

# --- FY2023 lowest ---
h1b_2023 = df[(df['visa_type'] == 'H-1B') & (df['fiscal_year'] == 2023)]
print(f'\nH-1B FY2023 rows: {len(h1b_2023):,}')

# Need at least 5 filings to avoid noise
h1b_2023_min = h1b_2023[h1b_2023['n_filings'] >= 5].copy()
print(f'H-1B FY2023 with >=5 filings: {len(h1b_2023_min):,}')

mean_col = 'mean_salary'

bot = h1b_2023_min.nsmallest(10, mean_col)
print()
print(f'=== BOTTOM 10 LOWEST AVG SALARY (H-1B, FY2023, >=5 filings) ===')
print(f'  (using column: {mean_col})')
for _, r in bot.iterrows():
    soc = r.get('soc_code', 'N/A')
    name = r.get('employer_name', r.get('employer_id', '?'))
    n = r.get('n_filings', 0)
    val = r[mean_col]
    jt = r.get('job_title_top', '')
    print(f'  ${val:>12,.0f}  {name}  |  SOC: {soc}  |  filings: {n}  |  role: {jt}')

# Also show the SOC title from dim_soc if available
print()
print('--- SOC Code Lookup ---')
try:
    soc_df = pd.read_parquet('artifacts/tables/dim_soc.parquet')
    # Get SOC codes from top and bottom
    codes = set(top['soc_code'].tolist() + bot['soc_code'].tolist())
    for code in sorted(codes):
        match = soc_df[soc_df['soc_code'] == code]
        if len(match) > 0:
            title = match.iloc[0].get('soc_title', 'N/A')
            print(f'  {code}: {title}')
        else:
            # Try 7-char match
            match7 = soc_df[soc_df['soc_code'].str[:7] == str(code)[:7]]
            if len(match7) > 0:
                title = match7.iloc[0].get('soc_title', 'N/A')
                print(f'  {code}: {title}')
            else:
                print(f'  {code}: (not in dim_soc)')
except Exception as e:
    print(f'  Could not load dim_soc: {e}')
