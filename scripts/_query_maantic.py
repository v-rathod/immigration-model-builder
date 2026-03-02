"""Query all Maantic Inc filings across LCA and PERM, past 10 years."""
import pandas as pd

# --- Search LCA (H-1B) ---
print('Loading fact_lca...')
lca = pd.read_parquet('artifacts/tables/fact_lca')
lca_m = lca[lca['employer_name_raw'].str.upper().str.contains('MAANTIC', na=False)]
print(f'LCA (H-1B) matches: {len(lca_m):,}')

# --- Search PERM ---
print('Loading fact_perm...')
perm = pd.read_parquet('artifacts/tables/fact_perm')
perm_m = perm[perm['employer_name'].str.upper().str.contains('MAANTIC', na=False)]
print(f'PERM matches: {len(perm_m):,}')

# --- Combine into table ---
rows = []

for _, r in lca_m.iterrows():
    wage = pd.to_numeric(r.get('wage_rate_from'), errors='coerce')
    rows.append({
        'Type': 'H-1B (LCA)',
        'FY': r.get('fiscal_year', ''),
        'Case Number': r.get('case_number', ''),
        'Status': r.get('case_status', ''),
        'Job Title': r.get('job_title', ''),
        'SOC Code': r.get('soc_code', ''),
        'Wage': wage,
        'Wage Unit': r.get('wage_unit', ''),
        'Worksite State': r.get('worksite_state', ''),
        'Decision Date': str(r.get('decision_date', ''))[:10],
        'Employer Name': r.get('employer_name_raw', ''),
    })

for _, r in perm_m.iterrows():
    wage = pd.to_numeric(r.get('wage_offer_from'), errors='coerce')
    rows.append({
        'Type': 'PERM',
        'FY': r.get('fiscal_year', ''),
        'Case Number': r.get('case_number', ''),
        'Status': r.get('case_status', ''),
        'Job Title': r.get('job_title', ''),
        'SOC Code': r.get('soc_code', ''),
        'Wage': wage,
        'Wage Unit': r.get('wage_offer_unit', ''),
        'Worksite State': r.get('worksite_state', ''),
        'Decision Date': str(r.get('decision_date', ''))[:10],
        'Employer Name': r.get('employer_name', ''),
    })

result = pd.DataFrame(rows)

# Filter last 10 years
result['FY'] = pd.to_numeric(result['FY'], errors='coerce')
result = result[result['FY'] >= 2016].sort_values(['FY', 'Type', 'Decision Date'])

print(f'\nEmployer name variants found: {result["Employer Name"].unique()}')
print(f'Total filings (FY2016-2025): {len(result)}')
print()

# Summary by year and type
print('=== SUMMARY BY YEAR ===')
summary = result.groupby(['FY', 'Type']).size().unstack(fill_value=0)
summary['Total'] = summary.sum(axis=1)
print(summary.to_string())
print(f'\nGRAND TOTAL: {len(result)} filings')
print()

# Full table
print('=== ALL FILINGS (past 10 years) ===')
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 40)
cols = ['FY', 'Type', 'Case Number', 'Status', 'Job Title', 'SOC Code', 'Wage', 'Wage Unit', 'Worksite State', 'Decision Date']
print(result[cols].to_string(index=False))
