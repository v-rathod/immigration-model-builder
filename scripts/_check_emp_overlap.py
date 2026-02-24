import pandas as pd, pathlib
TABLES = pathlib.Path('artifacts/tables')
df_feat = pd.read_parquet(TABLES / 'employer_features.parquet')
df_emp  = pd.read_parquet(TABLES / 'dim_employer.parquet')
df_emm  = pd.read_parquet(TABLES / 'employer_monthly_metrics.parquet')

feat_uniq = set(df_feat['employer_id'].dropna().unique()) if 'employer_id' in df_feat.columns else set()
emp_uniq  = set(df_emp['employer_id'].dropna().unique())  if 'employer_id' in df_emp.columns  else set()
emm_uniq  = set(df_emm['employer_id'].dropna().unique())  if 'employer_id' in df_emm.columns  else set()

print(f'unique in employer_features: {len(feat_uniq):,}')
print(f'unique in dim_employer: {len(emp_uniq):,}')
print(f'unique in employer_monthly_metrics: {len(emm_uniq):,}')
print(f'feat & emp: {len(feat_uniq & emp_uniq):,} ({100*len(feat_uniq & emp_uniq)/len(feat_uniq):.1f}%)')
print(f'feat & emm: {len(feat_uniq & emm_uniq):,} ({100*len(feat_uniq & emm_uniq)/len(feat_uniq):.1f}%)')
print(f'feat - emp (not in dim): {len(feat_uniq - emp_uniq):,}')
