import pandas as pd, pathlib
fact_perm = pathlib.Path('artifacts/tables/fact_perm')
dfs = []
for pf in sorted(fact_perm.rglob('*.parquet')):
    try:
        df_p = pd.read_parquet(pf, columns=['employer_id', 'employer_name'])
    except Exception:
        df_p = pd.read_parquet(pf, columns=['employer_id'])
        df_p['employer_name'] = None
    dfs.append(df_p)
df = pd.concat(dfs, ignore_index=True)
uniq = df.dropna(subset=['employer_id']).groupby('employer_id')['employer_name'].agg(lambda x: x.dropna().mode().iloc[0] if len(x.dropna()) > 0 else None).reset_index()
print(f'unique employer_ids in fact_perm: {len(uniq):,}')
print(f'null employer_names: {uniq["employer_name"].isna().sum():,}')
