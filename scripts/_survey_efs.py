#!/usr/bin/env python3
"""Quick data survey for EFS implementation."""
import pandas as pd
from pathlib import Path

# fact_perm
fp = Path('artifacts/tables/fact_perm')
pfs = list(fp.rglob('*.parquet'))
df = pd.concat([pd.read_parquet(p) for p in pfs[:3]], ignore_index=True)
print('=== fact_perm (sample 3 partitions) ===')
print('columns:', list(df.columns))
print('case_status:', df['case_status'].value_counts().head(8).to_dict())
print('wage_offer_unit:', df['wage_offer_unit'].value_counts().head(6).to_dict())
print('decision_date range:', df['decision_date'].min(), '-', df['decision_date'].max())
print('audit_flag:', df['audit_flag'].value_counts().to_dict() if 'audit_flag' in df.columns else 'N/A')
print()

# dim_employer
de = pd.read_parquet('artifacts/tables/dim_employer.parquet')
print('=== dim_employer ===')
print('columns:', list(de.columns))
print('rows:', len(de))
print()

# fact_oews
fo = Path('artifacts/tables/fact_oews')
pfs2 = list(fo.rglob('*.parquet'))
df2 = pd.read_parquet(pfs2[0])
print('=== fact_oews ===')
print('columns:', list(df2.columns))
print('rows:', len(df2))
print('a_median null%:', round(df2['a_median'].isna().mean()*100, 1) if 'a_median' in df2.columns else 'N/A')
print('a_pct75 null%:', round(df2['a_pct75'].isna().mean()*100, 1) if 'a_pct75' in df2.columns else 'N/A')
print()

# dim_area NATIONAL
da = pd.read_parquet('artifacts/tables/dim_area.parquet')
print('=== dim_area ===')
print('columns:', list(da.columns))
nat = da[da['area_type']=='NATIONAL']
print('NATIONAL:', nat[['area_code','area_title']].to_dict('records') if len(nat) > 0 else 'None')
print()

# io readers
print('=== io.readers ===')
import importlib
m = importlib.import_module('src.io.readers')
print(dir(m))
