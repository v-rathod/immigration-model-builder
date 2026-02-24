"""Check PERM case_status values."""
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TABLES = ROOT / "artifacts" / "tables"

# Read a sample of fact_perm to see actual case_status values
perm_dir = TABLES / "fact_perm"
files = sorted(perm_dir.rglob("*.parquet"))
print(f"Total files: {len(files)}")

# Sample from first few files
dfs = []
for f in files[:3]:
    df = pd.read_parquet(f)
    dfs.append(df)
sample = pd.concat(dfs)
print(f"Sample rows: {len(sample)}")
print(f"Columns: {list(sample.columns[:12])}")
print(f"case_status sample: {sample['case_status'].value_counts().head(10).to_dict()}")
print(f"decision_date sample: {sample['decision_date'].head(3).tolist()}")
print(f"employer_id sample: {sample['employer_id'].head(3).tolist()}")

# Check if employer_id is in dim_employer
dim_emp = pd.read_parquet(TABLES / "dim_employer.parquet")
print(f"\ndim_employer sample: {dim_emp['employer_id'].head(5).tolist()}")
print(f"fact_perm employer_id type: {sample['employer_id'].dtype}")
print(f"dim_employer employer_id type: {dim_emp['employer_id'].dtype}")
sample_ids = set(sample['employer_id'].dropna().astype(str).head(5))
dim_ids = set(dim_emp['employer_id'].astype(str).head(5))
print(f"sample perm ids: {sample_ids}")
print(f"sample dim ids: {dim_ids}")
