"""Deep audit of normalization gaps across all P2 artifacts."""
import pandas as pd

TABLES = "artifacts/tables"

print("=== Employer name quality in non-salary artifacts ===")
for name in ["employer_features", "employer_friendliness_scores", "employer_monthly_metrics"]:
    df = pd.read_parquet(f"{TABLES}/{name}.parquet", columns=["employer_name"])
    vals = df["employer_name"].dropna()
    numeric = vals[vals.str.match(r"^\d+$", na=False)]
    allcaps_long = vals[(vals.str.len() > 5) & (vals == vals.str.upper()) & ~vals.str.match(r"^\d+$", na=False)]
    print(f"\n{name}:")
    print(f"  total rows: {len(vals)}, unique: {vals.nunique()}")
    print(f"  numeric-only names: {len(numeric)} examples: {numeric.unique()[:5].tolist()}")
    print(f"  all-caps long names: {len(allcaps_long)} examples: {allcaps_long.unique()[:5].tolist()}")

print("\n=== dim_employer sanity ===")
dim = pd.read_parquet(f"{TABLES}/dim_employer.parquet", columns=["employer_id", "employer_name"])
numeric_dim = dim[dim["employer_name"].str.match(r"^\d+$", na=False)]
allcaps_dim = dim[(dim["employer_name"].str.len() > 5) & (dim["employer_name"] == dim["employer_name"].str.upper()) & ~dim["employer_name"].str.match(r"^\d+$", na=False)]
print(f"  total: {len(dim)}, numeric names: {len(numeric_dim)}, all-caps long: {len(allcaps_dim)}")
print(f"  all-caps examples: {allcaps_dim['employer_name'].unique()[:8].tolist()}")

print("\n=== Country values in feature/model tables ===")
for name, col in [
    ("backlog_estimates", "country"),
    ("queue_depth_estimates", "chargeability_country"),
    ("pd_forecasts", "chargeability_country"),
    ("category_movement_metrics", "chargeability_country"),
    ("visa_demand_metrics", "country"),
    ("fact_cutoffs_all", "country"),
]:
    import os
    path = f"{TABLES}/{name}.parquet"
    if not os.path.exists(path):
        print(f"  {name}: NOT FOUND")
        continue
    df = pd.read_parquet(path)
    if col not in df.columns:
        actual_country_cols = [c for c in df.columns if "country" in c.lower()]
        print(f"  {name}: no column '{col}', has {actual_country_cols}")
        if actual_country_cols:
            col = actual_country_cols[0]
    if col in df.columns:
        vals = df[col].dropna().unique().tolist()
        print(f"  {name}.{col} ({len(vals)} unique): {vals[:10]}")

print("\n=== SOC code format check in employer_features ===")
ef = pd.read_parquet(f"{TABLES}/employer_features.parquet", columns=["soc_code"])
vals = ef["soc_code"].dropna().unique()
dotted = [v for v in vals if "." in str(v)]
no_dash = [v for v in vals if "-" not in str(v) and len(str(v)) >= 6]
print(f"  total unique SOC codes: {len(vals)}")
print(f"  with dot: {dotted[:5]}, without dash: {no_dash[:5]}")
print(f"  sample: {list(vals[:8])}")
