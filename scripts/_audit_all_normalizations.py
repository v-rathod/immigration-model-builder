"""Audit all P2 artifacts for normalization gaps in soc_code, country, visa category, employer_name."""
import pandas as pd
import os

TABLES = "artifacts/tables"

checks = {
    "soc_demand_metrics": ["soc_code"],
    "salary_benchmarks": ["soc_code"],
    "worksite_geo_metrics": ["soc_code"],
    "employer_features": ["employer_name", "soc_code"],
    "backlog_estimates": ["chargeability_country"],
    "queue_depth_estimates": ["chargeability_country", "category"],
    "pd_forecasts": ["chargeability_country", "category"],
    "category_movement_metrics": ["chargeability_country", "category"],
    "employer_monthly_metrics": ["employer_name"],
    "employer_friendliness_scores": ["employer_name"],
    "employer_risk_features": ["employer_name"],
    "fact_cutoffs_all": ["chargeability_country", "visa_class"],
    "dim_country": ["country_name", "iso_3"],
    "dim_soc": ["soc_code"],
    "processing_times_trends": ["form_type"],
    "visa_demand_metrics": ["chargeability_country", "visa_class"],
}

for artifact, cols in checks.items():
    path = f"{TABLES}/{artifact}.parquet"
    if not os.path.exists(path):
        print(f"  MISSING  {artifact}")
        continue
    df = pd.read_parquet(path)
    available = df.columns.tolist()
    use_cols = [c for c in cols if c in available]
    if not use_cols:
        print(f"  SKIP     {artifact}: wanted {cols}, has {available[:6]}")
        continue
    for col in use_cols:
        vals = df[col].dropna().unique()
        # Check for all-caps strings
        allcaps = [v for v in vals if isinstance(v, str) and v == v.upper() and len(v) > 3]
        # Check for inconsistent SOC code formats
        if col == "soc_code":
            dotted = [v for v in vals if isinstance(v, str) and "." in v]
            undash = [v for v in vals if isinstance(v, str) and "-" not in v and len(v) >= 6]
            issues = dotted[:5] + undash[:5]
        else:
            issues = allcaps[:8]
        n_unique = len(vals)
        if issues:
            print(f"  ISSUES   {artifact}.{col} ({n_unique} unique): {issues}")
        else:
            print(f"  OK       {artifact}.{col} ({n_unique} unique): {list(vals[:5])}")
