#!/usr/bin/env python3
"""Check whether prediction model was rebuilt with corrected data."""
import pandas as pd
import os
from datetime import datetime

print("=== Artifact Timestamps ===")
for f in ['pd_forecasts.parquet', 'employer_friendliness_scores.parquet',
          'fact_cutoff_trends.parquet', 'fact_cutoffs_all.parquet']:
    path = f'artifacts/tables/{f}'
    if os.path.exists(path):
        mtime = os.path.getmtime(path)
        ts = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
        df = pd.read_parquet(path)
        print(f"  {f}: {len(df):,} rows, modified {ts}")

# Check pd_forecasts structure
print("\n=== pd_forecasts ===")
fc = pd.read_parquet('artifacts/tables/pd_forecasts.parquet')
print(f"Columns: {list(fc.columns)}")
print(f"Shape: {fc.shape}")

# EB2 India forecasts
eb2 = fc[(fc['category'] == 'EB2') & (fc['country'] == 'IND')]
print(f"\nEB2 India forecasts: {len(eb2)} rows")
if len(eb2) > 0:
    print(eb2[['category','country','forecast_month','projected_cutoff_date','velocity_days_per_month']].tail(10).to_string(index=False))

# Check model JSON
import json
model_path = 'artifacts/models/pd_forecast_model.json'
if os.path.exists(model_path):
    mtime = os.path.getmtime(model_path)
    ts = datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')
    with open(model_path) as f:
        model = json.load(f)
    print(f"\n=== pd_forecast_model.json (modified {ts}) ===")
    if isinstance(model, dict):
        # Check EB2 India velocity
        for key in model:
            if 'EB2' in str(key) and 'IND' in str(key):
                print(f"  {key}: {model[key]}")
        # If it's a list or nested
        if 'models' in model:
            for m in model['models']:
                if m.get('category') == 'EB2' and m.get('country') == 'IND':
                    print(f"  EB2/IND: velocity={m.get('velocity_days_per_month')}, trained_on={m.get('trained_on_rows')} rows")
    elif isinstance(model, list):
        for m in model:
            if isinstance(m, dict) and m.get('category') == 'EB2' and m.get('country') == 'IND':
                print(f"  EB2/IND: {m}")
