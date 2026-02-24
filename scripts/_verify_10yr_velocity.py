#!/usr/bin/env python3
"""
Cross-verify PD forecast model velocity against actual 10-year historical data.

For each EB category × country × chart (FAD/DFF):
  - Find cutoff date ~10 years ago (2016) and today (2026)
  - Compute actual total advancement and actual avg days/month
  - Compare with model's base_velocity_days
  - Also compute 5-year, 3-year, and 1-year velocities
"""

import json
import pandas as pd
import numpy as np
from pathlib import Path

ROOT = Path("artifacts")
TABLES = ROOT / "tables"
MODELS = ROOT / "models"

# ── Load data ────────────────────────────────────────────────────────────
trends = pd.read_parquet(TABLES / "fact_cutoff_trends.parquet")
trends["bulletin_date"] = pd.to_datetime(
    trends["bulletin_year"].astype(str) + "-" + trends["bulletin_month"].astype(str).str.zfill(2) + "-01"
)
trends["cutoff_date"] = pd.to_datetime(trends["cutoff_date"])
trends = trends.sort_values(["chart","category","country","bulletin_date"])

with open(MODELS / "pd_forecast_model.json") as f:
    model = json.load(f)
model_params = {(p["chart"], p["category"], p["country"]): p for p in model["series"]}

# Also load raw cutoffs for comparison
cutoffs = pd.read_parquet(TABLES / "fact_cutoffs_all.parquet")
cutoffs["cutoff_date"] = pd.to_datetime(cutoffs["cutoff_date"])
cutoffs["bulletin_date"] = pd.to_datetime(
    cutoffs["bulletin_year"].astype(str) + "-" + cutoffs["bulletin_month"].astype(str).str.zfill(2) + "-01"
)

print("=" * 80)
print("10-YEAR HISTORICAL VELOCITY CROSS-VERIFICATION")
print("=" * 80)

# ── Focus on key series ──────────────────────────────────────────────────
FOCUS = [
    ("FAD", "EB2", "IND"),
    ("FAD", "EB2", "CHN"),
    ("FAD", "EB3", "IND"),
    ("FAD", "EB3", "CHN"),
    ("FAD", "EB1", "IND"),
    ("DFF", "EB2", "IND"),
    ("DFF", "EB2", "CHN"),
    ("DFF", "EB3", "IND"),
]

for chart, cat, country in FOCUS:
    print(f"\n{'─' * 70}")
    print(f"  {chart} | {cat} | {country}")
    print(f"{'─' * 70}")
    
    # Get series from trends
    mask = (trends["chart"] == chart) & (trends["category"] == cat) & (trends["country"] == country)
    s = trends[mask].copy().sort_values("bulletin_date").drop_duplicates("bulletin_date", keep="last")
    
    if len(s) < 12:
        print(f"  Insufficient data: {len(s)} rows")
        continue
    
    latest = s.iloc[-1]
    latest_bdate = latest["bulletin_date"]
    latest_cutoff = latest["cutoff_date"]
    
    print(f"  Latest bulletin: {latest_bdate.strftime('%Y-%m')}  cutoff: {latest_cutoff.strftime('%Y-%m-%d')}")
    
    # Compute velocities for different windows
    windows = [
        ("10-year", 120),
        ("5-year", 60),
        ("3-year", 36),
        ("2-year", 24),
        ("1-year", 12),
    ]
    
    print(f"\n  {'Window':<12} {'Start Bul':>10} {'Start Cut':>12} {'End Cut':>12} {'Adv(days)':>10} {'Months':>7} {'d/mo':>8}")
    print(f"  {'─'*12} {'─'*10} {'─'*12} {'─'*12} {'─'*10} {'─'*7} {'─'*8}")
    
    for label, months in windows:
        target_date = latest_bdate - pd.DateOffset(months=months)
        # Find closest row to target date
        diffs = (s["bulletin_date"] - target_date).abs()
        if diffs.min() > pd.Timedelta(days=60):
            print(f"  {label:<12} {'N/A':>10} — no data near {target_date.strftime('%Y-%m')}")
            continue
        
        start_row = s.loc[diffs.idxmin()]
        start_bdate = start_row["bulletin_date"]
        start_cutoff = start_row["cutoff_date"]
        
        if pd.isna(start_cutoff) or pd.isna(latest_cutoff):
            print(f"  {label:<12} {'N/A':>10} — NaT cutoff")
            continue
        
        actual_months = (latest_bdate.year - start_bdate.year) * 12 + (latest_bdate.month - start_bdate.month)
        if actual_months <= 0:
            continue
        
        adv_days = (latest_cutoff - start_cutoff).days
        vel = adv_days / actual_months
        
        print(f"  {label:<12} {start_bdate.strftime('%Y-%m'):>10} {start_cutoff.strftime('%Y-%m-%d'):>12} "
              f"{latest_cutoff.strftime('%Y-%m-%d'):>12} {adv_days:>10,} {actual_months:>7} {vel:>8.1f}")
    
    # Get model velocity
    key = (chart, cat, country)
    if key in model_params:
        mp = model_params[key]
        print(f"\n  Model base_velocity: {mp['base_velocity_days']:.1f} d/mo")
        print(f"  Model rolling_12m:   {mp['rolling_12m_mean']:.1f} d/mo")
        print(f"  Model rolling_24m:   {mp['rolling_24m_mean']:.1f} d/mo")
        
        # Check if model velocity matches long-term
        # Long-term is the 10-year or longest available
        for label, months in windows:
            target_date = latest_bdate - pd.DateOffset(months=months)
            diffs = (s["bulletin_date"] - target_date).abs()
            if diffs.min() <= pd.Timedelta(days=60):
                start_row = s.loc[diffs.idxmin()]
                actual_months = (latest_bdate.year - start_row["bulletin_date"].year) * 12 + (latest_bdate.month - start_row["bulletin_date"].month)
                if actual_months > 0:
                    adv_days = (latest_cutoff - start_row["cutoff_date"]).days
                    longterm_vel = adv_days / actual_months
                    diff_pct = ((mp["base_velocity_days"] - longterm_vel) / longterm_vel * 100) if longterm_vel != 0 else float('inf')
                    print(f"  vs {label} actual: {longterm_vel:.1f} d/mo  (model is {diff_pct:+.0f}%)")
                    break
    else:
        print(f"\n  ⚠ No model params found for this series")

# ── Year-by-year breakdown for EB2-India FAD ──────────────────────────────
print(f"\n{'=' * 80}")
print("YEAR-BY-YEAR BREAKDOWN: EB2-India FAD")
print(f"{'=' * 80}")

mask = (trends["chart"] == "FAD") & (trends["category"] == "EB2") & (trends["country"] == "IND")
s = trends[mask].copy().sort_values("bulletin_date").drop_duplicates("bulletin_date", keep="last")

print(f"\n  {'FY':>6} {'Start Cut':>12} {'End Cut':>12} {'Adv(days)':>10} {'Months':>7} {'d/mo':>8} {'Notes'}")
print(f"  {'─'*6} {'─'*12} {'─'*12} {'─'*10} {'─'*7} {'─'*8} {'─'*20}")

for yr in range(2016, 2027):
    oct_start = pd.Timestamp(f"{yr-1}-10-01")
    sep_end = pd.Timestamp(f"{yr}-09-01") if yr < 2026 else s["bulletin_date"].max()
    
    diffs_start = (s["bulletin_date"] - oct_start).abs()
    diffs_end = (s["bulletin_date"] - sep_end).abs()
    
    if diffs_start.min() > pd.Timedelta(days=60) or diffs_end.min() > pd.Timedelta(days=60):
        print(f"  FY{yr:>4}  N/A — insufficient data")
        continue
    
    row_start = s.loc[diffs_start.idxmin()]
    row_end = s.loc[diffs_end.idxmin()]
    
    if pd.isna(row_start["cutoff_date"]) or pd.isna(row_end["cutoff_date"]):
        print(f"  FY{yr:>4}  N/A — NaT cutoff")
        continue
    
    months = (row_end["bulletin_date"].year - row_start["bulletin_date"].year) * 12 + \
             (row_end["bulletin_date"].month - row_start["bulletin_date"].month)
    if months <= 0:
        continue
    
    adv = (row_end["cutoff_date"] - row_start["cutoff_date"]).days
    vel = adv / months
    
    note = ""
    if adv < 0:
        note = "RETROGRESSION"
    elif vel > 50:
        note = "big jump"
    elif vel == 0:
        note = "stagnant"
    
    print(f"  FY{yr:>4} {row_start['cutoff_date'].strftime('%Y-%m-%d'):>12} "
          f"{row_end['cutoff_date'].strftime('%Y-%m-%d'):>12} {adv:>10,} {months:>7} {vel:>8.1f} {note}")

# ── Same for DFF ──────────────────────────────────────────────────────────
print(f"\n{'=' * 80}")
print("YEAR-BY-YEAR BREAKDOWN: EB2-India DFF")
print(f"{'=' * 80}")

mask = (trends["chart"] == "DFF") & (trends["category"] == "EB2") & (trends["country"] == "IND")
s = trends[mask].copy().sort_values("bulletin_date").drop_duplicates("bulletin_date", keep="last")

print(f"\n  {'FY':>6} {'Start Cut':>12} {'End Cut':>12} {'Adv(days)':>10} {'Months':>7} {'d/mo':>8} {'Notes'}")
print(f"  {'─'*6} {'─'*12} {'─'*12} {'─'*10} {'─'*7} {'─'*8} {'─'*20}")

for yr in range(2016, 2027):
    oct_start = pd.Timestamp(f"{yr-1}-10-01")
    sep_end = pd.Timestamp(f"{yr}-09-01") if yr < 2026 else s["bulletin_date"].max()
    
    diffs_start = (s["bulletin_date"] - oct_start).abs()
    diffs_end = (s["bulletin_date"] - sep_end).abs()
    
    if diffs_start.min() > pd.Timedelta(days=60) or diffs_end.min() > pd.Timedelta(days=60):
        print(f"  FY{yr:>4}  N/A — insufficient data")
        continue
    
    row_start = s.loc[diffs_start.idxmin()]
    row_end = s.loc[diffs_end.idxmin()]
    
    if pd.isna(row_start["cutoff_date"]) or pd.isna(row_end["cutoff_date"]):
        print(f"  FY{yr:>4}  N/A — NaT cutoff")
        continue
    
    months = (row_end["bulletin_date"].year - row_start["bulletin_date"].year) * 12 + \
             (row_end["bulletin_date"].month - row_start["bulletin_date"].month)
    if months <= 0:
        continue
    
    adv = (row_end["cutoff_date"] - row_start["cutoff_date"]).days
    vel = adv / months
    
    note = ""
    if adv < 0:
        note = "RETROGRESSION"
    elif vel > 50:
        note = "big jump"
    elif vel == 0:
        note = "stagnant"
    
    print(f"  FY{yr:>4} {row_start['cutoff_date'].strftime('%Y-%m-%d'):>12} "
          f"{row_end['cutoff_date'].strftime('%Y-%m-%d'):>12} {adv:>10,} {months:>7} {vel:>8.1f} {note}")

# ── Model confirmation ──────────────────────────────────────────────────
print(f"\n{'=' * 80}")
print("MODEL STRUCTURE CONFIRMATION")
print(f"{'=' * 80}")

# Confirm separate FAD and DFF models
fad_series = [k for k in model_params if k[0] == "FAD"]
dff_series = [k for k in model_params if k[0] == "DFF"]
print(f"\n  FAD model series: {len(fad_series)}")
print(f"  DFF model series: {len(dff_series)}")
print(f"  Total series: {len(model_params)}")
print(f"  ✓ Separate FAD and DFF prediction models: {'YES' if fad_series and dff_series else 'NO'}")

# List all EB2 model velocities
print(f"\n  All EB2 model velocities:")
for key in sorted(model_params.keys()):
    if key[1] == "EB2":
        p = model_params[key]
        print(f"    {key[0]:>4} {key[1]:>4} {key[2]:>5}  base_vel={p['base_velocity_days']:6.1f} d/mo  "
              f"r12m={p['rolling_12m_mean']:6.1f}  r24m={p['rolling_24m_mean']:6.1f}  "
              f"pos_pct={p['positive_month_pct']:.1%}")

print()
