#!/usr/bin/env python3
"""For EB2 India PD June 2014, find when FAD and DFF reach that date."""
import pandas as pd

target_pd = pd.Timestamp("2014-06-01")

fc = pd.read_parquet("artifacts/tables/pd_forecasts.parquet")
eb2 = fc[(fc["category"] == "EB2") & (fc["country"] == "IND")].copy()
eb2 = eb2.sort_values(["chart", "forecast_month"])

print(f"Target Priority Date: {target_pd.strftime('%Y-%m-%d')} (June 2014)")
print(f"Current date: Feb 2026")
print()

for chart in ["FAD", "DFF"]:
    sub = eb2[eb2["chart"] == chart].copy()
    if len(sub) == 0:
        print(f"{chart}: No forecast data")
        continue

    sub["projected_cutoff_date"] = pd.to_datetime(sub["projected_cutoff_date"])
    
    # Current cutoff (month 0 or earliest forecast)
    current = sub[sub["months_ahead"] == 0]
    if len(current):
        cur_date = current.iloc[0]["projected_cutoff_date"]
        print(f"{chart} current cutoff: {cur_date.strftime('%Y-%m-%d')}")
    
    # Find first month where projected cutoff >= target PD
    reached = sub[sub["projected_cutoff_date"] >= target_pd]
    if len(reached) > 0:
        first = reached.iloc[0]
        print(f"{chart} reaches June 2014: {first['forecast_month']} "
              f"(projected cutoff: {first['projected_cutoff_date'].strftime('%Y-%m-%d')}, "
              f"velocity: {first['velocity_days_per_month']:.1f} days/month)")
    else:
        last = sub.iloc[-1]
        gap = (target_pd - last["projected_cutoff_date"]).days
        avg_vel = sub["velocity_days_per_month"].mean()
        est_months = gap / avg_vel if avg_vel > 0 else float("inf")
        last_month = pd.Timestamp(last["forecast_month"] + "-01")
        est_date = last_month + pd.DateOffset(months=int(est_months))
        print(f"{chart} does NOT reach June 2014 within forecast window")
        print(f"  Last forecast: {last['forecast_month']}, cutoff={last['projected_cutoff_date'].strftime('%Y-%m-%d')}")
        print(f"  Remaining gap: {gap} days")
        print(f"  Avg velocity: {avg_vel:.1f} days/month → estimated ~{est_months:.0f} more months → ~{est_date.strftime('%b %Y')}")
    print()

# Show full forecast trajectory
print("=== Full FAD Forecast Trajectory ===")
fad = eb2[eb2["chart"] == "FAD"][["forecast_month","projected_cutoff_date","velocity_days_per_month","confidence_low","confidence_high"]].copy()
fad["projected_cutoff_date"] = pd.to_datetime(fad["projected_cutoff_date"]).dt.strftime("%Y-%m-%d")
fad["confidence_low"] = pd.to_datetime(fad["confidence_low"]).dt.strftime("%Y-%m-%d")
fad["confidence_high"] = pd.to_datetime(fad["confidence_high"]).dt.strftime("%Y-%m-%d")
print(fad.to_string(index=False))

print("\n=== Full DFF Forecast Trajectory ===")
dff = eb2[eb2["chart"] == "DFF"][["forecast_month","projected_cutoff_date","velocity_days_per_month","confidence_low","confidence_high"]].copy()
dff["projected_cutoff_date"] = pd.to_datetime(dff["projected_cutoff_date"]).dt.strftime("%Y-%m-%d")
dff["confidence_low"] = pd.to_datetime(dff["confidence_low"]).dt.strftime("%Y-%m-%d")
dff["confidence_high"] = pd.to_datetime(dff["confidence_high"]).dt.strftime("%Y-%m-%d")
print(dff.to_string(index=False))
