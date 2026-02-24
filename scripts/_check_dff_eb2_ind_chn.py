#!/usr/bin/env python3
"""When will DFF become current for June 2016 PD — EB2 India and EB2 China?"""
import pandas as pd
from datetime import timedelta

USER_PD = pd.Timestamp("2016-06-01")

# Load artifacts
pf = pd.read_parquet("artifacts/tables/pd_forecasts.parquet")
fc = pd.read_parquet("artifacts/tables/fact_cutoffs")

for country, label in [("IND", "India"), ("CHN", "China")]:
    print(f"{'='*60}")
    print(f"EB2 {label} — DFF — PD: June 2016")
    print(f"{'='*60}")

    # Current DFF cutoff
    dff_hist = fc[(fc["category"] == "EB2") & (fc["country"] == country) & (fc["chart"] == "DFF")]
    dff_hist = dff_hist.copy()
    dff_hist["cutoff_date"] = pd.to_datetime(dff_hist["cutoff_date"], errors="coerce")
    dff_hist = dff_hist.dropna(subset=["cutoff_date"]).sort_values(["bulletin_year", "bulletin_month"])
    
    if len(dff_hist) > 0:
        latest = dff_hist.iloc[-1]
        current_cutoff = latest["cutoff_date"]
        gap_days = (USER_PD - current_cutoff).days
        print(f"Current DFF cutoff (Mar 2026): {current_cutoff.strftime('%B %d, %Y')}")
        print(f"Gap to your PD: {gap_days} days ({gap_days/30.44:.1f} months)")
        print()

    # Forecast
    forecasts = pf[(pf["category"] == "EB2") & (pf["country"] == country) & (pf["chart"] == "DFF")]
    forecasts = forecasts.copy()
    forecasts["projected_cutoff_date"] = pd.to_datetime(forecasts["projected_cutoff_date"], errors="coerce")
    forecasts = forecasts.dropna(subset=["projected_cutoff_date"]).sort_values("forecast_month")

    if len(forecasts) > 0:
        avg_vel = forecasts["velocity_days_per_month"].mean()
        print(f"Forecast: {forecasts['forecast_month'].min()} to {forecasts['forecast_month'].max()}")
        print(f"Avg velocity: {avg_vel:.1f} days/month")
        print()

        # Find when cutoff >= user PD
        becomes_current = forecasts[forecasts["projected_cutoff_date"] >= USER_PD]
        if len(becomes_current) > 0:
            first = becomes_current.iloc[0]
            print(f"*** DFF BECOMES CURRENT: {first['forecast_month']} ***")
            print(f"  Projected cutoff: {first['projected_cutoff_date'].strftime('%B %d, %Y')}")
            cl = pd.to_datetime(first.get("confidence_low"), errors="coerce")
            ch = pd.to_datetime(first.get("confidence_high"), errors="coerce")
            if pd.notna(cl) and pd.notna(ch):
                print(f"  90% CI: {cl.strftime('%B %Y')} — {ch.strftime('%B %Y')}")
        else:
            last = forecasts.iloc[-1]
            remaining = (USER_PD - last["projected_cutoff_date"]).days
            if avg_vel > 0:
                extra_months = remaining / avg_vel
                from dateutil.relativedelta import relativedelta
                estimated = pd.Timestamp(last["forecast_month"] + "-01") + relativedelta(months=int(extra_months))
                print(f"  Not reached within forecast window.")
                print(f"  Last forecast ({last['forecast_month']}): {last['projected_cutoff_date'].strftime('%B %d, %Y')}")
                print(f"  Remaining gap: {remaining} days ({remaining/30.44:.1f} months)")
                print(f"  *** ESTIMATED DFF CURRENT: ~{estimated.strftime('%B %Y')} ***")
            else:
                print(f"  Cannot estimate — zero velocity")

        print()
        print("DFF trajectory (every 3 months):")
        for i in range(0, len(forecasts), 3):
            row = forecasts.iloc[i]
            pcd = pd.to_datetime(row["projected_cutoff_date"])
            marker = " <-- YOUR PD CURRENT" if pcd >= USER_PD else ""
            print(f"  {row['forecast_month']}  cutoff={pcd.strftime('%Y-%m-%d')}  vel={row['velocity_days_per_month']:.0f} d/mo{marker}")
    else:
        print("  No DFF forecasts available")

    print()
