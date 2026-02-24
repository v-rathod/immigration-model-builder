#!/usr/bin/env python3
"""Simulate P3 query: EB2-India, PD = June 2016 — when does PD become current?"""
import pandas as pd

# Load artifacts
pf = pd.read_parquet("artifacts/tables/pd_forecasts.parquet")
fc = pd.read_parquet("artifacts/tables/fact_cutoffs")
be = pd.read_parquet("artifacts/tables/backlog_estimates.parquet")

USER_PD = pd.Timestamp("2016-06-01")
CATEGORY = "EB2"
COUNTRY = "IND"

print(f"=== User Profile ===")
print(f"Priority Date: {USER_PD.strftime('%B %d, %Y')}")
print(f"Category: {CATEGORY}")
print(f"Country: {COUNTRY} (India)")
print()

# --- 1. Current cutoff date (latest FAD) ---
fad = fc[(fc["category"] == CATEGORY) & (fc["country"] == COUNTRY) & (fc["chart"] == "FAD")]
fad = fad.copy()
fad["cutoff_date"] = pd.to_datetime(fad["cutoff_date"], errors="coerce")
fad = fad.dropna(subset=["cutoff_date"])
fad = fad.sort_values(["bulletin_year", "bulletin_month"])
latest = fad.iloc[-1] if len(fad) > 0 else None

if latest is not None:
    current_cutoff = latest["cutoff_date"]
    print(f"=== Current Visa Bulletin (FAD) ===")
    print(f"Bulletin: {int(latest['bulletin_year'])}-{int(latest['bulletin_month']):02d}")
    print(f"Current EB2-India cutoff: {current_cutoff.strftime('%B %d, %Y')}")
    gap_days = (USER_PD - current_cutoff).days
    if gap_days > 0:
        print(f"Your PD is {gap_days} days ({gap_days/30.44:.1f} months) AHEAD of the current cutoff")
        print(f"Status: NOT YET CURRENT")
    else:
        print(f"Your PD is {abs(gap_days)} days BEHIND the cutoff")
        print(f"Status: *** ALREADY CURRENT ***")
else:
    print("Could not determine current cutoff")
    current_cutoff = None

print()

# --- 2. Forecast: when does cutoff reach user's PD? ---
print(f"=== Priority Date Forecast ===")
forecasts = pf[(pf["category"] == CATEGORY) & (pf["country"] == COUNTRY) & (pf["chart"] == "FAD")]
forecasts = forecasts.copy()
forecasts["projected_cutoff_date"] = pd.to_datetime(forecasts["projected_cutoff_date"], errors="coerce")
forecasts = forecasts.dropna(subset=["projected_cutoff_date"]).sort_values("forecast_month")

if len(forecasts) > 0:
    print(f"Forecast period: {forecasts['forecast_month'].min()} to {forecasts['forecast_month'].max()}")
    print(f"Velocity: {forecasts['velocity_days_per_month'].mean():.1f} days/month average")
    print()

    # Find the month when projected cutoff >= user's PD
    becomes_current = forecasts[forecasts["projected_cutoff_date"] >= USER_PD]
    if len(becomes_current) > 0:
        first_current = becomes_current.iloc[0]
        print(f"*** PROJECTED CURRENT DATE: {first_current['forecast_month']} ***")
        print(f"  Projected cutoff at that time: {first_current['projected_cutoff_date'].strftime('%B %d, %Y')}")
        if "confidence_low" in first_current.index:
            cl = pd.to_datetime(first_current["confidence_low"], errors="coerce")
            ch = pd.to_datetime(first_current["confidence_high"], errors="coerce")
            if pd.notna(cl) and pd.notna(ch):
                print(f"  90% CI: {cl.strftime('%B %Y')} to {ch.strftime('%B %Y')}")
    else:
        last = forecasts.iloc[-1]
        remaining_days = (USER_PD - last["projected_cutoff_date"]).days
        velocity = forecasts["velocity_days_per_month"].mean()
        if velocity > 0:
            extra_months = remaining_days / velocity
            from dateutil.relativedelta import relativedelta
            estimated = pd.Timestamp(last["forecast_month"] + "-01") + relativedelta(months=int(extra_months))
            print(f"  Cutoff not reached within forecast window.")
            print(f"  Last forecasted cutoff ({last['forecast_month']}): {last['projected_cutoff_date'].strftime('%B %d, %Y')}")
            print(f"  Remaining gap: {remaining_days} days ({remaining_days/30.44:.1f} months)")
            print(f"  At avg velocity of {velocity:.1f} days/month, ~{extra_months:.0f} more months needed")
            print(f"  *** ESTIMATED CURRENT: ~{estimated.strftime('%B %Y')} ***")
        else:
            print(f"  Cannot estimate — zero or negative velocity")

    print()
    print("Forecast trajectory (FAD cutoff progression):")
    show = forecasts[["forecast_month", "projected_cutoff_date", "velocity_days_per_month", "cumulative_advancement_days"]]
    # Show every 3rd month
    for i in range(0, len(show), 3):
        row = show.iloc[i]
        marker = " <-- CURRENT" if pd.to_datetime(row["projected_cutoff_date"]) >= USER_PD else ""
        print(f"  {row['forecast_month']}  cutoff={pd.to_datetime(row['projected_cutoff_date']).strftime('%Y-%m-%d')}  vel={row['velocity_days_per_month']:.0f} d/mo{marker}")
else:
    print("No EB2-India FAD forecasts found")

print()

# --- 3. Backlog estimate ---
print(f"=== Backlog Context ===")
eb2_be = be[(be["category"] == CATEGORY) & (be["country"] == COUNTRY)]
eb2_be = eb2_be.sort_values(["bulletin_year", "bulletin_month"])
if len(eb2_be) > 0:
    latest_be = eb2_be.iloc[-1]
    print(f"Latest backlog estimate ({int(latest_be['bulletin_year'])}-{int(latest_be['bulletin_month']):02d}):")
    print(f"  Backlog months to clear: {latest_be['backlog_months_to_clear_est']:.1f}")
    print(f"  12-month inflow estimate: {latest_be['inflow_estimate_12m']:.0f}")
    print(f"  12-month avg advancement: {latest_be['advancement_days_12m_avg']:.1f} days")
