#!/usr/bin/env python3
"""Deep analysis of EB2-India historical movement patterns for model tuning."""
import pandas as pd
import numpy as np

fc = pd.read_parquet("artifacts/tables/fact_cutoffs")

for chart in ["FAD", "DFF"]:
    print(f"\n{'='*70}")
    print(f"EB2-India {chart} — Historical Monthly Advancement Analysis")
    print(f"{'='*70}")

    eb2 = fc[(fc["category"] == "EB2") & (fc["country"] == "IND") & (fc["chart"] == chart)]
    eb2 = eb2.copy()
    eb2["cutoff_date"] = pd.to_datetime(eb2["cutoff_date"], errors="coerce")
    eb2 = eb2[eb2["status_flag"] == "D"].dropna(subset=["cutoff_date"])
    eb2["bulletin_year"] = eb2["bulletin_year"].astype(int)
    eb2["bulletin_month"] = eb2["bulletin_month"].astype(int)
    eb2 = eb2.sort_values(["bulletin_year", "bulletin_month"])
    eb2 = eb2.drop_duplicates(subset=["bulletin_year", "bulletin_month"], keep="last")

    # Compute monthly advancement
    eb2["prev_cutoff"] = eb2["cutoff_date"].shift(1)
    eb2["advancement_days"] = (eb2["cutoff_date"] - eb2["prev_cutoff"]).dt.days
    eb2 = eb2.dropna(subset=["advancement_days"])

    print(f"\nTotal months with data: {len(eb2)}")
    print(f"Date range: {eb2.bulletin_year.iloc[0]}-{eb2.bulletin_month.iloc[0]:02d} to {eb2.bulletin_year.iloc[-1]}-{eb2.bulletin_month.iloc[-1]:02d}")
    print(f"Cutoff range: {eb2.cutoff_date.min().strftime('%Y-%m-%d')} to {eb2.cutoff_date.max().strftime('%Y-%m-%d')}")

    adv = eb2["advancement_days"]
    print(f"\n--- Overall Stats ---")
    print(f"Mean advancement: {adv.mean():.1f} days/month")
    print(f"Median advancement: {adv.median():.1f} days/month")
    print(f"Std: {adv.std():.1f}")
    print(f"Min: {adv.min():.0f}, Max: {adv.max():.0f}")
    print(f"Negative months (retrogression): {(adv < 0).sum()}")
    print(f"Zero months (no movement): {(adv == 0).sum()}")
    print(f"Months > 180 days: {(adv > 180).sum()}")

    # By calendar month
    print(f"\n--- Avg Advancement by Calendar Month ---")
    eb2["cal_month"] = eb2["bulletin_month"].astype(int)
    by_month = eb2.groupby("cal_month")["advancement_days"].agg(["mean", "median", "std", "count", "min", "max"])
    for m, row in by_month.iterrows():
        mo_name = pd.Timestamp(2020, m, 1).strftime("%b")
        print(f"  {mo_name:>3} (n={int(row['count']):2d}): mean={row['mean']:7.1f}d  median={row['median']:6.1f}d  std={row['std']:6.1f}  range=[{row['min']:.0f}, {row['max']:.0f}]")

    # By year
    print(f"\n--- Avg Advancement by Year ---")
    eb2["yr"] = eb2["bulletin_year"].astype(int)
    by_year = eb2.groupby("yr")["advancement_days"].agg(["mean", "median", "count"])
    for y, row in by_year.iterrows():
        print(f"  {y}: mean={row['mean']:7.1f}d  median={row['median']:6.1f}d  (n={int(row['count'])})")

    # Last 24 months detail
    print(f"\n--- Last 24 Months Detail ---")
    recent = eb2.tail(24)
    for _, r in recent.iterrows():
        adv_val = r["advancement_days"]
        bar = "+" * min(int(max(adv_val, 0) / 10), 30) if adv_val > 0 else "-" * min(int(abs(adv_val) / 10), 30) if adv_val < 0 else "."
        print(f"  {int(r.bulletin_year)}-{int(r.bulletin_month):02d}  cutoff={r.cutoff_date.strftime('%Y-%m-%d')}  adv={adv_val:+7.0f}d  {bar}")

    # Rolling 12-month average
    print(f"\n--- Rolling 12-month Avg Velocity ---")
    eb2["rolling_12m_avg"] = eb2["advancement_days"].rolling(12, min_periods=6).mean()
    for _, r in eb2.tail(12).iterrows():
        r12 = r.get("rolling_12m_avg", float("nan"))
        print(f"  {int(r.bulletin_year)}-{int(r.bulletin_month):02d}  12m_avg={r12:6.1f} d/mo")
