#!/usr/bin/env python3
"""Check Optum Services EFS and compare with peers."""
import pandas as pd

efs = pd.read_parquet("artifacts/tables/employer_friendliness_scores.parquet")

# Optum entities (overall scope)
optum = efs[
    (efs["employer_name"].str.contains("OPTUM", case=False, na=False))
    & (efs["scope"] == "overall")
]
print("=== ALL OPTUM ENTITIES (overall scope) ===")
cols = [
    "employer_name", "efs", "efs_tier", "n_24m",
    "approval_rate_24m", "wage_ratio_med",
    "outcome_subscore", "wage_subscore", "sustainability_subscore",
]
print(optum[cols].sort_values("n_24m", ascending=False).to_string(index=False))

# Compare with peers
print("\n=== COMPARISON WITH PEER COMPANIES ===")
peers = [
    "OPTUM SERVICES", "UNITEDHEALTH", "GOOGLE", "MICROSOFT", "META PLATFORMS",
    "AMAZON", "DELOITTE", "COGNIZANT", "INFOSYS", "TATA CONSULTANCY",
    "WIPRO", "ACCENTURE", "GOLDMAN SACHS", "APPLE", "INTEL",
    "CIGNA", "ELEVANCE", "CVS HEALTH", "WALMART", "JPMORGAN",
]
rows = []
for p in peers:
    match = efs[
        (efs["employer_name"].str.contains(p, case=False, na=False))
        & (efs["scope"] == "overall")
        & (efs["efs"].notna())
    ]
    if len(match) > 0:
        best = match.sort_values("n_24m", ascending=False).iloc[0]
        rows.append({
            "Company": best["employer_name"],
            "EFS": best["efs"],
            "Tier": best["efs_tier"],
            "Cases_24m": int(best["n_24m"]),
            "Approval%": round(best["approval_rate_24m"] * 100, 1),
            "Wage_Ratio": round(best["wage_ratio_med"], 2),
        })

df = pd.DataFrame(rows).sort_values("EFS", ascending=False)
print(df.to_string(index=False))

# Percentile rank
rated = efs[(efs["scope"] == "overall") & (efs["efs"].notna())]
optum_efs = 81.0
pct = (rated["efs"] < optum_efs).sum() / len(rated) * 100
print(f"\nOptum Services EFS={optum_efs} -> Percentile: {pct:.1f}th (out of {len(rated):,} rated employers)")

# Tier distribution
print("\n=== TIER DISTRIBUTION (all rated employers) ===")
print(rated["efs_tier"].value_counts().to_string())
