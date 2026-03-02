"""Print EB2 India FAD vs DFF table for all available months."""
import pandas as pd

df = pd.read_parquet("artifacts/tables/fact_cutoff_trends.parquet")

# Filter for EB2 India
mask = (df["category"] == "EB2") & (df["country"] == "IND")
eb2 = df[mask][["bulletin_year", "bulletin_month", "chart", "status_flag", "cutoff_date"]].copy()

# Separate FAD and DFF
fad = eb2[eb2["chart"] == "FAD"][["bulletin_year", "bulletin_month", "status_flag", "cutoff_date"]].rename(
    columns={"status_flag": "fad_flag", "cutoff_date": "fad_date"})
dff = eb2[eb2["chart"] == "DFF"][["bulletin_year", "bulletin_month", "status_flag", "cutoff_date"]].rename(
    columns={"status_flag": "dff_flag", "cutoff_date": "dff_date"})

merged = fad.merge(dff, on=["bulletin_year", "bulletin_month"], how="outer")
merged = merged.sort_values(["bulletin_year", "bulletin_month"]).reset_index(drop=True)

# Format month/year
month_names = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
               7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
merged["Month_Year"] = merged["bulletin_month"].map(month_names) + " " + merged["bulletin_year"].astype(str)


def fmt(row, date_col, flag_col):
    flag = row.get(flag_col)
    if pd.notna(flag) and flag == "C":
        return "Current"
    if pd.notna(flag) and flag == "U":
        return "Unavailable"
    dt = row[date_col]
    if pd.notna(dt):
        return dt.strftime("%d-%b-%Y")
    return ""


merged["FAD"] = merged.apply(lambda r: fmt(r, "fad_date", "fad_flag"), axis=1)
merged["DFF"] = merged.apply(lambda r: fmt(r, "dff_date", "dff_flag"), axis=1)

# Print
print("EB2 India — FAD vs DFF (all available bulletin months)")
print(f"{'Month/Year':>12}  |  {'FAD':>14}  |  {'DFF':>14}")
print("-" * 52)
for _, r in merged.iterrows():
    print(f"{r['Month_Year']:>12}  |  {r['FAD']:>14}  |  {r['DFF']:>14}")
print(f"\nTotal rows: {len(merged)}")
print(f"FAD data range: {eb2[eb2['chart']=='FAD']['bulletin_year'].min()}-{eb2[eb2['chart']=='FAD']['bulletin_year'].max()}")
print(f"DFF data range: {eb2[eb2['chart']=='DFF']['bulletin_year'].min()}-{eb2[eb2['chart']=='DFF']['bulletin_year'].max()}")
