"""Verify the rebuilt fact_cutoffs data for EB2 India is correct."""
import pandas as pd

# Read from the partitioned fact_cutoffs
df = pd.read_parquet("artifacts/tables/fact_cutoffs")
print(f"Total rows in rebuilt fact_cutoffs: {len(df):,}")
print(f"Countries: {sorted(df['country'].unique())}")
print(f"Charts: {sorted(df['chart'].unique())}")
print()

# Filter EB2 India
mask = (df['category'] == 'EB2') & (df['country'] == 'IND')
eb2 = df[mask].sort_values(['bulletin_year', 'bulletin_month', 'chart'])

print(f"EB2 India rows: {len(eb2)}")
print()

# Show FAD values  
fad = eb2[eb2['chart'] == 'FAD'][['bulletin_year', 'bulletin_month', 'status_flag', 'cutoff_date']]
print("EB2 India FAD (should NOT be Current):")
for _, r in fad.iterrows():
    flag = r['status_flag']
    dt = r['cutoff_date']
    if flag == 'C':
        date_str = 'Current'
    elif flag == 'U':
        date_str = 'Unavailable'
    elif pd.notna(dt):
        date_str = dt.strftime('%d-%b-%Y')
    else:
        date_str = '(empty)'
    print(f"  {r['bulletin_month']:02d}/{r['bulletin_year']}  {date_str}")

# Also check some known values to verify correctness
print()
print("=== VERIFICATION CHECKS ===")
# Jan 2018: PDF shows EB2 India FAD = 22NOV08, DFF = 08FEB09  
row = eb2[(eb2['bulletin_year']==2018) & (eb2['bulletin_month']==1)]
for _, r in row.iterrows():
    chart = r['chart']
    flag = r['status_flag']
    dt = r['cutoff_date']
    date_str = dt.strftime('%d-%b-%Y') if pd.notna(dt) else flag
    print(f"Jan 2018 {chart}: {date_str}")

# Oct 2017: PDF shows EB2 India FAD = 15SEP08
row = eb2[(eb2['bulletin_year']==2017) & (eb2['bulletin_month']==10)]
for _, r in row.iterrows():
    chart = r['chart']
    flag = r['status_flag']
    dt = r['cutoff_date']
    date_str = dt.strftime('%d-%b-%Y') if pd.notna(dt) else flag
    print(f"Oct 2017 {chart}: {date_str}")

# Dec 2022: Should be around Jan-2011 range, NOT Nov-2022
row = eb2[(eb2['bulletin_year']==2022) & (eb2['bulletin_month']==12)]
for _, r in row.iterrows():
    chart = r['chart']
    flag = r['status_flag']
    dt = r['cutoff_date']
    date_str = dt.strftime('%d-%b-%Y') if pd.notna(dt) else flag
    print(f"Dec 2022 {chart}: {date_str}")

# Check if any EB2 India FAD is "Current"
current_count = len(eb2[(eb2['chart']=='FAD') & (eb2['status_flag']=='C')])
print(f"\nEB2 India FAD 'Current' count: {current_count} (should be 0)")
