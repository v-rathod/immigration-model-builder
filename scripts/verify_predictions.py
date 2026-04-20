import pandas as pd
from datetime import datetime
import math
from dateutil.relativedelta import relativedelta

pd.set_option('display.width', 200)
df = pd.read_parquet('artifacts/tables/pd_forecasts_retrograde.parquet')
df_base = pd.read_parquet('artifacts/tables/pd_forecasts.parquet')

USER_PD = '2016-06-15'
pd_time = datetime.strptime(USER_PD, '%Y-%m-%d')

def compute_prediction(series_df, label):
    series = series_df.sort_values('months_ahead')
    velocities = series['velocity_days_per_month'].tolist()
    avg_vel = sum(velocities) / len(velocities)
    
    # Check if PD found in 24-month window
    for _, row in series.iterrows():
        cutoff = pd.to_datetime(row['projected_cutoff_date'])
        if cutoff.to_pydatetime().replace(tzinfo=None) >= pd_time:
            print(f'{label}: FOUND in {int(row["months_ahead"])}mo -> {row["forecast_month"]}')
            return
    
    # Extrapolate
    last_row = series.iloc[-1]
    last_cutoff = pd.to_datetime(last_row['projected_cutoff_date']).to_pydatetime().replace(tzinfo=None)
    
    if avg_vel <= 0:
        print(f'{label}: UNABLE TO ESTIMATE (avg_vel={avg_vel:.1f})')
        return
    
    remaining_days = (pd_time - last_cutoff).days
    if remaining_days <= 0:
        print(f'{label}: Already current at end of window!')
        return
    
    additional_months = math.ceil(remaining_days / avg_vel)
    total_months = 24 + additional_months
    
    # Calculate estimated month
    end_month = datetime.strptime(last_row['forecast_month'] + '-01', '%Y-%m-%d')
    estimate = end_month + relativedelta(months=additional_months)
    
    print(f'{label}: EXTRAPOLATED -> {estimate.strftime("%b %Y")} '
          f'(in {total_months}mo total, avg_vel={avg_vel:.1f}d/mo, '
          f'last_cutoff={last_cutoff.strftime("%Y-%m-%d")})')

for chart in ['DFF', 'FAD']:
    mcra = df[(df['chart'] == chart) & (df['category'] == 'EB2') & (df['country'] == 'IND')]
    base = df_base[(df_base['chart'] == chart) & (df_base['category'] == 'EB2') & (df_base['country'] == 'IND')]
    compute_prediction(mcra, f'MCRA {chart} EB2/IND')
    compute_prediction(base, f'BASE {chart} EB2/IND')
    print()

# Double check: start of series
print("=== MCRA FAD start/end cutoffs ===")
fad = df[(df['chart'] == 'FAD') & (df['category'] == 'EB2') & (df['country'] == 'IND')].sort_values('months_ahead')
print("Month 1:", fad.iloc[0]['forecast_month'], "->", pd.to_datetime(fad.iloc[0]['projected_cutoff_date']).strftime('%Y-%m-%d'))
print("Month 24:", fad.iloc[-1]['forecast_month'], "->", pd.to_datetime(fad.iloc[-1]['projected_cutoff_date']).strftime('%Y-%m-%d'))
print("Net advancement:", (pd.to_datetime(fad.iloc[-1]['projected_cutoff_date']) - pd.to_datetime(fad.iloc[0]['projected_cutoff_date'])).days, "days")
print("Avg velocity:", round(fad['velocity_days_per_month'].mean(), 1), "d/mo")
print("All positive?", (fad['velocity_days_per_month'] > 0).all())
