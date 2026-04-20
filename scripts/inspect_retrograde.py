import pandas as pd
import json

df = pd.read_parquet('artifacts/tables/pd_forecasts_retrograde.parquet')

# Find EB2 India
eb2_india = df[(df['category'] == 'EB2') & (df['country'] == 'India')]
print('EB2 India rows count:', len(eb2_india))
print('Charts available:', list(eb2_india['chart'].unique()))

if len(eb2_india) > 0:
    fad = eb2_india[eb2_india['chart'] == 'FAD'].sort_values('months_ahead')
    dff = eb2_india[eb2_india['chart'] == 'DFF'].sort_values('months_ahead')
    
    print()
    print('FAD EB2 India:')
    if len(fad) > 0:
        pd.set_option('display.width', 200)
        print(fad[['forecast_month', 'months_ahead', 'projected_cutoff_date', 'velocity_days_per_month', 'retrograde_prob', 'risk_adjusted_velocity']].to_string())
    else:
        print('EMPTY - no FAD series for EB2 India in MCRA artifact')
    
    print()
    print('DFF EB2 India:')    
    if len(dff) > 0:
        print(dff[['forecast_month', 'months_ahead', 'projected_cutoff_date', 'velocity_days_per_month', 'retrograde_prob', 'risk_adjusted_velocity']].to_string())
    else:
        print('EMPTY - no DFF series for EB2 India in MCRA artifact')

print()
print('All India series in artifact:')
india = df[df['country'] == 'India'][['chart','category','country']].drop_duplicates()
print(india.to_string())

# Compare base model
df_base = pd.read_parquet('artifacts/tables/pd_forecasts.parquet')
fad_base = df_base[(df_base['chart'] == 'FAD') & (df_base['category'] == 'EB2') & (df_base['country'] == 'India')].sort_values('months_ahead')
print()
print('FAD EB2 India BASE model:')
print(fad_base[['forecast_month', 'months_ahead', 'projected_cutoff_date', 'velocity_days_per_month']].to_string())

# Also look at model JSON parameters
with open('artifacts/models/pd_forecast_retrograde_model.json') as f:
    model = json.load(f)

india_params = {k: v for k, v in model.items() if 'India' in k and 'EB2' in k}
for key, params in list(india_params.items())[:3]:
    print()
    print(f'--- {key} ---')
    print('base_velocity_days:', params.get('base_velocity_days'))
    print('retro_overall_prob:', params.get('retro_overall_prob'))
    print('retro_overall_severity_days:', params.get('retro_overall_severity_days'))
    print('retro_regime:', params.get('retro_regime'))
    print('rolling_12m_mean:', params.get('rolling_12m_mean'))
    print('rolling_24m_mean:', params.get('rolling_24m_mean'))
