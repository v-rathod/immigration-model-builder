"""Employer-level feature engineering for EFS.

Reads curated fact_perm, dim_employer, dim_soc, fact_oews, dim_area.
Produces artifacts/tables/employer_features.parquet.
"""

import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


# ── Wage annualisation multipliers ──────────────────────────
WAGE_UNIT_MULTIPLIER = {
    'Year': 1.0,
    'year': 1.0,
    'yr': 1.0,
    'Hour': 2080.0,
    'hour': 2080.0,
    'hr': 2080.0,
    'Week': 52.0,
    'week': 52.0,
    'wk': 52.0,
    'Month': 12.0,
    'month': 12.0,
    'mo': 12.0,
    'Bi-Weekly': 26.0,
    'bi-weekly': 26.0,
    'bi': 26.0,
}


def _read_partitioned(table_dir: Path) -> pd.DataFrame:
    """Read a partitioned parquet directory (Hive-style) into a single DF."""
    pfiles = sorted(table_dir.rglob('*.parquet'))
    if not pfiles:
        return pd.DataFrame()
    dfs = []
    for pf in pfiles:
        pdf = pd.read_parquet(pf)
        # Restore partition columns from directory names
        for part in pf.parts:
            if '=' in part:
                col, val = part.split('=', 1)
                if col not in pdf.columns:
                    pdf[col] = val
        dfs.append(pdf)
    return pd.concat(dfs, ignore_index=True)


def _annualise_wage(wage: float, unit: str) -> Optional[float]:
    """Convert wage to annual equivalent."""
    if pd.isna(wage) or pd.isna(unit):
        return None
    mult = WAGE_UNIT_MULTIPLIER.get(str(unit).strip(), None)
    if mult is None:
        return None
    return float(wage) * mult


def build_employer_features(in_tables: Path, out_path: Path) -> None:
    """Compute employer and employer×SOC features.

    Parameters
    ----------
    in_tables : Path
        Directory containing curated parquet tables.
    out_path : Path
        Output path for employer_features.parquet.
    """
    log_lines = []
    metrics_dir = out_path.parent.parent / 'metrics'
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = metrics_dir / 'employer_features.log'

    def log(msg):
        print(msg)
        log_lines.append(msg)

    log('=' * 70)
    log('EMPLOYER FEATURES BUILD')
    log('=' * 70)

    # ── Load tables ─────────────────────────────────────────
    log('\n[A] Loading curated tables')
    perm_dir = in_tables / 'fact_perm'
    if perm_dir.is_dir():
        perm = _read_partitioned(perm_dir)
    else:
        perm = pd.read_parquet(in_tables / 'fact_perm.parquet')
    log(f'  fact_perm: {len(perm):,} rows')

    emp = pd.read_parquet(in_tables / 'dim_employer.parquet')
    log(f'  dim_employer: {len(emp):,} rows')

    oews_dir = in_tables / 'fact_oews'
    if oews_dir.is_dir():
        oews = _read_partitioned(oews_dir)
    else:
        oews = pd.read_parquet(in_tables / 'fact_oews.parquet')
    log(f'  fact_oews: {len(oews):,} rows')

    area = pd.read_parquet(in_tables / 'dim_area.parquet')
    log(f'  dim_area: {len(area):,} rows')

    # ── Prepare PERM data ───────────────────────────────────
    log('\n[B] Preparing PERM data')
    perm['decision_date'] = pd.to_datetime(perm['decision_date'], errors='coerce')
    perm = perm.dropna(subset=['decision_date', 'employer_id'])

    anchor = perm['decision_date'].max()
    anchor_month = anchor.to_period('M')
    log(f'  Anchor date: {anchor.date()} (max decision_date)')

    start_36m = anchor - pd.DateOffset(months=36)
    perm_36 = perm[perm['decision_date'] > start_36m].copy()
    log(f'  Rows in last 36 months: {len(perm_36):,}')

    # Decision month for window bucketing
    perm_36['dec_month'] = perm_36['decision_date'].dt.to_period('M')
    months_back = (anchor_month - perm_36['dec_month']).apply(lambda x: x.n)
    perm_36['in_12m'] = months_back < 12
    perm_36['in_24m'] = months_back < 24
    # 36m = all rows already

    # Normalise SOC codes to 7-char (XX-XXXX) for OEWS matching
    if 'soc_code' in perm_36.columns:
        perm_36['soc_code_7'] = perm_36['soc_code'].astype(str).str[:7]
    else:
        perm_36['soc_code_7'] = None
    log(f'  SOC code normalised: {perm_36["soc_code"].str.len().mode().values} → 7 chars for OEWS match')

    # Status flags
    perm_36['is_certified'] = perm_36['case_status'].str.lower().str.startswith('certified')
    perm_36['is_denied'] = perm_36['case_status'].str.lower().str.contains('denied')
    # audit_flag column may be all 'N'; fall back to case_status for audit detection
    if 'audit_flag' in perm_36.columns:
        perm_36['is_audit'] = perm_36['audit_flag'].str.upper().eq('Y')
    else:
        perm_36['is_audit'] = False

    # Annualise wage — vectorised (much faster than apply row-by-row)
    perm_36['wage_offer_from_num'] = pd.to_numeric(perm_36['wage_offer_from'], errors='coerce')
    perm_36['wage_mult'] = perm_36['wage_offer_unit'].map(WAGE_UNIT_MULTIPLIER).fillna(1.0)
    perm_36['annual_wage'] = perm_36['wage_offer_from_num'] * perm_36['wage_mult']

    # ── Build OEWS lookup ───────────────────────────────────
    log('\n[C] Building OEWS wage lookup')
    # Use latest ref_year
    if 'ref_year' in oews.columns:
        oews['ref_year'] = pd.to_numeric(oews['ref_year'], errors='coerce')
        latest_yr = oews['ref_year'].max()
        oews_latest = oews[oews['ref_year'] == latest_yr].copy()
    else:
        oews_latest = oews.copy()
        latest_yr = 'unknown'
    log(f'  OEWS ref_year: {latest_yr}, rows: {len(oews_latest):,}')

    # SOC×area lookup
    oews_area = oews_latest[['soc_code', 'area_code', 'a_median', 'a_pct75']].copy()
    oews_area = oews_area.dropna(subset=['a_median'])

    # National fallback: area_code = '99' (from dim_area NATIONAL)
    nat_code = '99'
    nat_rows = area[area['area_type'] == 'NATIONAL']
    if len(nat_rows) > 0:
        nat_code = str(nat_rows.iloc[0]['area_code'])
    oews_national = oews_area[oews_area['area_code'] == nat_code][['soc_code', 'a_median', 'a_pct75']].copy()
    oews_national = oews_national.rename(columns={'a_median': 'nat_a_median', 'a_pct75': 'nat_a_pct75'})
    log(f'  OEWS area-level rows: {len(oews_area):,}, national SOC rows: {len(oews_national):,}')

    # Cross-SOC national fallback: overall national median when SOC is not in OEWS
    overall_national_median = float(oews_national['nat_a_median'].median()) if len(oews_national) else 65000.0
    log(f'  Cross-SOC national fallback median: ${overall_national_median:,.0f}')

    # ── Compute features per employer and employer×SOC ──────
    log('\n[D] Computing features')

    windows_info = json.dumps({
        'anchor_date': str(anchor.date()),
        'last_36m_start': str(start_36m.date()),
    })
    refreshed_at = datetime.now(timezone.utc)

    def _compute_slice(grp: pd.DataFrame, employer_id: str, employer_name: str,
                       scope: str, soc_code=None) -> dict:
        """Compute feature vector for one employer (or employer×SOC) slice."""
        n_12m = int(grp['in_12m'].sum())
        n_24m = int(grp['in_24m'].sum())
        n_36m = len(grp)

        g24 = grp[grp['in_24m']]
        g12 = grp[grp['in_12m']]
        months_back = (anchor_month - grp['dec_month']).apply(lambda x: x.n)
        g_prior_12 = grp[(months_back >= 12) & (months_back < 24)]

        # Rates
        def _rate(sub, col):
            if len(sub) == 0:
                return None
            return float(sub[col].sum()) / len(sub)

        approval_rate_12m = _rate(g12, 'is_certified')
        approval_rate_24m = _rate(g24, 'is_certified')
        approval_rate_36m = _rate(grp, 'is_certified')
        denial_rate_12m = _rate(g12, 'is_denied')
        denial_rate_24m = _rate(g24, 'is_denied')
        denial_rate_36m = _rate(grp, 'is_denied')
        audit_rate_12m = _rate(g12, 'is_audit')
        audit_rate_24m = _rate(g24, 'is_audit')
        audit_rate_36m = _rate(grp, 'is_audit')

        # Months active
        months_active_24m = int(g24['dec_month'].nunique()) if n_24m > 0 else 0

        # Breadth (24m)
        soc_breadth_24m = int(g24['soc_code'].nunique()) if n_24m > 0 else 0
        site_breadth_24m = int(g24['worksite_state'].nunique()) if n_24m > 0 else 0

        # Trend: last 12m minus prior 12m
        ar_last12 = approval_rate_12m
        ar_prior12 = _rate(g_prior_12, 'is_certified')
        if ar_last12 is not None and ar_prior12 is not None:
            approval_rate_trend_12v12 = ar_last12 - ar_prior12
        else:
            approval_rate_trend_12v12 = None

        # Outcome volatility: std dev of monthly approval rate over 24m
        outcome_volatility = None
        if n_24m >= 6:
            monthly = g24.groupby('dec_month')['is_certified'].mean()
            if len(monthly) >= 3:
                outcome_volatility = float(monthly.std())

        # Wage ratios: prefer 24m window for recency; fall back to 36m for employers
        # with no 24m filings (employers who filed only in the older 12-36m bucket)
        wage_ratio_med = None
        wage_ratio_p75 = None
        w24 = g24.dropna(subset=['annual_wage'])
        w36 = grp.dropna(subset=['annual_wage'])
        w_use = w24 if len(w24) >= 1 else w36   # 36m fallback for sparse employers
        if len(w_use) >= 1:
            offered_median = w_use['annual_wage'].median()
            # Find OEWS match: prefer area-level, fallback national, fallback cross-SOC
            grp_ref = w24 if len(w24) >= 1 else grp   # use same window for SOC/area detection
            soc_for_match = soc_code if soc_code else (
                grp_ref['soc_code_7'].mode().iloc[0] if len(grp_ref['soc_code_7'].mode()) > 0 else None
            )
            # Normalise to 7-char for OEWS matching
            if soc_for_match and len(str(soc_for_match)) > 7:
                soc_for_match = str(soc_for_match)[:7]
            if soc_for_match:
                # Try area-level first (most common non-empty area in slice)
                area_vals = grp_ref['area_code'].dropna()
                area_vals = area_vals[area_vals.astype(str).str.strip().ne('')]
                area_for_match = area_vals.mode().iloc[0] if len(area_vals.mode()) > 0 else None

                oews_match = None
                if area_for_match is not None:
                    match = oews_area[
                        (oews_area['soc_code'] == soc_for_match) &
                        (oews_area['area_code'] == area_for_match)
                    ]
                    if len(match) > 0:
                        oews_match = match.iloc[0]

                # Fallback to national SOC-specific
                if oews_match is None:
                    nat_match = oews_national[oews_national['soc_code'] == soc_for_match]
                    if len(nat_match) > 0:
                        oews_match_row = nat_match.iloc[0]
                        oews_match = pd.Series({
                            'a_median': oews_match_row['nat_a_median'],
                            'a_pct75': oews_match_row['nat_a_pct75'],
                        })

                # Last resort: cross-SOC national fallback (all occupations median)
                if oews_match is None:
                    oews_match = pd.Series({'a_median': overall_national_median, 'a_pct75': None})

            else:
                # No SOC at all — use overall national median
                oews_match = pd.Series({'a_median': overall_national_median, 'a_pct75': None})

            if oews_match is not None:
                oews_med = oews_match.get('a_median')
                oews_p75 = oews_match.get('a_pct75')
                if oews_med and oews_med > 0:
                    wage_ratio_med = min(1.3, offered_median / oews_med)
                if oews_p75 and oews_p75 > 0:
                    wage_ratio_p75 = min(1.3, offered_median / oews_p75)

        return {
            'employer_id': employer_id,
            'employer_name': employer_name,
            'scope': scope,
            'soc_code': soc_code,
            'n_12m': n_12m,
            'n_24m': n_24m,
            'n_36m': n_36m,
            'months_active_24m': months_active_24m,
            'soc_breadth_24m': soc_breadth_24m,
            'site_breadth_24m': site_breadth_24m,
            'approval_rate_12m': approval_rate_12m,
            'approval_rate_24m': approval_rate_24m,
            'approval_rate_36m': approval_rate_36m,
            'denial_rate_12m': denial_rate_12m,
            'denial_rate_24m': denial_rate_24m,
            'denial_rate_36m': denial_rate_36m,
            'audit_rate_12m': audit_rate_12m,
            'audit_rate_24m': audit_rate_24m,
            'audit_rate_36m': audit_rate_36m,
            'approval_rate_trend_12v12': approval_rate_trend_12v12,
            'outcome_volatility': outcome_volatility,
            'wage_ratio_med': wage_ratio_med,
            'wage_ratio_p75': wage_ratio_p75,
            'windows_used': windows_info,
            'last_refreshed_at': refreshed_at,
        }

    # Join employer names
    emp_names = emp.set_index('employer_id')['employer_name'].to_dict()

    results = []
    employer_groups = perm_36.groupby('employer_id')
    total_employers = len(employer_groups)
    warn_count = 0

    for i, (eid, egrp) in enumerate(employer_groups):
        ename = emp_names.get(eid, eid[:12])

        # Overall slice
        row = _compute_slice(egrp, eid, ename, 'overall')
        results.append(row)

        # Employer×SOC slices where 24m count ≥ 10
        g24 = egrp[egrp['in_24m']]
        soc_counts = g24.groupby('soc_code').size()
        soc_eligible = soc_counts[soc_counts >= 10].index
        for soc in soc_eligible:
            soc_grp = egrp[egrp['soc_code'] == soc]
            soc_row = _compute_slice(soc_grp, eid, ename, 'SOC', soc_code=soc)
            results.append(soc_row)

        if (i + 1) % 5000 == 0:
            log(f'  Processed {i+1:,}/{total_employers:,} employers')

    log(f'  Total feature rows: {len(results):,} ({total_employers:,} employers)')

    # ── Write output ────────────────────────────────────────
    log('\n[E] Writing employer_features.parquet')
    df_out = pd.DataFrame(results)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(out_path, index=False, engine='pyarrow')
    log(f'  Written: {out_path} ({len(df_out):,} rows)')

    # Coverage stats
    overall = df_out[df_out['scope'] == 'overall']
    soc_slices = df_out[df_out['scope'] == 'SOC']
    wage_null_pct = round(overall['wage_ratio_med'].isna().mean() * 100, 1)
    log(f'\n  Overall rows: {len(overall):,}')
    log(f'  SOC-specific rows: {len(soc_slices):,}')
    log(f'  wage_ratio_med null: {wage_null_pct}%')
    if wage_null_pct > 50:
        log(f'  WARN: High wage_ratio null rate ({wage_null_pct}%) — OEWS coverage may be limited')
        warn_count += 1
    log(f'  Warnings: {warn_count}')

    with open(log_path, 'w') as f:
        f.write('\n'.join(log_lines))
    log(f'  Log: {log_path}')
    log('\n✓ EMPLOYER FEATURES COMPLETE')
