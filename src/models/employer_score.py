"""Employer Friendliness Score (EFS) v1 — rules-based scoring.

Reads employer_features.parquet → produces employer_friendliness_scores.parquet.

Scoring Model:
  Outcome Sub-score   (50%): approval_rate_24m with Bayesian shrinkage
  Wage Sub-score      (30%): wage_ratio_med (cap 1.3) rescaled 0-100
  Sustainability      (20%): months_active, trend, volume, volatility blend

Eligibility guardrails:
  - n_24m < 3 → EFS = NULL (insufficient data)
  - All denials in 24m → EFS capped at 10
"""

import numpy as np
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path


# ── Constants ───────────────────────────────────────────────
WEIGHT_OUTCOME = 0.50
WEIGHT_WAGE = 0.30
WEIGHT_SUSTAINABILITY = 0.20

# Bayesian shrinkage prior: population approval rate / strength
SHRINKAGE_PRIOR = 0.88          # ~88% pop approval rate
SHRINKAGE_STRENGTH = 20         # pseudo-observations

# Eligibility
MIN_CASES_24M = 3
ALL_DENIED_CAP = 10.0


def _bayesian_rate(observed_rate, n, prior=SHRINKAGE_PRIOR, strength=SHRINKAGE_STRENGTH):
    """Shrink observed_rate toward prior for small samples."""
    if observed_rate is None or np.isnan(observed_rate):
        return prior
    return (observed_rate * n + prior * strength) / (n + strength)


def _outcome_subscore(row: pd.Series) -> float:
    """Outcome subscore: 0-100 based on shrunk approval rate."""
    ar = row.get('approval_rate_24m')
    n = row.get('n_24m', 0)
    shrunk = _bayesian_rate(ar, n)
    return round(shrunk * 100.0, 2)


def _wage_subscore(row: pd.Series) -> float:
    """Wage subscore: 0-100 mapped from wage_ratio_med ∈ [0.5, 1.3]."""
    ratio = row.get('wage_ratio_med')
    if ratio is None or np.isnan(ratio):
        return 50.0  # neutral if unknown
    ratio = max(0.5, min(1.3, ratio))
    # Linear map: 0.5 → 0, 1.0 → 75, 1.3 → 100
    if ratio <= 1.0:
        return round((ratio - 0.5) / 0.5 * 75.0, 2)
    else:
        return round(75.0 + (ratio - 1.0) / 0.3 * 25.0, 2)


def _sustainability_subscore(row: pd.Series) -> float:
    """Blend of months_active, trend, volume, volatility."""
    parts = []
    weights = []

    # Months active: 0-24 mapped to 0-100
    ma = row.get('months_active_24m', 0)
    parts.append(min(100.0, ma / 24.0 * 100.0))
    weights.append(0.30)

    # Volume: log-scaled n_24m (10 → ~50, 100 → ~90, 500+ → 100)
    n24 = max(row.get('n_24m', 0), 1)
    vol_score = min(100.0, np.log10(n24) / np.log10(500) * 100.0)
    parts.append(vol_score)
    weights.append(0.25)

    # Trend: approval_rate_trend_12v12 in [-1,1] mapped to [0,100]
    trend = row.get('approval_rate_trend_12v12')
    if trend is not None and not np.isnan(trend):
        trend_score = (trend + 1.0) / 2.0 * 100.0
    else:
        trend_score = 50.0
    parts.append(max(0.0, min(100.0, trend_score)))
    weights.append(0.25)

    # Low volatility is good: volatility 0 → 100, ≥0.3 → 0
    vol = row.get('outcome_volatility')
    if vol is not None and not np.isnan(vol):
        stab_score = max(0.0, (1.0 - vol / 0.3)) * 100.0
    else:
        stab_score = 50.0
    parts.append(max(0.0, min(100.0, stab_score)))
    weights.append(0.20)

    return round(sum(p * w for p, w in zip(parts, weights)) / sum(weights), 2)


def fit_employer_score(in_tables: Path, out_tables: Path) -> None:
    """Compute EFS for every employer row in employer_features.parquet.

    Parameters
    ----------
    in_tables : Path
        Directory containing employer_features.parquet.
    out_tables : Path
        Directory for output employer_friendliness_scores.parquet.
    """
    log_lines = []
    metrics_dir = Path(str(out_tables).replace('/tables', '/metrics'))
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = metrics_dir / 'employer_score.log'

    def log(msg):
        print(msg)
        log_lines.append(msg)

    log('=' * 70)
    log('EMPLOYER FRIENDLINESS SCORE (EFS) v1')
    log('=' * 70)

    # ── Load features ───────────────────────────────────────
    feat_path = in_tables / 'employer_features.parquet'
    if not feat_path.exists():
        log(f'ERROR: {feat_path} not found — run feature pipeline first')
        with open(log_path, 'w') as f:
            f.write('\n'.join(log_lines))
        return

    df = pd.read_parquet(feat_path)
    log(f'\n[A] Loaded employer features: {len(df):,} rows')
    log(f'  Scopes: {df["scope"].value_counts().to_dict()}')

    # ── Calculate subscores ─────────────────────────────────
    log('\n[B] Computing subscores')
    df['outcome_subscore'] = df.apply(_outcome_subscore, axis=1)
    df['wage_subscore'] = df.apply(_wage_subscore, axis=1)
    df['sustainability_subscore'] = df.apply(_sustainability_subscore, axis=1)

    # ── Composite EFS ───────────────────────────────────────
    log('\n[C] Computing composite EFS')
    df['efs_raw'] = (
        df['outcome_subscore'] * WEIGHT_OUTCOME +
        df['wage_subscore'] * WEIGHT_WAGE +
        df['sustainability_subscore'] * WEIGHT_SUSTAINABILITY
    )

    # ── Eligibility guardrails ──────────────────────────────
    log('\n[D] Applying eligibility guardrails')
    insufficient = df['n_24m'] < MIN_CASES_24M
    all_denied = df['approval_rate_24m'].fillna(0) == 0
    df['efs'] = df['efs_raw'].round(1)
    df.loc[insufficient, 'efs'] = np.nan
    df.loc[all_denied & ~insufficient, 'efs'] = df.loc[all_denied & ~insufficient, 'efs'].clip(upper=ALL_DENIED_CAP)

    elig_null = int(insufficient.sum())
    capped = int((all_denied & ~insufficient).sum())
    log(f'  Insufficient data (n_24m<{MIN_CASES_24M}): {elig_null:,} → EFS=NULL')
    log(f'  All-denied cap (≤{ALL_DENIED_CAP}): {capped:,}')

    # ── Assign tier labels ──────────────────────────────────
    log('\n[E] Assigning tier labels')
    conditions = [
        df['efs'] >= 85,
        df['efs'] >= 70,
        df['efs'] >= 50,
        df['efs'] >= 30,
        df['efs'] < 30,
    ]
    labels = ['Excellent', 'Good', 'Moderate', 'Below Average', 'Poor']
    df['efs_tier'] = np.select(conditions, labels, default='Unrated')
    tier_dist = df['efs_tier'].value_counts().to_dict()
    log(f'  Tier distribution: {tier_dist}')

    # ── Select output columns ───────────────────────────────
    out_cols = [
        'employer_id', 'employer_name', 'scope', 'soc_code',
        'n_12m', 'n_24m', 'n_36m',
        'approval_rate_24m', 'denial_rate_24m',
        'wage_ratio_med', 'wage_ratio_p75',
        'outcome_subscore', 'wage_subscore', 'sustainability_subscore',
        'efs', 'efs_tier',
        'months_active_24m', 'soc_breadth_24m', 'site_breadth_24m',
        'approval_rate_trend_12v12', 'outcome_volatility',
        'last_refreshed_at',
    ]
    out_cols = [c for c in out_cols if c in df.columns]
    df_out = df[out_cols].copy()

    # ── Write output ────────────────────────────────────────
    out_path = out_tables / 'employer_friendliness_scores.parquet'
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df_out.to_parquet(out_path, index=False, engine='pyarrow')
    log(f'\n[F] Written: {out_path} ({len(df_out):,} rows)')

    # Summary stats
    overall = df_out[df_out['scope'] == 'overall']
    valid = overall.dropna(subset=['efs'])
    log(f'\n  Overall employers: {len(overall):,}')
    log(f'  With valid EFS: {len(valid):,}')
    if len(valid) > 0:
        log(f'  EFS mean: {valid["efs"].mean():.1f}')
        log(f'  EFS median: {valid["efs"].median():.1f}')
        log(f'  EFS std: {valid["efs"].std():.1f}')
        log(f'  EFS range: [{valid["efs"].min():.1f}, {valid["efs"].max():.1f}]')

    with open(log_path, 'w') as f:
        f.write('\n'.join(log_lines))
    log(f'  Log: {log_path}')
    log('\n✓ EMPLOYER FRIENDLINESS SCORE COMPLETE')
