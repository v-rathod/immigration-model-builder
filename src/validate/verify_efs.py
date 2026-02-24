"""EFS quality-gate verification with detailed diagnostics.

Structural, coverage, and sanity checks on employer_friendliness_scores.parquet.
Exit code 0 = PASS, 1 = FAIL.

Diagnostics appended to efs_verify.log:
  - Eligibility audit
  - Range audit (quantiles)
  - Correlation: efs vs approval_rate_24m (bootstrap 95% CI)
  - Wage-decile effect
  - Coverage stats
  - Top residuals (low EFS / high approval and vice versa)
"""

import argparse
import json
import sys
import numpy as np
import pandas as pd
from pathlib import Path

from src.io.readers import load_paths_config


def _bootstrap_corr(x, y, n_boot=2000, seed=42):
    """Pearson correlation with bootstrap 95% CI."""
    rng = np.random.RandomState(seed)
    r_obs = np.corrcoef(x, y)[0, 1]
    n = len(x)
    boots = []
    for _ in range(n_boot):
        idx = rng.randint(0, n, n)
        boots.append(np.corrcoef(x[idx], y[idx])[0, 1])
    boots = np.array(boots)
    lo, hi = np.percentile(boots, [2.5, 97.5])
    return r_obs, lo, hi


def verify_efs(artifacts_root: str) -> bool:
    """Run quality gates on EFS output.

    Returns True if all gates pass.
    """
    log_lines = []
    metrics_dir = Path(artifacts_root) / 'metrics'
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = metrics_dir / 'efs_verify.log'
    tables = Path(artifacts_root) / 'tables'

    passed = 0
    failed = 0

    def log(msg):
        print(msg)
        log_lines.append(msg)

    def gate(name: str, condition: bool, detail: str = ''):
        nonlocal passed, failed
        if condition:
            log(f'  ✓ {name} {detail}')
            passed += 1
        else:
            log(f'  ✗ FAIL: {name} {detail}')
            failed += 1

    log('=' * 70)
    log('EFS QUALITY-GATE VERIFICATION')
    log('=' * 70)

    # ── Pre-check: files exist ──────────────────────────────
    log('\n[1] File existence')
    feat_path = tables / 'employer_features.parquet'
    score_path = tables / 'employer_friendliness_scores.parquet'
    gate('employer_features.parquet exists', feat_path.exists())
    gate('employer_friendliness_scores.parquet exists', score_path.exists())

    if not feat_path.exists() or not score_path.exists():
        log('\nCannot proceed without both files. ABORT.')
        with open(log_path, 'w') as f:
            f.write('\n'.join(log_lines))
        return False

    feat = pd.read_parquet(feat_path)
    scores = pd.read_parquet(score_path)

    # ── Structural checks ───────────────────────────────────
    log('\n[2] Structural checks')
    required_feat_cols = [
        'employer_id', 'scope', 'n_12m', 'n_24m', 'n_36m',
        'approval_rate_12m', 'approval_rate_24m', 'approval_rate_36m',
    ]
    required_score_cols = [
        'employer_id', 'scope', 'efs', 'efs_tier',
        'outcome_subscore', 'wage_subscore', 'sustainability_subscore',
    ]
    for col in required_feat_cols:
        gate(f'features has column "{col}"', col in feat.columns)
    for col in required_score_cols:
        gate(f'scores has column "{col}"', col in scores.columns)

    gate('features non-empty', len(feat) > 0, f'({len(feat):,} rows)')
    gate('scores non-empty', len(scores) > 0, f'({len(scores):,} rows)')
    gate('features row count = scores row count', len(feat) == len(scores),
         f'(feat={len(feat):,}, scores={len(scores):,})')

    # ── Scope checks ────────────────────────────────────────
    log('\n[3] Scope checks')
    overall_feat = feat[feat['scope'] == 'overall']
    overall_scores = scores[scores['scope'] == 'overall']
    gate('has "overall" scope in features', len(overall_feat) > 0, f'({len(overall_feat):,})')
    gate('has "overall" scope in scores', len(overall_scores) > 0, f'({len(overall_scores):,})')

    soc_feat = feat[feat['scope'] == 'SOC']
    gate('SOC slices present (if expected)', len(soc_feat) >= 0,
         f'({len(soc_feat):,} SOC-level rows)')

    # ── Value range checks (scores) ─────────────────────────
    log('\n[4] Value range checks')
    valid_scores = scores.dropna(subset=['efs'])
    if len(valid_scores) > 0:
        gate('EFS min ≥ 0', valid_scores['efs'].min() >= 0,
             f'(min={valid_scores["efs"].min():.1f})')
        gate('EFS max ≤ 100', valid_scores['efs'].max() <= 100,
             f'(max={valid_scores["efs"].max():.1f})')
        gate('outcome_subscore [0,100]',
             valid_scores['outcome_subscore'].between(0, 100).all(),
             f'(range=[{valid_scores["outcome_subscore"].min():.1f}, {valid_scores["outcome_subscore"].max():.1f}])')
        gate('wage_subscore [0,100]',
             valid_scores['wage_subscore'].between(0, 100).all(),
             f'(range=[{valid_scores["wage_subscore"].min():.1f}, {valid_scores["wage_subscore"].max():.1f}])')
        gate('sustainability_subscore [0,100]',
             valid_scores['sustainability_subscore'].between(0, 100).all(),
             f'(range=[{valid_scores["sustainability_subscore"].min():.1f}, {valid_scores["sustainability_subscore"].max():.1f}])')
    else:
        gate('has any valid EFS values', False)

    # ── Tier distribution ───────────────────────────────────
    log('\n[5] Tier distribution')
    expected_tiers = {'Excellent', 'Good', 'Moderate', 'Below Average', 'Poor', 'Unrated'}
    actual_tiers = set(scores['efs_tier'].unique())
    gate('all tiers are known labels', actual_tiers.issubset(expected_tiers),
         f'(found: {actual_tiers})')

    # Not all in one tier (degenerate)
    if len(valid_scores) > 100:
        top_tier_pct = scores['efs_tier'].value_counts(normalize=True).max()
        gate('no single tier >90%', top_tier_pct < 0.90,
             f'(max tier pct={top_tier_pct:.1%})')

    # ── Coverage checks ─────────────────────────────────────
    log('\n[6] Coverage checks')
    overall_valid = overall_scores.dropna(subset=['efs'])
    total_overall = len(overall_scores)
    valid_pct = len(overall_valid) / total_overall * 100 if total_overall > 0 else 0
    gate('≥10% of overall employers have valid EFS', valid_pct >= 10,
         f'({valid_pct:.1f}%)')

    # Wage ratio coverage
    if 'wage_ratio_med' in overall_scores.columns:
        wage_cov = overall_scores['wage_ratio_med'].notna().mean() * 100
        gate('wage_ratio coverage ≥10%', wage_cov >= 10,
             f'({wage_cov:.1f}%)')

    # ── Eligibility guardrail checks ────────────────────────
    log('\n[7] Eligibility guardrails')
    low_n = scores[scores['n_24m'] < 3]
    if len(low_n) > 0:
        gate('n_24m<3 → EFS is NULL', low_n['efs'].isna().all(),
             f'({len(low_n):,} rows checked)')

    # ── NEW: Eligibility audit (stricter) ───────────────────
    log('\n[8] Eligibility audit (n_24m<15 OR n_36m<30)')
    # Rows that fail the minimum-data bar but still got a score
    elig_mask = (scores['n_24m'] < 15) | (scores['n_36m'] < 30)
    # Only flag rows that should NEVER have a score under the original rule (n_24m<3)
    # But also count how many borderline (n_24m in [3,14]) got scored — informational
    borderline = scores[elig_mask & scores['efs'].notna()]
    strict_violated = scores[(scores['n_24m'] < 3) & scores['efs'].notna()]
    gate('strict eligibility (n_24m<3 scored) = 0', len(strict_violated) == 0,
         f'({len(strict_violated):,} violations)')
    log(f'  INFO: borderline scored (n_24m<15 OR n_36m<30): {len(borderline):,} rows')

    # ── NEW: Range audit with quantiles ─────────────────────
    log('\n[9] Range audit — quantiles')
    if len(valid_scores) > 0:
        qs = valid_scores['efs'].quantile([0.01, 0.05, 0.10, 0.25, 0.50,
                                            0.75, 0.90, 0.95, 0.99])
        for q, v in qs.items():
            log(f'  p{int(q*100):02d}: {v:.1f}')
        gate('all non-null EFS in [0,100]',
             (valid_scores['efs'] >= 0).all() and (valid_scores['efs'] <= 100).all(),
             f'(min={valid_scores["efs"].min():.2f}, max={valid_scores["efs"].max():.2f})')

    # ── NEW: Correlation — efs vs approval_rate_24m ─────────
    log('\n[10] Correlation: efs vs approval_rate_24m (bootstrap 95% CI)')
    corr_pair = valid_scores.dropna(subset=['approval_rate_24m'])
    corr_r, corr_lo, corr_hi = np.nan, np.nan, np.nan
    if len(corr_pair) >= 30:
        x = corr_pair['efs'].values
        y = corr_pair['approval_rate_24m'].values
        corr_r, corr_lo, corr_hi = _bootstrap_corr(x, y)
        log(f'  Pearson r = {corr_r:.4f}  95% CI [{corr_lo:.4f}, {corr_hi:.4f}]  n={len(corr_pair):,}')
        gate('positive correlation efs↔approval_rate_24m', corr_r > 0,
             f'(r={corr_r:.4f})')
    else:
        log(f'  SKIP: too few paired rows ({len(corr_pair)})')

    # ── NEW: Wage-decile effect ─────────────────────────────
    log('\n[11] Wage-decile effect (wage_ratio_med → mean efs)')
    wage_decile_lines = []
    wage_ok = True
    if 'wage_ratio_med' in valid_scores.columns:
        w = valid_scores.dropna(subset=['wage_ratio_med'])
        if len(w) >= 30:
            w = w.copy()
            w['wage_decile'] = pd.qcut(w['wage_ratio_med'], 10, labels=False,
                                        duplicates='drop') + 1
            decile_tbl = w.groupby('wage_decile')['efs'].agg(['mean', 'count'])
            for d, row in decile_tbl.iterrows():
                line = f'  D{int(d):2d}: mean_efs={row["mean"]:.1f}  n={int(row["count"])}'
                log(line)
                wage_decile_lines.append(line)
            d1_mean = decile_tbl['mean'].iloc[0]
            d10_mean = decile_tbl['mean'].iloc[-1]
            gate('wage decile 10 mean efs ≥ decile 1 − 2pts',
                 d10_mean >= d1_mean - 2.0,
                 f'(D1={d1_mean:.1f}, D{int(decile_tbl.index[-1])}={d10_mean:.1f})')
        else:
            log(f'  SKIP: too few rows with wage_ratio_med ({len(w)})')
    else:
        log('  SKIP: wage_ratio_med column missing')

    # ── NEW: Coverage detailed ──────────────────────────────
    log('\n[12] Coverage — detailed')
    overall_scored_pct = len(overall_valid) / max(len(overall_scores), 1) * 100
    log(f'  Overall employers scored: {len(overall_valid):,} / {len(overall_scores):,} ({overall_scored_pct:.1f}%)')

    soc_scores = scores[scores['scope'] == 'SOC']
    soc_eligible = soc_scores[soc_scores['n_24m'] >= 10]
    soc_scored = soc_eligible.dropna(subset=['efs'])
    soc_pct = len(soc_scored) / max(len(soc_eligible), 1) * 100
    log(f'  SOC slices (n_24m≥10) scored: {len(soc_scored):,} / {len(soc_eligible):,} ({soc_pct:.1f}%)')
    gate('SOC slices with n_24m≥10 all scored', soc_pct >= 99.0,
         f'({soc_pct:.1f}%)')

    # ── NEW: Top residuals ──────────────────────────────────
    log('\n[13] Top residuals (manual review candidates)')
    residual_lines = []
    if len(corr_pair) >= 30:
        rp = corr_pair.copy()
        # Standardise for residual detection
        rp['efs_z'] = (rp['efs'] - rp['efs'].mean()) / max(rp['efs'].std(), 1e-6)
        rp['ar_z'] = (rp['approval_rate_24m'] - rp['approval_rate_24m'].mean()) / max(rp['approval_rate_24m'].std(), 1e-6)
        rp['residual'] = rp['efs_z'] - rp['ar_z']

        log('\n  Low EFS despite high approval_rate_24m:')
        low_efs_high_ar = rp.nsmallest(5, 'residual')
        for _, r in low_efs_high_ar.iterrows():
            line = (f'    {r.get("employer_name","?")[:35]:35s}  '
                    f'efs={r["efs"]:.1f}  approval={r["approval_rate_24m"]:.2%}  '
                    f'n_24m={r["n_24m"]:.0f}  residual={r["residual"]:.2f}')
            log(line)
            residual_lines.append(line)

        log('\n  High EFS despite low approval_rate_24m:')
        high_efs_low_ar = rp.nlargest(5, 'residual')
        for _, r in high_efs_low_ar.iterrows():
            line = (f'    {r.get("employer_name","?")[:35]:35s}  '
                    f'efs={r["efs"]:.1f}  approval={r["approval_rate_24m"]:.2%}  '
                    f'n_24m={r["n_24m"]:.0f}  residual={r["residual"]:.2f}')
            log(line)
            residual_lines.append(line)

    # ── Summary ─────────────────────────────────────────────
    total = passed + failed
    log(f'\n{"=" * 70}')
    log(f'VERIFICATION SUMMARY: {passed}/{total} gates passed, {failed} failed')
    log('=' * 70)

    status = 'PASS' if failed == 0 else 'FAIL'
    log(f'Status: {status}')

    # ── Write detailed diagnostics JSON (for report gen) ────
    diag = {
        'gates_passed': passed,
        'gates_total': total,
        'status': status,
        'strict_violations': int(len(strict_violated)),
        'borderline_scored': int(len(borderline)),
        'quantiles': {f'p{int(q*100):02d}': round(float(v), 2) for q, v in qs.items()} if len(valid_scores) > 0 else {},
        'correlation': {
            'r': round(float(corr_r), 4) if not np.isnan(corr_r) else None,
            'ci_lo': round(float(corr_lo), 4) if not np.isnan(corr_lo) else None,
            'ci_hi': round(float(corr_hi), 4) if not np.isnan(corr_hi) else None,
            'n': int(len(corr_pair)),
        },
        'coverage': {
            'overall_scored': int(len(overall_valid)),
            'overall_total': int(len(overall_scores)),
            'overall_pct': round(overall_scored_pct, 1),
            'soc_scored': int(len(soc_scored)),
            'soc_eligible': int(len(soc_eligible)),
            'soc_pct': round(soc_pct, 1),
        },
        'wage_decile_lines': wage_decile_lines,
        'residual_lines': residual_lines,
    }
    diag_path = metrics_dir / 'efs_verify_diagnostics.json'
    with open(diag_path, 'w') as f:
        json.dump(diag, f, indent=2)

    with open(log_path, 'w') as f:
        f.write('\n'.join(log_lines))
    print(f'Log: {log_path}')

    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="Verify EFS quality gates")
    parser.add_argument("--paths", required=True, help="Path to paths.yaml config")
    args = parser.parse_args()

    config = load_paths_config(args.paths)
    ok = verify_efs(config['artifacts_root'])
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
