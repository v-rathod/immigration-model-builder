"""Monte Carlo Retrograde-Adjusted (MCRA) priority date forecast model.

Extends the base pd_forecast model (v2.1) with probabilistic retrograde risk
modeling. Instead of treating retrogression as a binary damper, this model:

1. **Estimates per-series retrograde probability** from 10-year history:
   - Monthly retrogression frequency per (chart, category, country)
   - Seasonal modulation (Oct/Nov = new fiscal year = higher risk)
   - Recent-trend weighting (last 36 months weighted 2× vs older data)

2. **Estimates retrograde severity** when it occurs:
   - Mean backward movement (days) from historical retrogressions
   - Separates mild (<60 days) vs severe (≥60 days) events
   - Uses exponentially-weighted moving average for regime detection

3. **Produces risk-adjusted monthly velocity**:
   For each forecast month m:
     expected_loss(m) = P(retro|month) × E[setback|retro]
     velocity_adj(m)  = base_velocity × seasonal(m) - expected_loss(m)
   This creates non-linear (stepped/dipping) forecast curves.

4. **Confidence intervals** widen in high-retrograde-risk months.

5. **Monte Carlo simulation** (N=2000 paths) produces:
   - Median trajectory (published as projected_cutoff_date)
   - P10/P90 bands (published as confidence_low / confidence_high)
   - Per-month retrograde probability and expected setback

Output:
    artifacts/tables/pd_forecasts_retrograde.parquet  (same schema as pd_forecasts
        PLUS: retrograde_prob, expected_setback_days, risk_adjusted_velocity)
    artifacts/models/pd_forecast_retrograde_model.json  (model parameters)

The original pd_forecasts.parquet is NOT modified — this is a parallel artifact
that P3 can switch between at runtime.
"""

from pathlib import Path
from datetime import datetime, timezone, timedelta
import json
import logging
import warnings

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore", category=FutureWarning)
logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger(__name__)

# -- Constants ----------------------------------------------------------------
FORECAST_HORIZON = 24          # months forward
MIN_HISTORY_MONTHS = 12        # minimum data points to fit a series
OUTLIER_LO_PCT = 5
OUTLIER_HI_PCT = 95
MC_SIMULATIONS = 2000          # Monte Carlo paths
MC_SEED = 42                   # reproducibility
RECENCY_WINDOW = 36            # months for recency weighting (2× weight)
SEVERE_RETRO_THRESHOLD = 60    # days — above = "severe" retrograde
HISTORY_WINDOW_YEARS = 8       # match pd_forecast_v2: use last 8 years for full_history_vel
                               # (avoids inflating velocity from early fast-advancement eras)
ANOMALY_THRESHOLD_PCT = 90    # match pd_forecast_v2: months above this pct are anomalous
ANOMALY_WEIGHT = 0.3          # match pd_forecast_v2: down-weight anomalous months (FY resets)

# Per-event retrograde cap: limits a single retrograde draw to at most 2 months
# of typical forward progress.  Without this cap, historical mega-retrogrades
# (e.g. the 2015 EB2-India FAD reset of 900+ days) would dominate severity
# estimates and push the MC median trajectory below zero, making FAD appear
# to regress indefinitely.  Historically, the typical worst-case forward
# retrograde seen in any single month is ~90 days; 60 days is a conservative
# but realistic cap for forward projection purposes.
MAX_SINGLE_SETBACK_DAYS = 60.0  # days — per-event cap in MC draws

# Minimum fraction of base_velocity the risk-adjusted velocity can fall to.
# Even in elevated-risk months the model must project at least this fraction
# of forward progress, preventing flat/negative FAD trajectories.
MIN_VEL_FRACTION = 0.30         # 30 % of base velocity is the floor


def _load_trends(in_tables: Path) -> pd.DataFrame:
    """Load fact_cutoff_trends and prepare for modeling."""
    path = in_tables / "fact_cutoff_trends.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Required input not found: {path}")
    df = pd.read_parquet(path)
    df["cutoff_date"] = pd.to_datetime(df["cutoff_date"], errors="coerce")
    df["bulletin_year"] = df["bulletin_year"].astype(int)
    df["bulletin_month"] = df["bulletin_month"].astype(int)
    df["bulletin_date"] = pd.to_datetime(
        df["bulletin_year"].astype(str) + "-"
        + df["bulletin_month"].astype(str).str.zfill(2) + "-01"
    )
    df = df.sort_values(
        ["chart", "category", "country", "bulletin_date"]
    ).reset_index(drop=True)
    return df


def _trim_outliers(values: np.ndarray) -> np.ndarray:
    if len(values) < 5:
        return values
    lo = np.nanpercentile(values, OUTLIER_LO_PCT)
    hi = np.nanpercentile(values, OUTLIER_HI_PCT)
    mask = (values >= lo) & (values <= hi)
    trimmed = values[mask]
    return trimmed if len(trimmed) >= 3 else values


def _compute_weighted_mean(
    values: np.ndarray,
    anomaly_threshold: float,
    anomaly_weight: float = ANOMALY_WEIGHT,
) -> float:
    """Anomaly-weighted mean matching pd_forecast_v2.

    Down-weights months above the anomaly threshold (fiscal-year resets,
    administrative catchups) so they don't inflate base_velocity.
    Retrogressions (negative values) are kept at full weight.
    """
    if len(values) == 0:
        return 0.0
    weights = np.where(values > anomaly_threshold, anomaly_weight, 1.0)
    return float(np.average(values, weights=weights))


def _compute_seasonal_factors(df: pd.DataFrame) -> dict:
    """Identical to base model — median-based seasonal factors clamped [0.5, 2.0]."""
    dated = df[df["status_flag"] == "D"].copy()
    if dated.empty:
        return {m: 1.0 for m in range(1, 13)}

    dated["cal_month"] = dated["bulletin_month"].astype(int)
    all_adv = dated["monthly_advancement_days"].dropna().values
    trimmed_global = _trim_outliers(all_adv)
    overall_median = float(np.median(trimmed_global)) if len(trimmed_global) > 0 else 1.0
    if overall_median <= 0:
        overall_median = float(np.mean(trimmed_global)) if len(trimmed_global) > 0 else 1.0
    if overall_median <= 0:
        overall_median = 1.0

    factors = {}
    for m in range(1, 13):
        month_vals = dated.loc[dated["cal_month"] == m, "monthly_advancement_days"].dropna().values
        if len(month_vals) < 3:
            factors[m] = 1.0
            continue
        trimmed = _trim_outliers(month_vals)
        month_mean = float(np.mean(trimmed))
        raw_factor = month_mean / overall_median if overall_median > 0 else 1.0
        factors[m] = max(0.5, min(2.0, raw_factor))

    smoothed = {}
    for m in range(1, 13):
        prev_m = 12 if m == 1 else m - 1
        next_m = 1 if m == 12 else m + 1
        smoothed[m] = 0.5 * factors[m] + 0.25 * factors[prev_m] + 0.25 * factors[next_m]

    avg = sum(smoothed.values()) / 12
    if avg > 0:
        smoothed = {m: v / avg for m, v in smoothed.items()}
    return smoothed


# ---------------------------------------------------------------------------
# Retrograde probability estimation
# ---------------------------------------------------------------------------

def _estimate_retrograde_profile(
    dated: pd.DataFrame
) -> dict:
    """Estimate per-calendar-month retrograde probability and severity.

    Uses all historical records for this series. Recent data (last 36 months)
    is weighted 2× to capture regime shifts.

    Returns:
        {
            "monthly_prob": {1: 0.08, 2: 0.05, ...},     # P(retro) per cal month
            "monthly_severity": {1: 45.0, 2: 30.0, ...}, # E[|setback|] per cal month
            "overall_prob": 0.12,
            "overall_severity": 52.3,
            "severe_prob": 0.04,    # P(setback ≥ 60 days)
            "regime": "low" | "moderate" | "elevated"
        }
    """
    adv = dated["monthly_advancement_days"].fillna(0).values.astype(float)
    months = dated["bulletin_month"].values.astype(int)
    bulletin_dates = pd.to_datetime(dated["bulletin_date"].values)

    # Recency weights: last RECENCY_WINDOW months get 2× weight
    cutoff_recent = bulletin_dates.max() - pd.DateOffset(months=RECENCY_WINDOW)
    weights = np.where(bulletin_dates >= cutoff_recent, 2.0, 1.0)

    is_retro = adv < 0
    retro_magnitudes = np.abs(adv[is_retro])

    # Overall stats (weighted)
    overall_prob = float(np.average(is_retro, weights=weights)) if len(adv) > 0 else 0.0

    if len(retro_magnitudes) > 0:
        # Weight retrograde magnitudes by their recency weights
        retro_weights = weights[is_retro]
        overall_severity = float(np.average(retro_magnitudes, weights=retro_weights))
    else:
        overall_severity = 0.0

    severe_mask = retro_magnitudes >= SEVERE_RETRO_THRESHOLD
    severe_prob = (
        float(np.sum(severe_mask)) / len(adv) if len(adv) > 0 else 0.0
    )

    # Per-month stats (weighted)
    monthly_prob = {}
    monthly_severity = {}
    for m in range(1, 13):
        month_mask = months == m
        if month_mask.sum() == 0:
            monthly_prob[m] = overall_prob
            monthly_severity[m] = overall_severity
            continue

        month_adv = adv[month_mask]
        month_weights = weights[month_mask]
        month_retro = month_adv < 0

        monthly_prob[m] = float(np.average(month_retro, weights=month_weights))

        month_retro_vals = np.abs(month_adv[month_retro])
        if len(month_retro_vals) > 0:
            month_retro_weights = month_weights[month_retro]
            monthly_severity[m] = float(
                np.average(month_retro_vals, weights=month_retro_weights)
            )
        else:
            monthly_severity[m] = overall_severity

    # Regime classification
    if overall_prob >= 0.20:
        regime = "elevated"
    elif overall_prob >= 0.10:
        regime = "moderate"
    else:
        regime = "low"

    return {
        "monthly_prob": monthly_prob,
        "monthly_severity": monthly_severity,
        "overall_prob": round(overall_prob, 4),
        "overall_severity": round(overall_severity, 1),
        "severe_prob": round(severe_prob, 4),
        "regime": regime,
    }


# ---------------------------------------------------------------------------
# Monte Carlo simulation
# ---------------------------------------------------------------------------

def _simulate_paths(
    base_velocity: float,
    seasonal_factors: dict,
    retro_profile: dict,
    robust_std: float,
    rng: np.random.Generator,
    start_month: int,
) -> np.ndarray:
    """Run MC_SIMULATIONS stochastic forward paths.

    Each path:
      for each month m in [1..24]:
        1. Draw base advancement from N(velocity × season, std)
        2. With P(retro|cal_month), replace with a negative draw
        3. Accumulate total advancement

    Returns:
        np.ndarray of shape (MC_SIMULATIONS, FORECAST_HORIZON) — cumulative days
    """
    paths = np.zeros((MC_SIMULATIONS, FORECAST_HORIZON))

    for sim in range(MC_SIMULATIONS):
        cumulative = 0.0
        for i in range(FORECAST_HORIZON):
            cal_month = ((start_month - 1 + i) % 12) + 1
            season = seasonal_factors.get(cal_month, 1.0)
            retro_prob = retro_profile["monthly_prob"].get(cal_month, 0.0)
            retro_sev = retro_profile["monthly_severity"].get(cal_month, 0.0)

            # Draw: will this month have a retrograde?
            if rng.random() < retro_prob and retro_sev > 0:
                # Retrograde month: draw magnitude from exponential distribution
                # centered on historical severity for this month, capped so that
                # rare mega-retrogrades don't dominate the MC median trajectory.
                raw_setback = rng.exponential(retro_sev)
                setback = min(raw_setback, MAX_SINGLE_SETBACK_DAYS)
                advancement = -setback
            else:
                # Normal month: draw from N(velocity * season, std)
                mean_vel = base_velocity * season
                advancement = rng.normal(mean_vel, robust_std)

            cumulative += advancement
            paths[sim, i] = cumulative

    return paths


def _fit_single_series_mcra(
    series_df: pd.DataFrame,
    seasonal_factors: dict,
    rng: np.random.Generator,
) -> dict | None:
    """Fit MCRA forecast for a single (chart, category, country) series."""
    dated = series_df[series_df["status_flag"] == "D"].copy()
    if len(dated) < MIN_HISTORY_MONTHS:
        return None

    chart = dated["chart"].iloc[0]
    category = dated["category"].iloc[0]
    country = dated["country"].iloc[0]

    dated = dated.sort_values("bulletin_date").drop_duplicates(
        subset=["bulletin_year", "bulletin_month"], keep="last"
    ).reset_index(drop=True)

    adv = dated["monthly_advancement_days"].fillna(0).values.astype(float)
    cutoff_dates = dated["cutoff_date"].values
    bulletin_dates = dated["bulletin_date"].values

    # -- Base velocity (8-year windowed anchor; matches pd_forecast_v2) --
    # Using ALL history overstated full_history_vel for series that advanced
    # rapidly in the early era (pre-2016) but have been slow since, causing
    # MCRA median paths to exceed the base model.  8-year window reflects the
    # current backlog regime.
    last_cutoff = pd.Timestamp(cutoff_dates[-1])
    last_bulletin = pd.Timestamp(bulletin_dates[-1])
    window_start = last_bulletin - pd.DateOffset(years=HISTORY_WINDOW_YEARS)
    windowed_mask = pd.to_datetime(dated["bulletin_date"]) >= window_start
    windowed_dated = dated[windowed_mask].copy()

    if len(windowed_dated) >= MIN_HISTORY_MONTHS:
        w_cutoff_first = pd.Timestamp(windowed_dated["cutoff_date"].iloc[0])
        w_bulletin_first = pd.Timestamp(windowed_dated["bulletin_date"].iloc[0])
        window_months = max(1, (last_bulletin.year - w_bulletin_first.year) * 12
                            + (last_bulletin.month - w_bulletin_first.month))
        window_net_adv = (last_cutoff - w_cutoff_first).days
        full_history_vel = max(window_net_adv / window_months, 0.0)
        total_months = window_months
        net_advancement = window_net_adv
    else:
        # Fallback: use all data when window has too few months
        first_cutoff_all = pd.Timestamp(cutoff_dates[0])
        first_bulletin_all = pd.Timestamp(bulletin_dates[0])
        total_months = max(1, (last_bulletin.year - first_bulletin_all.year) * 12
                           + (last_bulletin.month - first_bulletin_all.month))
        net_advancement = (last_cutoff - first_cutoff_all).days
        full_history_vel = max(net_advancement / total_months, 0.0)

    # Anomaly-weighted rolling means — matches pd_forecast_v2 to prevent
    # fiscal-year reset spikes from inflating base_velocity above the base model.
    # Threshold computed from windowed history (same window as full_history_vel).
    history_adv = adv[windowed_mask.values] if len(windowed_dated) >= MIN_HISTORY_MONTHS else adv
    anomaly_threshold = max(float(np.percentile(history_adv, ANOMALY_THRESHOLD_PCT)), 1.0)
    recent_12 = adv[-12:] if len(adv) >= 12 else adv
    rolling_mean_12m = _compute_weighted_mean(recent_12, anomaly_threshold)
    recent_24 = adv[-24:] if len(adv) >= 24 else adv
    rolling_mean_24m = _compute_weighted_mean(recent_24, anomaly_threshold)

    velocity_cap = max(full_history_vel * 1.25, full_history_vel + 5.0)
    capped_12m = min(max(rolling_mean_12m, 0.0), velocity_cap)
    capped_24m = min(max(rolling_mean_24m, 0.0), velocity_cap)
    base_velocity = max(
        0.50 * full_history_vel + 0.25 * capped_24m + 0.25 * capped_12m,
        0.0,
    )

    # -- Robust volatility --
    trimmed_adv = _trim_outliers(adv)
    if len(trimmed_adv) >= 4:
        q25 = float(np.percentile(trimmed_adv, 25))
        q75 = float(np.percentile(trimmed_adv, 75))
        robust_std = (q75 - q25) / 1.35
    else:
        robust_std = float(np.nanstd(adv)) if len(adv) > 3 else 30.0
    robust_std = max(robust_std, 5.0)

    # -- Retrograde probability profile --
    retro_profile = _estimate_retrograde_profile(dated)

    # -- Monte Carlo simulation --
    start_month = last_bulletin.month + 1
    if start_month > 12:
        start_month = 1

    paths = _simulate_paths(
        base_velocity, seasonal_factors, retro_profile,
        robust_std, rng, start_month,
    )

    # -- Extract percentiles --
    median_path = np.percentile(paths, 50, axis=0)
    p10_path = np.percentile(paths, 10, axis=0)
    p90_path = np.percentile(paths, 90, axis=0)

    # -- Build projections --
    projections = []
    for i in range(FORECAST_HORIZON):
        proj_date = last_bulletin + pd.DateOffset(months=i + 1)
        cal_month = ((start_month - 1 + i) % 12) + 1

        projected_cutoff = last_cutoff + timedelta(days=float(median_path[i]))
        ci_low = last_cutoff + timedelta(days=float(p10_path[i]))
        ci_high = last_cutoff + timedelta(days=float(p90_path[i]))

        # Risk-adjusted velocity for this month.
        # expected_setback_days (stored in artifact) reflects actual historical
        # severity for informational purposes; the projection uses a capped
        # effective setback so outlier mega-retrogrades do not dominate.
        retro_prob = retro_profile["monthly_prob"].get(cal_month, 0.0)
        retro_sev = retro_profile["monthly_severity"].get(cal_month, 0.0)
        season = seasonal_factors.get(cal_month, 1.0)
        effective_sev = min(retro_sev, MAX_SINGLE_SETBACK_DAYS)
        expected_loss = retro_prob * effective_sev
        min_vel = base_velocity * season * MIN_VEL_FRACTION
        risk_adjusted_vel = max(base_velocity * season - expected_loss, min_vel, 0.5)

        # Actual velocity from MC median (delta from previous month)
        if i == 0:
            mc_velocity = float(median_path[i])
        else:
            mc_velocity = float(median_path[i] - median_path[i - 1])

        cumulative_days = float(median_path[i])

        projections.append({
            "forecast_month": proj_date.strftime("%Y-%m"),
            "months_ahead": i + 1,
            "chart": chart,
            "category": category,
            "country": country,
            "projected_cutoff_date": projected_cutoff,
            "confidence_low": ci_low,
            "confidence_high": ci_high,
            "velocity_days_per_month": round(mc_velocity, 1),
            "cumulative_advancement_days": round(cumulative_days, 0),
            "retrograde_prob": round(retro_prob, 4),
            "expected_setback_days": round(retro_sev, 1),
            "risk_adjusted_velocity": round(risk_adjusted_vel, 1),
        })

    params = {
        "chart": chart,
        "category": category,
        "country": country,
        "base_velocity_days": round(float(base_velocity), 2),
        "full_history_net_vel": round(float(full_history_vel), 2),
        "rolling_12m_mean": round(float(rolling_mean_12m), 2),
        "rolling_24m_mean": round(float(rolling_mean_24m), 2),
        "robust_std_days": round(float(robust_std), 2),
        "history_months": len(dated),
        "total_months_span": total_months,
        "net_advancement_days": net_advancement,
        "last_cutoff_date": last_cutoff.strftime("%Y-%m-%d"),
        "last_bulletin_date": last_bulletin.strftime("%Y-%m-%d"),
        # Retrograde-specific params
        "retro_overall_prob": retro_profile["overall_prob"],
        "retro_overall_severity_days": retro_profile["overall_severity"],
        "retro_severe_prob": retro_profile["severe_prob"],
        "retro_regime": retro_profile["regime"],
        "retro_monthly_prob": {str(k): round(v, 4) for k, v in retro_profile["monthly_prob"].items()},
        "retro_monthly_severity": {str(k): round(v, 1) for k, v in retro_profile["monthly_severity"].items()},
        "mc_simulations": MC_SIMULATIONS,
        "mc_seed": MC_SEED,
    }

    return {"params": params, "projections": projections}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def fit_pd_forecast_retrograde(
    in_tables: Path, out_models: Path, out_tables: Path
) -> None:
    """Train MCRA priority date forecast model.

    Produces a SEPARATE artifact (pd_forecasts_retrograde.parquet) alongside
    the existing pd_forecasts.parquet. The original is never modified.

    Args:
        in_tables: Path to curated tables directory
        out_models: Path to models output directory
        out_tables: Path to tables output directory
    """
    print("[PD FORECAST MODEL v3 — Monte Carlo Retrograde-Adjusted (MCRA)]")
    print(f"  Input: {in_tables}/fact_cutoff_trends.parquet")

    df = _load_trends(in_tables)
    print(f"  Loaded {len(df):,} trend rows")

    seasonal_factors = _compute_seasonal_factors(df)
    print(f"  Seasonal factors computed for 12 months")

    rng = np.random.default_rng(MC_SEED)

    all_params = []
    all_projections = []
    skipped = 0

    groups = df.groupby(["chart", "category", "country"])
    for (chart, category, country), g_df in groups:
        result = _fit_single_series_mcra(g_df, seasonal_factors, rng)
        if result is None:
            skipped += 1
            continue
        all_params.append(result["params"])
        all_projections.extend(result["projections"])

    print(f"  Fitted {len(all_params)} MCRA series, skipped {skipped}")

    # -- Write model parameters --
    model_path = out_models / "pd_forecast_retrograde_model.json"
    model_path.parent.mkdir(parents=True, exist_ok=True)

    model_doc = {
        "model_type": "monte_carlo_retrograde_adjusted",
        "version": "3.0.0",
        "trained_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forecast_horizon_months": FORECAST_HORIZON,
        "mc_simulations": MC_SIMULATIONS,
        "mc_seed": MC_SEED,
        "severe_retro_threshold_days": SEVERE_RETRO_THRESHOLD,
        "recency_window_months": RECENCY_WINDOW,
        "confidence_level": "P10/P90 (80% band from Monte Carlo)",
        "seasonal_factors": {str(k): round(v, 3) for k, v in seasonal_factors.items()},
        "series_count": len(all_params),
        "series": all_params,
    }

    with open(model_path, "w") as f:
        json.dump(model_doc, f, indent=2, default=str)
    print(f"  Model params: {model_path}")

    # -- Write predictions table --
    if all_projections:
        df_pred = pd.DataFrame(all_projections)
        for col in ["projected_cutoff_date", "confidence_low", "confidence_high"]:
            df_pred[col] = pd.to_datetime(df_pred[col], errors="coerce")

        col_order = [
            "forecast_month", "months_ahead", "chart", "category", "country",
            "projected_cutoff_date", "confidence_low", "confidence_high",
            "velocity_days_per_month", "cumulative_advancement_days",
            "retrograde_prob", "expected_setback_days", "risk_adjusted_velocity",
        ]
        df_pred = df_pred[[c for c in col_order if c in df_pred.columns]]
    else:
        df_pred = pd.DataFrame()

    pred_path = out_tables / "pd_forecasts_retrograde.parquet"
    pred_path.parent.mkdir(parents=True, exist_ok=True)
    df_pred.to_parquet(pred_path, index=False)
    print(f"  MCRA Predictions: {pred_path} ({len(df_pred):,} rows)")

    if len(df_pred):
        for chart in sorted(df_pred["chart"].unique()):
            sub = df_pred[df_pred["chart"] == chart]
            series_count = sub.groupby(["category", "country"]).ngroups
            avg_retro = sub["retrograde_prob"].mean() * 100
            print(f"    {chart}: {series_count} series, avg retro prob: {avg_retro:.1f}%")

    # Summary of retrograde regimes
    regimes = {p["retro_regime"] for p in all_params}
    elevated = sum(1 for p in all_params if p["retro_regime"] == "elevated")
    moderate = sum(1 for p in all_params if p["retro_regime"] == "moderate")
    low = sum(1 for p in all_params if p["retro_regime"] == "low")
    print(f"  Regimes: {elevated} elevated, {moderate} moderate, {low} low")
