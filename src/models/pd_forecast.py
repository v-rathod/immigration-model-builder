"""Priority date forecast model.

Produces forward-looking predictions of when each (category, country)
cutoff date will reach a given priority date, using historical velocity
data from fact_cutoff_trends.parquet.

Model approach v2.1 (per series = chart × category × country):
    1. Load historical cutoff-date time series from fact_cutoff_trends.
    2. Compute three velocity signals:
       a. Full-history net velocity (total advancement / total months)
          — the ground-truth long-term pace including retrogressions.
       b. Rolling 24-month mean — medium-term trend.
       c. Rolling 12-month mean — recent momentum.
    3. Blend: 50% full-history + 25% 24m + 25% 12m (anchored to long-term reality).
    4. Exclude extreme outliers (>P95 / <P5) from seasonal estimation.
    5. Apply neighbor-smoothed seasonal factors clamped to [0.5, 2.0].
    6. Project forward 24 months using adjusted velocity.
    7. Confidence intervals based on IQR (not std) of monthly advancement.
    8. Write predictions + model parameters.

Key design principles:
    - Full-history net velocity as anchor (captures retrogression impact).
    - P5/P95 outlier trimming for seasonal factors.
    - Smooth seasonal factors with neighbor blending.
    - Floor at 0 days for projection (retrogression is modeled via uncertainty).
    - Conservative long-term velocity wins over short-term optimism.

Outputs:
    - artifacts/models/pd_forecast_model.json  (model parameters per series)
    - artifacts/tables/pd_forecasts.parquet     (24-month forward projections)
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
CONFIDENCE_Z = 1.645           # 90% confidence interval
OUTLIER_LO_PCT = 5             # percentile floor for trimming
OUTLIER_HI_PCT = 95            # percentile ceiling for trimming
ROLLING_WINDOW = 12            # months for rolling velocity


def _load_trends(in_tables: Path) -> pd.DataFrame:
    """Load fact_cutoff_trends and prepare for modeling."""
    path = in_tables / "fact_cutoff_trends.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Required input not found: {path}")
    df = pd.read_parquet(path)

    # Parse dates
    df["cutoff_date"] = pd.to_datetime(df["cutoff_date"], errors="coerce")

    # Ensure integer types for year/month
    df["bulletin_year"] = df["bulletin_year"].astype(int)
    df["bulletin_month"] = df["bulletin_month"].astype(int)

    # Create a proper time index: bulletin month as date
    df["bulletin_date"] = pd.to_datetime(
        df["bulletin_year"].astype(str) + "-" + df["bulletin_month"].astype(str).str.zfill(2) + "-01"
    )

    # Sort chronologically
    df = df.sort_values(["chart", "category", "country", "bulletin_date"]).reset_index(drop=True)
    return df


def _trim_outliers(values: np.ndarray) -> np.ndarray:
    """Remove values below P5 and above P95 to get robust estimates."""
    if len(values) < 5:
        return values
    lo = np.nanpercentile(values, OUTLIER_LO_PCT)
    hi = np.nanpercentile(values, OUTLIER_HI_PCT)
    mask = (values >= lo) & (values <= hi)
    trimmed = values[mask]
    return trimmed if len(trimmed) >= 3 else values


def _compute_seasonal_factors(df: pd.DataFrame) -> dict:
    """Compute robust seasonal factors using MEDIAN of trimmed data.

    Returns dict of month -> multiplicative factor (1.0 = average).
    Uses per-series seasonal estimation, then averages across series
    to avoid any single series dominating.
    """
    dated = df[df["status_flag"] == "D"].copy()
    if dated.empty:
        return {m: 1.0 for m in range(1, 13)}

    dated["cal_month"] = dated["bulletin_month"].astype(int)

    # Compute per-month median advancement across ALL series
    # (using median of monthly medians across series for extra robustness)
    all_adv = dated["monthly_advancement_days"].dropna().values
    trimmed_global = _trim_outliers(all_adv)
    overall_median = float(np.median(trimmed_global)) if len(trimmed_global) > 0 else 1.0
    if overall_median <= 0:
        # If overall median is 0 (common for India), use mean of trimmed
        overall_median = float(np.mean(trimmed_global)) if len(trimmed_global) > 0 else 1.0
    if overall_median <= 0:
        overall_median = 1.0  # avoid division by zero

    factors = {}
    for m in range(1, 13):
        month_vals = dated.loc[dated["cal_month"] == m, "monthly_advancement_days"].dropna().values
        if len(month_vals) < 3:
            factors[m] = 1.0
            continue
        trimmed = _trim_outliers(month_vals)
        # Use MEAN of trimmed (not median) because median would be 0 for stall months
        month_mean = float(np.mean(trimmed))
        raw_factor = month_mean / overall_median if overall_median > 0 else 1.0
        # Clamp to [0.5, 2.0] — gentle seasonal variation only
        factors[m] = max(0.5, min(2.0, raw_factor))

    # Smooth: blend each month with its neighbors to avoid sharp oscillations
    smoothed = {}
    for m in range(1, 13):
        prev_m = 12 if m == 1 else m - 1
        next_m = 1 if m == 12 else m + 1
        smoothed[m] = 0.5 * factors[m] + 0.25 * factors[prev_m] + 0.25 * factors[next_m]

    # Normalize so factors average to 1.0
    avg = sum(smoothed.values()) / 12
    if avg > 0:
        smoothed = {m: v / avg for m, v in smoothed.items()}

    return smoothed


def _fit_single_series(
    series_df: pd.DataFrame,
    seasonal_factors: dict,
) -> dict | None:
    """Fit forecast model for a single (chart, category, country) series.

    Uses rolling 12-month median velocity as primary signal.
    Returns dict with model parameters and 24-month projections, or None
    if insufficient data.
    """
    # Only use rows with actual dates
    dated = series_df[series_df["status_flag"] == "D"].copy()
    if len(dated) < MIN_HISTORY_MONTHS:
        return None

    chart = dated["chart"].iloc[0]
    category = dated["category"].iloc[0]
    country = dated["country"].iloc[0]

    # Sort and dedup by bulletin_date
    dated = dated.sort_values("bulletin_date").drop_duplicates(
        subset=["bulletin_year", "bulletin_month"], keep="last"
    ).reset_index(drop=True)

    # Monthly advancement in days
    adv = dated["monthly_advancement_days"].fillna(0).values.astype(float)
    cutoff_dates = dated["cutoff_date"].values
    bulletin_dates = dated["bulletin_date"].values
    retro_flags = dated["retrogression_flag"].fillna(0).values

    # -- Full-history NET velocity (ground truth) --
    # Total actual cutoff advancement / total months.
    # This is the REAL long-term pace, including retrogressions.
    first_cutoff = pd.Timestamp(cutoff_dates[0])
    last_cutoff = pd.Timestamp(cutoff_dates[-1])
    first_bulletin = pd.Timestamp(bulletin_dates[0])
    last_bulletin = pd.Timestamp(bulletin_dates[-1])
    total_months = max(1, (last_bulletin.year - first_bulletin.year) * 12
                       + (last_bulletin.month - first_bulletin.month))
    net_advancement = (last_cutoff - first_cutoff).days
    full_history_vel = net_advancement / total_months
    full_history_vel = max(full_history_vel, 0.0)  # floor at 0

    # -- Rolling 12-month mean (recent momentum) --
    recent_12 = adv[-ROLLING_WINDOW:] if len(adv) >= ROLLING_WINDOW else adv
    rolling_mean_12m = float(np.mean(recent_12))

    # -- Rolling 24-month mean (medium-term trend) --
    recent_24 = adv[-24:] if len(adv) >= 24 else adv
    rolling_mean_24m = float(np.mean(recent_24))

    # -- Blend: 50% full-history + 25% 24m + 25% 12m --
    # The full-history net velocity anchors us to the long-term reality.
    # Short windows capture recent momentum but can be overly optimistic
    # after recovery from retrogression (e.g., EB2-India post-2023).
    #
    # Cap rolling means: recent velocity cannot exceed the long-term
    # pace by more than 25% or +5 d/mo (whichever is greater).
    # This prevents a single fast year from dominating the forecast.
    velocity_cap = max(full_history_vel * 1.25, full_history_vel + 5.0)
    capped_12m = min(max(rolling_mean_12m, 0.0), velocity_cap)
    capped_24m = min(max(rolling_mean_24m, 0.0), velocity_cap)

    base_velocity = (0.50 * full_history_vel
                     + 0.25 * capped_24m
                     + 0.25 * capped_12m)
    base_velocity = max(base_velocity, 0.0)  # floor at 0

    # -- Robust volatility: use IQR of trimmed data, not raw std --
    trimmed_adv = _trim_outliers(adv)
    if len(trimmed_adv) >= 4:
        q25 = float(np.percentile(trimmed_adv, 25))
        q75 = float(np.percentile(trimmed_adv, 75))
        iqr = q75 - q25
        robust_std = iqr / 1.35  # IQR-based std estimate
    else:
        robust_std = float(np.nanstd(adv)) if len(adv) > 3 else 30.0
    robust_std = max(robust_std, 5.0)  # floor

    # -- Retrogression regime detection --
    recent_retro = retro_flags[-12:] if len(retro_flags) >= 12 else retro_flags
    retro_rate = float(np.mean(recent_retro > 0))

    # If > 30% of last 12 months had retrogression, dampen by 30%
    retro_dampen = 0.7 if retro_rate > 0.3 else 1.0

    # -- Movement pattern analysis --
    # What fraction of months actually have positive movement?
    positive_months = float(np.mean(recent_12 > 0))
    zero_months = float(np.mean(recent_12 == 0))

    # -- Current state --
    last_cutoff = pd.Timestamp(cutoff_dates[-1])
    last_bulletin = pd.Timestamp(bulletin_dates[-1])

    # -- Project forward --
    projections = []
    running_cutoff = last_cutoff
    cumulative_days = 0

    for i in range(1, FORECAST_HORIZON + 1):
        proj_date = last_bulletin + pd.DateOffset(months=i)
        proj_month = proj_date.month

        # Seasonal adjustment (from robust median-based factors)
        season = seasonal_factors.get(proj_month, 1.0)

        # Compute velocity for this month
        velocity = base_velocity * season * retro_dampen

        # Floor at 0 (projections don't go backwards)
        velocity = max(velocity, 0.0)

        cumulative_days += velocity
        projected_cutoff = running_cutoff + timedelta(days=velocity)
        running_cutoff = projected_cutoff

        # Confidence interval: IQR-based, widens with sqrt(horizon)
        ci_width = CONFIDENCE_Z * robust_std * np.sqrt(i)
        ci_low = projected_cutoff - timedelta(days=ci_width)
        ci_high = projected_cutoff + timedelta(days=ci_width)

        projections.append({
            "forecast_month": proj_date.strftime("%Y-%m"),
            "months_ahead": i,
            "chart": chart,
            "category": category,
            "country": country,
            "projected_cutoff_date": projected_cutoff,
            "confidence_low": ci_low,
            "confidence_high": ci_high,
            "velocity_days_per_month": round(velocity, 1),
            "cumulative_advancement_days": round(cumulative_days, 0),
        })

    # Model parameters for this series
    params = {
        "chart": chart,
        "category": category,
        "country": country,
        "base_velocity_days": round(float(base_velocity), 2),
        "full_history_net_vel": round(float(full_history_vel), 2),
        "rolling_12m_mean": round(float(rolling_mean_12m), 2),
        "rolling_24m_mean": round(float(rolling_mean_24m), 2),
        "robust_std_days": round(float(robust_std), 2),
        "retro_regime": retro_rate > 0.3,
        "retro_rate_12m": round(float(retro_rate), 3),
        "positive_month_pct": round(float(positive_months), 3),
        "zero_month_pct": round(float(zero_months), 3),
        "history_months": len(dated),
        "total_months_span": total_months,
        "net_advancement_days": net_advancement,
        "last_cutoff_date": last_cutoff.strftime("%Y-%m-%d"),
        "last_bulletin_date": last_bulletin.strftime("%Y-%m-%d"),
    }

    return {"params": params, "projections": projections}


def fit_pd_forecast(in_tables: Path, out_models: Path, out_tables: Path) -> None:
    """Train priority date movement forecasting model (v2.1 — long-term anchored).

    v2.1 changes from v2.0:
    - Added full-history net velocity (total advancement / total months)
    - Blend: 50% full-history + 25% 24m + 25% 12m (anchored to long-term reality)
    - Prevents over-optimism from recent recovery periods post-retrogression

    Args:
        in_tables: Path to curated tables directory
        out_models: Path to models output directory
        out_tables: Path to tables output directory (for predictions)
    """
    print("[PD FORECAST MODEL v2 — Robust]")
    print(f"  Input: {in_tables}/fact_cutoff_trends.parquet")

    # -- Load data --
    df = _load_trends(in_tables)
    print(f"  Loaded {len(df):,} trend rows")

    # -- Compute empirical seasonal factors --
    seasonal_factors = _compute_seasonal_factors(df)
    print(f"  Seasonal factors computed for 12 months")

    # -- Fit per-series models --
    all_params = []
    all_projections = []
    skipped = 0

    groups = df.groupby(["chart", "category", "country"])
    for (chart, category, country), g_df in groups:
        result = _fit_single_series(g_df, seasonal_factors)
        if result is None:
            skipped += 1
            continue
        all_params.append(result["params"])
        all_projections.extend(result["projections"])

    print(f"  Fitted {len(all_params)} series, skipped {skipped} (insufficient data)")

    # -- Write model parameters --
    model_path = out_models / "pd_forecast_model.json"
    model_path.parent.mkdir(parents=True, exist_ok=True)

    model_doc = {
        "model_type": "robust_longterm_anchored_seasonal",
        "version": "2.1.0",
        "trained_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forecast_horizon_months": FORECAST_HORIZON,
        "rolling_window_months": ROLLING_WINDOW,
        "outlier_trim_pct": [OUTLIER_LO_PCT, OUTLIER_HI_PCT],
        "confidence_level": "90%",
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

        # Ensure datetime columns
        for col in ["projected_cutoff_date", "confidence_low", "confidence_high"]:
            df_pred[col] = pd.to_datetime(df_pred[col], errors="coerce")

        # Order columns
        col_order = [
            "forecast_month", "months_ahead", "chart", "category", "country",
            "projected_cutoff_date", "confidence_low", "confidence_high",
            "velocity_days_per_month", "cumulative_advancement_days",
        ]
        df_pred = df_pred[[c for c in col_order if c in df_pred.columns]]
    else:
        df_pred = pd.DataFrame(columns=[
            "forecast_month", "months_ahead", "chart", "category", "country",
            "projected_cutoff_date", "confidence_low", "confidence_high",
            "velocity_days_per_month", "cumulative_advancement_days",
        ])

    pred_path = out_tables / "pd_forecasts.parquet"
    pred_path.parent.mkdir(parents=True, exist_ok=True)
    df_pred.to_parquet(pred_path, index=False)
    print(f"  Predictions: {pred_path} ({len(df_pred):,} rows)")

    # Summary stats
    if len(df_pred):
        for chart in sorted(df_pred["chart"].unique()):
            sub = df_pred[df_pred["chart"] == chart]
            series_count = sub.groupby(["category", "country"]).ngroups
            print(f"    {chart}: {series_count} series x {FORECAST_HORIZON} months = {len(sub):,} rows")
