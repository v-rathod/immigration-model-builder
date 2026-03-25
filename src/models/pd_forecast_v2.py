"""Priority date forecast model (v2.2).

Windowed full-history velocity + anomaly-weighted rolling means.

    1. Windowed full-history velocity (8-year window instead of all-time):
       Restricting to the last 8 years gives a realistic long-term anchor
       that reflects the current backlog regime, avoiding inflation from
       early visa-bulletin jumps (e.g., EB3-CHN Oct-2015: 2488 d/mo).

    2. Anomaly-weighted rolling means:
       Months with monthly_advancement_days above P90 of the windowed
       history receive a reduced ANOMALY_WEIGHT = 0.3 (fiscal-year resets,
       administrative catchups, etc.).

    Core design:
    - 50/25/25 blend of full-history + 24m + 12m
    - velocity cap at max(vel*1.25, vel+5)
    - P5/P95 outlier trim for seasonal factors only
    - IQR-based confidence intervals
    - Retrogression dampen (*0.7 if retro_rate > 30%)

Outputs:
    - artifacts/models/pd_forecast_model.json
    - artifacts/tables/pd_forecasts.parquet
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
OUTLIER_LO_PCT = 5             # percentile floor for seasonal trimming
OUTLIER_HI_PCT = 95            # percentile ceiling for seasonal trimming
ROLLING_WINDOW = 12            # months for rolling velocity

# V2 additions
HISTORY_WINDOW_YEARS = 8       # use only last N years for full_history_vel anchor
ANOMALY_WEIGHT = 0.3           # weight for anomalous months in rolling means
ANOMALY_THRESHOLD_PCT = 90     # months above this percentile are "anomalous"


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
        df["bulletin_year"].astype(str) + "-" + df["bulletin_month"].astype(str).str.zfill(2) + "-01"
    )
    df = df.sort_values(["chart", "category", "country", "bulletin_date"]).reset_index(drop=True)
    return df


def _trim_outliers(values: np.ndarray) -> np.ndarray:
    """Remove values below P5 and above P95 for robust seasonal estimation."""
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
    """Weighted mean that downweights anomalous months.

    Months with advancement above `anomaly_threshold` get weight
    `anomaly_weight` (default 0.3) instead of 1.0.  This prevents
    one-time fiscal-year resets or administrative catchups from
    dominating the recent-momentum signals.

    Negative advancement (retrogression) is kept at full weight because
    retrogressions are real signals, not noise.
    """
    if len(values) == 0:
        return 0.0
    weights = np.where(values > anomaly_threshold, anomaly_weight, 1.0)
    return float(np.average(values, weights=weights))


def _compute_seasonal_factors(df: pd.DataFrame) -> dict:
    """Compute robust seasonal factors using median of P5/P95-trimmed data.

    Identical to V2.1 — seasonal factors use all series to remain stable.
    """
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


def _fit_single_series_v2(
    series_df: pd.DataFrame,
    seasonal_factors: dict,
) -> dict | None:
    """Fit V2 forecast model for a single (chart, category, country) series.

    V2 changes vs V2.1:
    - full_history_vel computed from last HISTORY_WINDOW_YEARS (8yr) only.
    - rolling 12m and 24m means use anomaly-weighted average.
    """
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
    retro_flags = dated["retrogression_flag"].fillna(0).values

    last_bulletin = pd.Timestamp(bulletin_dates[-1])
    last_cutoff = pd.Timestamp(cutoff_dates[-1])

    # -- V2: Windowed full-history velocity (last 8 years) --
    # The "full history" anchor in V2.1 used all available data (up to 177
    # months), which may include early-era anomalies where a series jumped
    # many years in a single October due to a visa-bulletin reset.  Using
    # only the last 8 years reflects the current backlog regime more accurately.
    window_start = last_bulletin - pd.DateOffset(years=HISTORY_WINDOW_YEARS)
    windowed_dated = dated[dated["bulletin_date"] >= window_start].copy()

    if len(windowed_dated) >= MIN_HISTORY_MONTHS:
        w_adv = windowed_dated["monthly_advancement_days"].fillna(0).values.astype(float)
        w_cutoff_first = pd.Timestamp(windowed_dated["cutoff_date"].iloc[0])
        w_cutoff_last = pd.Timestamp(windowed_dated["cutoff_date"].iloc[-1])
        w_bulletin_first = pd.Timestamp(windowed_dated["bulletin_date"].iloc[0])
        w_bulletin_last = pd.Timestamp(windowed_dated["bulletin_date"].iloc[-1])
        window_months = max(1, (w_bulletin_last.year - w_bulletin_first.year) * 12
                            + (w_bulletin_last.month - w_bulletin_first.month))
        window_net_adv = (w_cutoff_last - w_cutoff_first).days
        full_history_vel = max(window_net_adv / window_months, 0.0)
        # Use windowed data for anomaly threshold
        history_for_percentile = w_adv
        window_months_used = window_months
        windowed_net_adv = window_net_adv
    else:
        # Fallback: use all data if 8-year window has insufficient rows
        first_cutoff_all = pd.Timestamp(cutoff_dates[0])
        first_bulletin_all = pd.Timestamp(bulletin_dates[0])
        total_months_all = max(1, (last_bulletin.year - first_bulletin_all.year) * 12
                               + (last_bulletin.month - first_bulletin_all.month))
        net_adv_all = (last_cutoff - first_cutoff_all).days
        full_history_vel = max(net_adv_all / total_months_all, 0.0)
        history_for_percentile = adv
        window_months_used = total_months_all
        windowed_net_adv = net_adv_all

    # -- V2: Anomaly threshold from windowed history --
    anomaly_threshold = float(np.percentile(history_for_percentile, ANOMALY_THRESHOLD_PCT))
    # Ensure threshold is at least 1 so non-zero months aren't all flagged
    anomaly_threshold = max(anomaly_threshold, 1.0)

    # -- V2: Anomaly-weighted rolling means --
    recent_12 = adv[-ROLLING_WINDOW:] if len(adv) >= ROLLING_WINDOW else adv
    recent_24 = adv[-24:] if len(adv) >= 24 else adv
    rolling_mean_12m = _compute_weighted_mean(recent_12, anomaly_threshold)
    rolling_mean_24m = _compute_weighted_mean(recent_24, anomaly_threshold)

    # -- Cap rolling means at long-term pace + margin (same as V2.1) --
    velocity_cap = max(full_history_vel * 1.25, full_history_vel + 5.0)
    capped_12m = min(max(rolling_mean_12m, 0.0), velocity_cap)
    capped_24m = min(max(rolling_mean_24m, 0.0), velocity_cap)

    # -- Blend: 50/25/25 (same as V2.1) --
    base_velocity = (0.50 * full_history_vel
                     + 0.25 * capped_24m
                     + 0.25 * capped_12m)
    base_velocity = max(base_velocity, 0.0)

    # -- Robust volatility: IQR of trimmed data (same as V2.1) --
    trimmed_adv = _trim_outliers(adv)
    if len(trimmed_adv) >= 4:
        q25 = float(np.percentile(trimmed_adv, 25))
        q75 = float(np.percentile(trimmed_adv, 75))
        iqr = q75 - q25
        robust_std = iqr / 1.35
    else:
        robust_std = float(np.nanstd(adv)) if len(adv) > 3 else 30.0
    robust_std = max(robust_std, 5.0)

    # -- Retrogression detection (same as V2.1) --
    recent_retro = retro_flags[-12:] if len(retro_flags) >= 12 else retro_flags
    retro_rate = float(np.mean(recent_retro > 0))
    retro_dampen = 0.7 if retro_rate > 0.3 else 1.0

    # -- Movement pattern analysis --
    positive_months = float(np.mean(recent_12 > 0))
    zero_months = float(np.mean(recent_12 == 0))

    # -- Project forward --
    projections = []
    running_cutoff = last_cutoff
    cumulative_days = 0

    for i in range(1, FORECAST_HORIZON + 1):
        proj_date = last_bulletin + pd.DateOffset(months=i)
        proj_month = proj_date.month
        season = seasonal_factors.get(proj_month, 1.0)
        velocity = max(base_velocity * season * retro_dampen, 0.0)
        cumulative_days += velocity
        projected_cutoff = running_cutoff + timedelta(days=velocity)
        running_cutoff = projected_cutoff

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

    params = {
        "chart": chart,
        "category": category,
        "country": country,
        "base_velocity_days": round(float(base_velocity), 2),
        # V2: windowed full-history vel (8yr)
        "full_history_net_vel": round(float(full_history_vel), 2),
        "window_months_used": window_months_used,
        "windowed_net_advancement_days": windowed_net_adv,
        # V2: anomaly info
        "anomaly_threshold_p90": round(float(anomaly_threshold), 1),
        "rolling_12m_weighted_mean": round(float(rolling_mean_12m), 2),
        "rolling_24m_weighted_mean": round(float(rolling_mean_24m), 2),
        # Capped values (after velocity cap)
        "capped_12m": round(float(capped_12m), 2),
        "capped_24m": round(float(capped_24m), 2),
        "robust_std_days": round(float(robust_std), 2),
        "retro_regime": retro_rate > 0.3,
        "retro_rate_12m": round(float(retro_rate), 3),
        "positive_month_pct": round(float(positive_months), 3),
        "zero_month_pct": round(float(zero_months), 3),
        "history_months": len(dated),
        "last_cutoff_date": last_cutoff.strftime("%Y-%m-%d"),
        "last_bulletin_date": last_bulletin.strftime("%Y-%m-%d"),
    }

    return {"params": params, "projections": projections}


def fit_pd_forecast_v2(in_tables: Path, out_models: Path, out_tables: Path) -> None:
    """Train priority date movement forecasting model (v2.2).

    Key design choices:
    - full_history_vel uses only last 8 years (HISTORY_WINDOW_YEARS=8)
    - rolling 12m/24m means use anomaly-weighted average (P90 threshold, weight=0.3)

    Args:
        in_tables: Path to curated tables directory
        out_models: Path to models output directory
        out_tables: Path to tables output directory
    """
    print("[PD FORECAST MODEL v2.2 — Windowed + Anomaly-Weighted]")
    print(f"  Input: {in_tables}/fact_cutoff_trends.parquet")
    print(f"  V2 settings: history_window={HISTORY_WINDOW_YEARS}yr, "
          f"anomaly_weight={ANOMALY_WEIGHT} for months >P{ANOMALY_THRESHOLD_PCT}")

    df = _load_trends(in_tables)
    print(f"  Loaded {len(df):,} trend rows")

    seasonal_factors = _compute_seasonal_factors(df)
    print(f"  Seasonal factors computed for 12 months")

    all_params = []
    all_projections = []
    skipped = 0

    groups = df.groupby(["chart", "category", "country"])
    for (chart, category, country), g_df in groups:
        result = _fit_single_series_v2(g_df, seasonal_factors)
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
        "model_type": "windowed_anomaly_weighted_seasonal",
        "version": "2.2.0",
        "trained_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forecast_horizon_months": FORECAST_HORIZON,
        "rolling_window_months": ROLLING_WINDOW,
        "history_window_years": HISTORY_WINDOW_YEARS,
        "anomaly_weight": ANOMALY_WEIGHT,
        "anomaly_threshold_pct": ANOMALY_THRESHOLD_PCT,
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
        for col in ["projected_cutoff_date", "confidence_low", "confidence_high"]:
            df_pred[col] = pd.to_datetime(df_pred[col], errors="coerce")
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

    if len(df_pred):
        for chart in sorted(df_pred["chart"].unique()):
            sub = df_pred[df_pred["chart"] == chart]
            series_count = sub.groupby(["category", "country"]).ngroups
            print(f"    {chart}: {series_count} series x {FORECAST_HORIZON} months = {len(sub):,} rows")

    # -- Comparison report: India + China EB2/EB3 (V2 vs V1) --
    _print_comparison_report(all_params, all_projections)


def _print_comparison_report(all_params: list, all_projections: list) -> None:
    """Print model parameters for India and China EB2/EB3 series.

    Shows: full_history_vel (windowed), base_velocity, 24m total_advancement.
    """
    focus = [
        ("DFF", "EB2", "IND"), ("DFF", "EB3", "IND"),
        ("FAD", "EB2", "IND"), ("FAD", "EB3", "IND"),
        ("DFF", "EB2", "CHN"), ("DFF", "EB3", "CHN"),
        ("FAD", "EB2", "CHN"), ("FAD", "EB3", "CHN"),
    ]

    params_idx = {(p["chart"], p["category"], p["country"]): p for p in all_params}

    # Build 24-month cumulative from projections
    proj_df = pd.DataFrame(all_projections)
    max_proj = proj_df[proj_df["months_ahead"] == FORECAST_HORIZON]
    cumul_idx = {
        (r["chart"], r["category"], r["country"]): r["cumulative_advancement_days"]
        for _, r in max_proj.iterrows()
    }

    print()
    print("=" * 72)
    print("  Model Parameters: India + China EB2/EB3")
    print(f"  (8-yr windowed velocity | anomaly-weighted rolling means)")
    print("=" * 72)
    print(f"  {'Series':<16} {'hist_vel':>9} {'base_vel':>9} {'12m_wtd':>9} {'24m_wtd':>9} {'cumul_24m':>10} {'P90_thr':>8}")
    print(f"  {'-'*16} {'-'*9} {'-'*9} {'-'*9} {'-'*9} {'-'*10} {'-'*8}")
    for (chart, cat, country) in focus:
        key = (chart, cat, country)
        p = params_idx.get(key)
        if p is None:
            print(f"  {chart} {cat} {country:<5}   (no data)")
            continue
        label = f"{chart} {cat} {country}"
        cumul = cumul_idx.get(key, 0)
        print(
            f"  {label:<16} "
            f"{p['full_history_net_vel']:>9.1f} "
            f"{p['base_velocity_days']:>9.2f} "
            f"{p['rolling_12m_weighted_mean']:>9.2f} "
            f"{p['rolling_24m_weighted_mean']:>9.2f} "
            f"{cumul:>10.0f} "
            f"{p['anomaly_threshold_p90']:>8.1f}"
        )
    print("=" * 72)
    print("  Columns: hist_vel=8yr full-hist d/mo | base_vel=final blend d/mo")
    print("           12m_wtd/24m_wtd=anomaly-weighted rolling means (pre-cap)")
    print("           cumul_24m=total projected advancement days over 24 months")
    print("           P90_thr=anomaly threshold (d/mo, months above get wt=0.3)")
    print()


if __name__ == "__main__":
    base = Path(__file__).parent.parent.parent  # repo root
    fit_pd_forecast_v2(
        in_tables=base / "artifacts" / "tables",
        out_models=base / "artifacts" / "models",
        out_tables=base / "artifacts" / "tables",
    )
