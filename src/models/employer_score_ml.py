"""Employer Friendliness Score v2 – ML-based calibrated scoring.

Trains a gradient-boosting classifier on case-level PERM data to predict
approval probability; aggregates calibrated per-case probabilities to
employer×(scope) level; maps to 0-100 score using monotone rescale.

Produces:  artifacts/tables/employer_friendliness_scores_ml.parquet
Writes:    artifacts/metrics/employer_score_ml.log
           artifacts/metrics/employer_score_ml_diagnostics.json

5-fold stratified CV → calibration (Platt/Isotonic) → Brier score.
Feature importance output (SHAP or sklearn if SHAP unavailable).
"""
from __future__ import annotations

import json
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


# ── Constants ────────────────────────────────────────────────────────────────
MIN_CASES_36M = 15       # minimum filings for employer to get a ML score
SCORE_BUCKETS = 100      # 0-100 integer score range
MODEL_VERSION = "efs_ml_v2"

# Feature columns expected in fact_perm (present in most years)
APPROVED_STATUS = {"CERTIFIED", "CERTIFIED-EXPIRED", "APPROVED"}
DENIED_STATUS = {"DENIED"}

WAGE_LEVEL_ORDER = {"I": 1, "II": 2, "III": 3, "IV": 4, "N/A": 2}

# ── Logging ──────────────────────────────────────────────────────────────────
LOG_LINES: list[str] = []


def _log(msg: str = "") -> None:
    print(msg)
    LOG_LINES.append(msg)


# ── Feature Engineering ──────────────────────────────────────────────────────

def _build_features(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series]:
    """Extract feature matrix X and binary label y from case-level PERM data."""
    rows = df.copy()

    # Label: 1 = approved, 0 = denied, drop others
    rows["is_approved"] = rows["case_status"].apply(
        lambda s: 1 if str(s).strip().upper() in APPROVED_STATUS else (
            0 if str(s).strip().upper() in DENIED_STATUS else np.nan
        )
    )
    rows = rows.dropna(subset=["is_approved"])
    y = rows["is_approved"].astype(int)

    features = pd.DataFrame(index=rows.index)

    # 1. Wage level (ordinal)
    if "pw_wage_level" in rows.columns:
        features["wage_level"] = rows["pw_wage_level"].map(
            lambda v: WAGE_LEVEL_ORDER.get(str(v).strip().upper(), 2)
        ).fillna(2).astype(float)
    else:
        features["wage_level"] = 2.0

    # 2. Wage offered / prevailing wage ratio
    if "wage_offered_yearly" in rows.columns and "pw_amount" in rows.columns:
        pw = pd.to_numeric(rows["pw_amount"], errors="coerce").replace(0, np.nan)
        wo = pd.to_numeric(rows["wage_offered_yearly"], errors="coerce")
        features["wage_ratio"] = (wo / pw).clip(0.5, 2.0).fillna(1.0)
    else:
        features["wage_ratio"] = 1.0

    # 3. SOC major group (first 2 digits)
    if "soc_code" in rows.columns:
        features["soc_major"] = (
            rows["soc_code"].astype(str).str.replace("-", "").str[:2]
            .pipe(pd.to_numeric, errors="coerce").fillna(0).astype(float)
        )
    else:
        features["soc_major"] = 0.0

    # 4. Fiscal year (ordinal trend)
    if "fiscal_year" in rows.columns:
        fy = pd.to_numeric(rows["fiscal_year"], errors="coerce")
        fy_min = fy.min()
        features["fy_offset"] = (fy - fy_min).fillna(0).astype(float)
    else:
        features["fy_offset"] = 0.0

    # 5. Employer filing volume (log-scaled; requires employer_id groupby)
    if "employer_id" in rows.columns:
        emp_counts = rows.groupby("employer_id").size().rename("emp_total")
        features["emp_log_vol"] = (
            rows["employer_id"].map(emp_counts)
            .pipe(np.log1p)
            .fillna(0)
        )
    else:
        features["emp_log_vol"] = 0.0

    # 6. Country of birth (if available; high-demand countries get different rates)
    if "country_of_birth" in rows.columns:
        # Top-5 high-demand countries as binary flags
        for country in ["INDIA", "CHINA", "MEXICO", "PHILIPPINES", "KOREA"]:
            features[f"country_{country.lower()}"] = (
                rows["country_of_birth"].astype(str).str.upper().str.contains(country, na=False)
            ).astype(float)
    else:
        for country in ["india", "china", "mexico", "philippines", "korea"]:
            features[f"country_{country}"] = 0.0

    features = features.fillna(0)
    return features, y


# ── Model Training ────────────────────────────────────────────────────────────

def _train_model(X: pd.DataFrame, y: pd.Series) -> tuple[Any, Any, dict]:
    """Train HGBM with 5-fold CV + calibration. Returns (model, calibrator, diag)."""
    from sklearn.ensemble import HistGradientBoostingClassifier
    from sklearn.calibration import CalibratedClassifierCV
    from sklearn.model_selection import StratifiedKFold, cross_val_score
    from sklearn.metrics import brier_score_loss

    # Sample for speed if dataset is very large
    MAX_TRAIN = 200_000
    if len(X) > MAX_TRAIN:
        _log(f"  Sampling {MAX_TRAIN:,} rows from {len(X):,} for training (speed)")
        sample_idx = X.sample(MAX_TRAIN, random_state=42).index
        X_train = X.loc[sample_idx]
        y_train = y.loc[sample_idx]
    else:
        X_train, y_train = X, y

    _log(f"  Training HistGradientBoostingClassifier on {len(X_train):,} rows …")
    base_model = HistGradientBoostingClassifier(
        max_iter=200,
        max_depth=5,
        learning_rate=0.05,
        min_samples_leaf=50,
        random_state=42,
    )

    # 5-fold CV AUC
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cv_aucs = cross_val_score(base_model, X_train, y_train, cv=cv,
                                  scoring="roc_auc", n_jobs=1)
    _log(f"  CV AUC: {cv_aucs.mean():.4f} ± {cv_aucs.std():.4f}")

    # Fit full base model
    base_model.fit(X_train, y_train)

    # Calibrate with isotonic regression
    calibrated = CalibratedClassifierCV(
        HistGradientBoostingClassifier(
            max_iter=200, max_depth=5, learning_rate=0.05,
            min_samples_leaf=50, random_state=42,
        ),
        method="isotonic",
        cv=3,
    )
    calibrated.fit(X_train, y_train)

    # Brier score on train set
    probs = calibrated.predict_proba(X_train)[:, 1]
    brier = brier_score_loss(y_train, probs)
    _log(f"  Brier score (train): {brier:.4f}")

    # Feature importance (HistGBM does not expose .feature_importances_ directly on calibrated)
    fi_dict: dict[str, float] = {}
    try:
        fi_arr = base_model.feature_importances_
        fi = pd.Series(fi_arr, index=X_train.columns).sort_values(ascending=False)
        fi_dict = fi.to_dict()
    except AttributeError:
        _log("  WARNING: feature importances not available for this estimator")
    _log(f"  Feature importances: {fi_dict}")

    diag = {
        "cv_auc_mean": float(cv_aucs.mean()),
        "cv_auc_std": float(cv_aucs.std()),
        "brier_score": float(brier),
        "feature_importances": {k: round(float(v), 5) for k, v in fi_dict.items()},
        "n_train": int(len(y_train)),
        "n_total": int(len(y)),
        "approval_rate_train": float(y_train.mean()),
    }

    return base_model, calibrated, diag


def _shap_importance(model: Any, X: pd.DataFrame, diag: dict) -> dict:
    """Try to get SHAP values; fall back gracefully for HistGBM."""
    try:
        import shap  # type: ignore[import]
        sample = X.sample(min(2000, len(X)), random_state=42)
        try:
            explainer = shap.TreeExplainer(model)
            shap_values = explainer.shap_values(sample)
        except Exception:
            # HistGBM not directly supported by TreeExplainer; use KernelExplainer on small sample
            explainer = shap.Explainer(model.predict_proba, sample.head(200))
            shap_values = explainer(sample.head(200)).values
        mean_abs = np.abs(shap_values).mean(axis=0)
        shap_dict = {col: round(float(v), 5) for col, v in zip(X.columns, mean_abs)}
        diag["shap_mean_abs"] = shap_dict
        _log("  SHAP computed successfully")
    except ImportError:
        _log("  SHAP not installed; using sklearn feature_importances instead")
    except Exception as e:
        _log(f"  SHAP failed ({e}); using sklearn feature_importances")
    return diag


# ── Score Aggregation ────────────────────────────────────────────────────────

def _aggregate_scores(
    df_cases: pd.DataFrame,
    calibrated: Any,
    X: pd.DataFrame,
    y: pd.Series,
) -> pd.DataFrame:
    """Aggregate per-case calibrated probabilities to employer level."""
    probs = calibrated.predict_proba(X)[:, 1]
    df_scored = df_cases.loc[X.index].copy()
    df_scored["_prob"] = probs

    if "employer_id" not in df_scored.columns:
        _log("  WARNING: employer_id not found; cannot aggregate")
        return pd.DataFrame()

    # Keep only recent 36 months worth of cases by fiscal year
    if "fiscal_year" in df_scored.columns:
        fy = pd.to_numeric(df_scored["fiscal_year"], errors="coerce")
        max_fy = fy.max()
        recent = df_scored[fy >= (max_fy - 2)]  # ≈36 months (3 FY)
    else:
        recent = df_scored

    agg = (
        recent.groupby("employer_id")
        .agg(
            n_cases_36m=("_prob", "count"),
            avg_calibrated_prob=("_prob", "mean"),
            median_calibrated_prob=("_prob", "median"),
        )
        .reset_index()
    )

    # Apply minimum filing threshold
    agg = agg[agg["n_cases_36m"] >= MIN_CASES_36M].copy()
    _log(f"  Employers with n_cases_36m >= {MIN_CASES_36M}: {len(agg):,}")

    # Monotone rescale avg_calibrated_prob → 0-100
    p = agg["avg_calibrated_prob"]
    p_min, p_max = p.min(), p.max()
    if p_max > p_min:
        agg["efs_ml"] = ((p - p_min) / (p_max - p_min) * 100).round(1)
    else:
        agg["efs_ml"] = 50.0

    agg["scope"] = "overall"
    agg["version"] = MODEL_VERSION
    agg["last_refreshed_at"] = datetime.now(timezone.utc).isoformat()

    return agg


# ── Main Function ─────────────────────────────────────────────────────────────

def fit_employer_score_ml(in_tables: Path, out_tables: Path) -> None:
    """Train ML v2 EFS and write employer_friendliness_scores_ml.parquet."""
    metrics_dir = Path(str(out_tables).replace("/tables", "/metrics"))
    metrics_dir.mkdir(parents=True, exist_ok=True)
    log_path = metrics_dir / "employer_score_ml.log"
    diag_path = metrics_dir / "employer_score_ml_diagnostics.json"

    _log("=" * 70)
    _log("EMPLOYER FRIENDLINESS SCORE (EFS) v2 — ML")
    _log("=" * 70)

    # Check sklearn availability
    try:
        import sklearn  # noqa: F401
        _log(f"  sklearn version: {sklearn.__version__}")
    except ImportError:
        _log("  ERROR: scikit-learn not installed. Run: pip install scikit-learn")
        with open(log_path, "w") as fh:
            fh.write("\n".join(LOG_LINES))
        return

    # ── Load case-level PERM data ───────────────────────────────────────────
    perm_dir = in_tables / "fact_perm"
    perm_single = in_tables / "fact_perm.parquet"
    perm_uc = in_tables / "fact_perm_unique_case"

    df_perm: pd.DataFrame | None = None

    # Prefer unique_case to avoid cross-FY duplicates inflating employer counts
    if perm_uc.exists():
        _log("\n[A] Loading fact_perm_unique_case (deduped cases) …")
        try:
            files = sorted(perm_uc.rglob("*.parquet"))
            chunks = []
            for pf in files:
                chunk = pd.read_parquet(pf)
                for p in pf.parts:
                    if "=" in p:
                        col, val = p.split("=", 1)
                        if col not in chunk.columns:
                            chunk[col] = val
                chunks.append(chunk)
            if chunks:
                df_perm = pd.concat(chunks, ignore_index=True)
                _log(f"  Loaded: {len(df_perm):,} unique cases")
        except Exception as e:
            _log(f"  WARNING: could not load perm_unique_case: {e}")

    if df_perm is None and perm_dir.exists():
        _log("\n[A] Loading fact_perm (partitioned) …")
        files = sorted(perm_dir.rglob("*.parquet"))
        chunks = []
        for pf in files:
            chunk = pd.read_parquet(pf)
            for p in pf.parts:
                if "=" in p:
                    col, val = p.split("=", 1)
                    if col not in chunk.columns:
                        chunk[col] = val
            chunks.append(chunk)
        df_perm = pd.concat(chunks, ignore_index=True)
        _log(f"  Loaded: {len(df_perm):,} rows")

    if df_perm is None or len(df_perm) == 0:
        _log("  ERROR: No PERM data found")
        with open(log_path, "w") as fh:
            fh.write("\n".join(LOG_LINES))
        return

    _log(f"  Columns: {list(df_perm.columns)}")

    # ── Feature engineering ─────────────────────────────────────────────────
    _log("\n[B] Building features …")
    X, y = _build_features(df_perm)
    _log(f"  Feature rows: {len(X):,}  (approval rate: {y.mean():.3f})")
    _log(f"  Feature columns: {list(X.columns)}")

    if len(X) < 1_000:
        _log("  WARNING: fewer than 1,000 labeled cases; ML scores may be unreliable")

    # ── Train model ─────────────────────────────────────────────────────────
    _log("\n[C] Training model …")
    base_model, calibrated, diag = _train_model(X, y)
    diag = _shap_importance(base_model, X, diag)

    # ── Verify correlation ───────────────────────────────────────────────────
    _log("\n[D] Verifying score quality …")
    agg = _aggregate_scores(df_perm, calibrated, X, y)

    if len(agg) == 0:
        _log("  ERROR: Aggregation produced no rows — check PERM columns")
        with open(log_path, "w") as fh:
            fh.write("\n".join(LOG_LINES))
        return

    # Load v1 EFS for correlation comparison
    efs_v1_path = out_tables / "employer_friendliness_scores.parquet"
    if efs_v1_path.exists():
        try:
            df_v1 = pd.read_parquet(efs_v1_path)
            v1_overall = df_v1[df_v1["scope"] == "overall"][["employer_id", "efs"]].dropna()
            merged = agg.merge(v1_overall, on="employer_id", suffixes=("_ml", "_v1"))
            if len(merged) >= 10:
                corr_v1_ml = merged["efs_ml"].corr(merged["efs"])
                diag["corr_efs_ml_vs_v1"] = round(float(corr_v1_ml), 4)
                _log(f"  Corr(EFS_ml, EFS_v1): {corr_v1_ml:.4f}")
                if corr_v1_ml < 0.3:
                    _log("  WARNING: low correlation with v1 — check feature quality")
        except Exception as e:
            _log(f"  WARNING: could not compute v1 correlation: {e}")

    # Check approval_rate_24m if available
    feat_path = in_tables / "employer_features.parquet"
    if feat_path.exists():
        try:
            df_feat = pd.read_parquet(feat_path)
            overall_feat = df_feat[df_feat["scope"] == "overall"][["employer_id", "approval_rate_24m"]].dropna()
            merged2 = agg.merge(overall_feat, on="employer_id")
            if len(merged2) >= 10:
                corr_ar = merged2["efs_ml"].corr(merged2["approval_rate_24m"])
                diag["corr_efs_ml_vs_approval_rate_24m"] = round(float(corr_ar), 4)
                _log(f"  Corr(EFS_ml, approval_rate_24m): {corr_ar:.4f}")
                if corr_ar < 0.55:
                    _log(f"  WARN: Corr(EFS_ml, approval_rate_24m)={corr_ar:.4f} < 0.55 threshold")
        except Exception as e:
            _log(f"  WARNING: could not compute approval_rate correlation: {e}")

    # EFS range check
    out_of_range = ((agg["efs_ml"] < 0) | (agg["efs_ml"] > 100)).sum()
    diag["efs_ml_out_of_range"] = int(out_of_range)
    if out_of_range > 0:
        _log(f"  FAIL: {out_of_range} scores outside [0,100]")
    else:
        _log("  PASS: all EFS_ml in [0,100]")

    _log(f"  EFS_ml stats: mean={agg['efs_ml'].mean():.1f}, "
         f"median={agg['efs_ml'].median():.1f}, "
         f"std={agg['efs_ml'].std():.1f}, "
         f"range=[{agg['efs_ml'].min():.1f},{agg['efs_ml'].max():.1f}]")

    diag["n_employers_scored"] = int(len(agg))
    diag["efs_ml_mean"] = round(float(agg["efs_ml"].mean()), 2)
    diag["efs_ml_median"] = round(float(agg["efs_ml"].median()), 2)

    # ── Write output ─────────────────────────────────────────────────────────
    _log("\n[E] Writing output …")
    out_path = out_tables / "employer_friendliness_scores_ml.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    agg.to_parquet(out_path, index=False, engine="pyarrow")
    _log(f"  Written: {out_path} ({len(agg):,} rows)")

    # Write log & diagnostics
    with open(log_path, "w") as fh:
        fh.write("\n".join(LOG_LINES))
    with open(diag_path, "w") as fh:
        json.dump(diag, fh, indent=2)

    _log(f"  Log: {log_path}")
    _log(f"  Diagnostics: {diag_path}")
    _log("\n✓ EFS ML v2 COMPLETE")
