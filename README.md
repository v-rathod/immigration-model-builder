# NorthStar · Meridian

> **Immigration Model Builder** — the analytical backbone of the NorthStar program

> **📋 For AI Assistants**: 
> - Read [`.github/copilot-instructions.md`](.github/copilot-instructions.md) first — it's the authoritative context file (auto-loaded by Copilot)
> - Review [`PROGRESS.md`](PROGRESS.md) for chronological work history (Milestones 1–12)

## The NorthStar Program

NorthStar is a three-part immigration data intelligence platform:

| Codename | Role | Repository |
|----------|------|------------|
| **Horizon** | Data collection — scans the horizon and gathers raw immigration filings, bulletins, and statistics | `fetch-immigration-data` |
| **Meridian** | Modeling & analytics — the reference framework that curates, measures, and models ← **THIS REPO** | `immigration-model-builder` |
| **Compass** | User experience — the instrument that guides users with personalized immigration insights | `immigration-insights-app` |

## Purpose

**Meridian** transforms raw data collected by **Horizon** into curated tables, feature sets, and lightweight predictive models for consumption by **Compass** (the public web app).

**Key outputs:**
- Canonical fact and dimension tables (Visa Bulletin history, PERM/LCA records, OEWS wages)
- Engineered features (employer friendliness, salary benchmarks)
- Model artifacts (priority-date forecasts, retrogression risk scores)
- Packaged exports ready for Compass

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    NorthStar Program                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   ★ Horizon  (fetch-immigration-data)                       │
│   │  Scans & collects raw data: PDFs, Excel, CSV            │
│   │  /Users/vrathod1/dev/NorthStar/fetch-immigration-data   │
│   │                                                         │
│   ▼                                                         │
│   ★ Meridian (immigration-model-builder) ← THIS REPO       │
│   │  Curates, engineers features, trains models              │
│   │  Writes artifacts to: ./artifacts/                      │
│   │                                                         │
│   ▼                                                         │
│   ★ Compass  (immigration-insights-app)                     │
│      Guides users with personalized immigration insights    │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Local Data Path

**Input (from Horizon):** `/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads`

This directory contains:
- Visa Bulletin PDFs
- DOS Visa Annual/Monthly statistics
- USCIS employment data
- DOL PERM/LCA disclosures + record layouts
- BLS OEWS wage data
- USCIS Processing Times
- DOS Waiting List reports
- Codebooks

**Output:** `./artifacts/`

## Setup

```bash
# Uses system Python 3.12 (no venv required)
pip install -r requirements.txt
```

## Run Order

```bash
# Full pipeline (recommended)
bash scripts/build_all.sh

# Or stage by stage:
python3 -m src.curate.run_curate --paths configs/paths.yaml   # Stage 1: Curate
python3 scripts/patch_dim_employer_from_fact_perm.py           # Stage 1b: Patch dim_employer (CRITICAL)
python3 -m src.features.run_features --paths configs/paths.yaml # Stage 2: Features
python3 -m src.models.run_models --paths configs/paths.yaml     # Stage 3: Models

# Incremental build (detect P1 changes → rebuild only affected artifacts)
bash scripts/build_incremental.sh              # plan only
bash scripts/build_incremental.sh --execute    # detect + rebuild
bash scripts/build_incremental.sh --init       # initialize/reset manifest

# P1 readiness check (run after any P1 fetch/update)
python3 scripts/check_p1_readiness.py          # full readiness report
python3 scripts/check_p1_readiness.py --fix    # auto-rebuild if changes found

# Tests
python3 -m pytest tests/ -q                    # 345 passed, 1 skipped, 3 deselected
```

## Handling New Horizon Data

When Horizon fetches new files or adds a new data source, run this workflow:

```bash
# Step 1: Check what changed
python3 scripts/check_p1_readiness.py

# Step 2: If changes detected, rebuild affected artifacts only
bash scripts/build_incremental.sh --execute

# Step 3: Run tests to validate
python3 -m pytest tests/ -q
```

**If Horizon adds a completely new data source** (new directory in downloads/):

1. Add a `DATASET_PATTERNS` entry in [src/incremental/change_detector.py](src/incremental/change_detector.py)
2. Create a curate script: `scripts/build_fact_<name>.py`
3. Add a `DEPENDENCY_GRAPH` entry mapping the dataset → artifacts + rebuild commands
4. Run the builder: `python3 scripts/build_fact_<name>.py`
5. Add validation tests in `tests/`
6. Re-init manifest: `bash scripts/build_incremental.sh --init`

See `scripts/check_p1_readiness.py` output section 3 for which datasets currently have Meridian builders and which are tracked but pending.

## Current State (Milestone 12)

- **41 artifacts** — 17.4M total rows across dims, facts, features, model outputs
- **All 7 P3 features backed** — pd_forecasts, employer scores, geo metrics, salary benchmarks, etc.
- **PD Forecast v2.1** — full-history anchored, velocity-capped, cross-verified within ±18% of 10-year actual
- **EFS dual models** — rules-based (70K employers) + ML gradient boosting (956 high-volume)
- **Incremental builds** — manifest-based change detection for P1 data (1,197 files tracked)
- **99.7% test pass rate** — 345 passed, 0 failed, 1 skipped, 3 deselected

See `PROGRESS.md` for full milestone history.

---

## Complete Artifact Inventory

> **41 artifacts · 17,404,766 total rows · Generated 2026-02-24**

### Dimension Tables (6 tables · 229,328 rows)

| Artifact | Rows | Cols | Data Source(s) | P3 Usage |
|----------|-----:|-----:|----------------|----------|
| dim_employer.parquet | 227,076 | 6 | DOL PERM Excel FY2005–FY2024 (patched from all 20 FY partitions) | Compass: Employer lookup, EFS dashboard, search |
| dim_soc.parquet | 1,396 | 12 | BLS OEWS 2023 all-data; SOC 2010→2018 crosswalk CSV | SOC occupation lookup, demand dashboard |
| dim_country.parquet | 249 | 6 | Hardcoded ISO 3166-1 (249 countries) | Country lookup, visa bulletin filter |
| dim_area.parquet | 587 | 10 | BLS OEWS all-data (2024 w/ 2023 fallback) | Geography lookup, worksite dashboard |
| dim_visa_class.parquet | 6 | 9 | EB subcategory codebook CSV | Visa category labels for UI |
| dim_visa_ceiling.parquet | 14 | 6 | DOS Numerical Limits PDF + hardcoded fallback | Annual visa limits context, backlog estimates |

### Fact Tables (15 tables · 16,037,136 rows)

| Artifact | Rows | Cols | Data Source(s) | P3 Usage |
|----------|-----:|-----:|----------------|----------|
| fact_perm/ (partitioned) | 1,675,051 | 20 | DOL PERM Excel FY2005–FY2024 (20 files) | EFS input, employer features, approval rates, wage analysis |
| fact_perm_all.parquet | 1,674,724 | 19 | ← fact_perm/ (flat concatenation) | Quick-query denormalized copy |
| fact_perm_unique_case/ | 1,671,899 | 20 | ← fact_perm/ (deduped by case_number, latest decision wins) | ML EFS training, case-level analysis (note: ~20% multi-year dupes) |
| fact_cutoffs/ (partitioned) | 13,915 | 10 | DOS Visa Bulletin PDFs (~180 PDFs, 2011–2026) | PD forecast model input, visa bulletin dashboard |
| fact_cutoffs_all.parquet | 8,315 | 10 | ← fact_cutoffs/ (deduplicated presentation copy) | Cutoff trends, movement analysis |
| fact_lca/ (partitioned) | 9,558,695 | 23 | DOL H-1B LCA disclosures FY2008–FY2026 (Excel/CSV) | H-1B filing analysis, employer H-1B volume |
| fact_oews/ (partitioned) | 446,432 | 18 | BLS OEWS all-data 2022–2024 | Salary benchmarks, prevailing wage context |
| fact_oews.parquet | 446,432 | 20 | ← fact_oews/ (flat copy) | Quick-query denormalized copy |
| fact_niv_issuance.parquet | 501,033 | 6 | DOS NIV Statistics XLS/XLSX (~32 files) | Nonimmigrant visa issuance trends |
| fact_visa_issuance.parquet | 28,531 | 6 | DOS Visa Annual Report PDFs (~274 PDFs, FY2015–FY2024) | Immigrant visa issuance trends |
| fact_visa_applications.parquet | 35,759 | 8 | DOS Visa Statistics PDFs (~198 PDFs) | Visa demand metrics, application trends |
| fact_dhs_admissions.parquet | 45 | 6 | DHS Yearbook XLSX (refugee arrivals FY1980–FY2024) | Immigration volume trends |
| fact_uscis_approvals.parquet | 146 | 7 | USCIS form performance data (~245 files: XLSX + CSV) | USCIS processing trends, approval volumes |
| fact_warn_events.parquet | 985 | 8 | WARN Act XLSX (CA + TX notices) | Employer risk signals, layoff events |
| fact_waiting_list.parquet | 9 | 6 | DOS Waiting List CSV + PDF (2023) | Visa demand context |

### Fact Stubs (2 tables · 0 rows)

| Artifact | Rows | Data Source(s) | Status |
|----------|-----:|----------------|--------|
| fact_trac_adjudications.parquet | 0 | TRAC FOIA CSVs | Stub — TRAC requires paid subscription |
| fact_acs_wages.parquet | 0 | Census ACS API + PUMS CSVs | Stub — ACS API returned HTTP 404 |

### Feature Tables (11 tables · 1,040,905 rows)

| Artifact | Rows | Cols | Data Source(s) | P3 Usage |
|----------|-----:|-----:|----------------|----------|
| employer_features.parquet | 70,206 | 25 | ← fact_perm/, dim_employer, fact_oews/, dim_area | EFS model input, employer analytics |
| salary_benchmarks.parquet | 224,047 | 7 | ← fact_oews/, dim_area, dim_soc | Salary comparison dashboard, wage context |
| employer_monthly_metrics.parquet | 74,350 | 10 | ← fact_perm/, dim_employer | Employer trend charts, monthly filing volume |
| employer_risk_features.parquet | 668 | 7 | ← fact_warn_events, dim_employer | Employer risk signals panel |
| soc_demand_metrics.parquet | 3,968 | 10 | ← fact_perm/, fact_lca/, dim_soc | Occupation demand dashboard, SOC trends |
| visa_demand_metrics.parquet | 537,735 | 5 | ← fact_visa_issuance, fact_visa_applications, fact_niv_issuance | Visa demand by category × country |
| worksite_geo_metrics.parquet | 104,951 | 13 | ← fact_perm/, fact_lca/, dim_area, fact_oews/ | Geo distribution dashboard, worksite heatmap |
| category_movement_metrics.parquet | 8,315 | 10 | ← fact_cutoff_trends | Visa bulletin movement trends |
| backlog_estimates.parquet | 8,315 | 8 | ← fact_cutoff_trends, fact_perm/ | Backlog context, wait time estimates |
| fact_cutoff_trends.parquet | 8,315 | 14 | ← fact_cutoffs_all (or fact_cutoffs/ fallback) | PD forecast input, cutoff movement analysis |
| processing_times_trends.parquet | 35 | 20 | USCIS I-485 quarterly performance data (FY2014–FY2025) | Processing times dashboard, I-485 trends |

### Model Outputs (3 tables + 1 JSON · 72,506 rows + 56 series)

| Artifact | Rows | Cols | Data Source(s) | P3 Usage |
|----------|-----:|-----:|----------------|----------|
| employer_friendliness_scores.parquet | 70,206 | 22 | ← employer_features | **Compass Panel D**: EFS dashboard, employer search, tier labels |
| employer_friendliness_scores_ml.parquet | 956 | 8 | ← fact_perm_unique_case/, employer_friendliness_scores | ML EFS for top employers, enhanced accuracy |
| pd_forecasts.parquet | 1,344 | 10 | ← fact_cutoff_trends | **Compass Panel A**: PD forecast — the #1 feature |
| pd_forecast_model.json | 56 series | — | ← fact_cutoff_trends | Model parameters & metadata for Compass display |

### Model Stubs (3 tables · 0 rows)

| Artifact | Rows | Status |
|----------|-----:|--------|
| employer_scores.parquet | 0 | Legacy — superseded by EFS |
| oews_wages.parquet | 0 | Legacy — data is in fact_oews |
| visa_bulletin.parquet | 0 | Legacy — data is in fact_cutoffs |

---

### Raw Data Source Summary (Horizon → Meridian)

| Horizon Source Directory | File Types | Count | Meridian Artifacts Fed |
|---------------------|-----------|------:|------------------|
| `PERM/PERM/FY*/` | Excel | 20 | fact_perm → dim_employer, employer_features, EFS, soc_demand, worksite_geo, backlog |
| `Visa_Bulletin/` | PDF | ~180 | fact_cutoffs → cutoff_trends → pd_forecasts, category_movement, backlog |
| `LCA/FY*/` | Excel/CSV | ~19 | fact_lca → soc_demand, worksite_geo |
| `BLS_OEWS/{year}/` | Excel/ZIP | 3 | dim_soc, dim_area, fact_oews → salary_benchmarks, employer_features, worksite_geo |
| `NIV_Statistics/` | XLS/XLSX | ~32 | fact_niv_issuance → visa_demand_metrics |
| `Visa_Annual_Reports/` | PDF | ~274 | fact_visa_issuance → visa_demand_metrics |
| `Visa_Statistics/` | PDF | ~198 | fact_visa_applications → visa_demand_metrics |
| `USCIS_IMMIGRATION/` | XLSX/CSV | ~245 | fact_uscis_approvals, processing_times_trends |
| `DHS_Yearbook/` | XLSX | 1 | fact_dhs_admissions |
| `WARN/{CA,TX}/` | XLSX | 2 | fact_warn_events → employer_risk_features |
| `DOS_Waiting_List/` | CSV+PDF | 2 | fact_waiting_list |
| `DOS_Numerical_Limits/` | PDF | 1 | dim_visa_ceiling |
| `Codebooks/` | CSV | 2 | dim_soc (crosswalk), dim_visa_class |
| *(hardcoded)* | Python | — | dim_country (ISO 3166-1) |

### Data Lineage Flow (Horizon → Meridian → Compass)

```
★ Horizon — Raw Downloads (PDFs, Excel, CSV)
  │
  ├──→ ★ Meridian Stage 1: Curate      → 6 dims + 15 facts + 2 stubs
  │       │
  │       └──→ Stage 1b: Patch dim_employer (from fact_perm)
  │
  ├──→ ★ Meridian Stage 2: Features     → 11 feature tables (1M+ rows)
  │       │
  │       ├── employer_features (PERM + OEWS)
  │       ├── salary_benchmarks (OEWS)
  │       ├── worksite_geo_metrics (PERM + LCA + OEWS)
  │       ├── soc_demand_metrics (PERM + LCA)
  │       ├── visa_demand_metrics (DOS issuance tables)
  │       ├── category_movement_metrics (visa bulletin)
  │       ├── backlog_estimates (visa bulletin + PERM)
  │       └── processing_times_trends (USCIS I-485)
  │
  └──→ ★ Meridian Stage 3: Models       → 3 model outputs + 1 JSON + 3 stubs
          ├── pd_forecasts          (Compass Panel A — #1 feature)
          ├── EFS rules-based       (Compass Panel D — 70K employers)
          └── EFS ML-based          (956 high-volume employers)
                                              │
                                              ▼
                                    ★ Compass — User-Facing App
```

---

## Model Architecture

### Priority Date Forecast (v2.1)

Predicts future visa bulletin cutoff dates for 56 series (EB1–EB5 × CHN/IND/MEX/PHL/ROW × FAD/DFF).

- **Velocity blend**: 50% full-history net velocity + 25% capped 24-month rolling + 25% capped 12-month rolling
- **Velocity cap**: `max(1.25 × long_term, long_term + 5 d/mo)` — prevents recent recovery from dominating
- **Seasonal adjustment**: P5/P95-trimmed monthly factors (12 bins, October visa year anchor)
- **Confidence intervals**: IQR-based bands from historical variance
- **Cross-verified**: All series within ±18% of 10-year actual velocity

### Employer Friendliness Score (Rules-based)

Scores 70,206 employers on immigration-friendliness (0–100 scale).

- **50% Outcome**: Bayesian-shrunk approval rate (prior 0.88, strength 20)
- **30% Wage**: Linear map of median wage ratio (0.5→0, 1.0→75, 1.3→100)
- **20% Sustainability**: months_active + volume + trend + low_volatility blend
- **Tiers**: Excellent ≥85, Good ≥70, Moderate ≥50, Below Average ≥30, Poor <30

### Employer Friendliness Score (ML)

Enhanced scoring for 956 high-volume employers (≥15 cases in 36 months).

- **Algorithm**: HistGradientBoostingClassifier with isotonic calibration
- **Features**: wage_level, wage_ratio, soc_major, fy_offset, emp_log_vol, country flags
- **Validation**: 5-fold stratified cross-validation

---

## Tech Stack

- Python 3.12 (system)
- pandas ≥2.0, pyarrow ≥12.0, pytest 9.0.2
- pdfplumber, openpyxl, pydantic, pyyaml

## Future Chat/Q&A Support

Meridian's architecture reserves space for future Q&A capabilities that may be introduced in Compass:

- **`artifacts/qa/`**: Reserved directory for RAG-ready outputs (document summaries, metadata, embeddings)
- **`configs/qa.yml`**: Configuration placeholder for embedding models, chunking strategies, and source selection
- **Purpose**: When Compass adds a chat interface, Meridian can optionally generate pre-computed bundles to support retrieval-augmented generation without re-parsing raw data

**Current status**: No implementation required. This scaffolding ensures future Compass chat features can integrate cleanly without architectural changes.

## License

MIT
