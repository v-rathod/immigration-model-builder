# Immigration Model Builder — Copilot Context

> **NorthStar Program Codenames** (public-facing names for documentation):
> | Internal | Codename | Repository | Role |
> |----------|----------|------------|------|
> | P1 | **Horizon** | fetch-immigration-data | Data collection — scans the horizon |
> | P2 | **Meridian** | immigration-model-builder (THIS REPO) | Analytics backbone — curates, measures, models |
> | P3 | **Compass** | immigration-insights-app | User experience — guides with insights |
>
> Use P1/P2/P3 in internal code and comments. Use Horizon/Meridian/Compass in public docs (README, reports).

> **START OF EVERY SESSION**:
> 1. Run `python scripts/print_objective_banner.py` to load the Program Objective (P1→P2→P3 scope, quality gates, and agent guidance).
> 2. Read `PROGRESS.md` and `artifacts/metrics/FINAL_SINGLE_REPORT.md` to understand where we left off.
> 3. Then proceed with the user's request.
>
> **Key objective files** (version-controlled, source of truth for program goals):
> - `configs/project_objective_P1_P2_P3.md` — human-readable program objectives
> - `configs/project_objective_P1_P2_P3.yaml` — machine-readable objectives (quality gates, table lists, P3 features)
> - `src/utils/objective_loader.py` — Python loader for the YAML

---

## Project Overview

Parquet-based immigration data pipeline (**NorthStar Meridian**) that transforms raw DOL/DOS/USCIS/BLS data into curated dimension & fact tables, engineered features, and ML model artifacts for a downstream public-facing web app (Compass).

**3-project architecture (NorthStar program):**
```
Horizon  (P1: fetch-immigration-data)   →  raw downloads (PDFs, Excel, CSV)
Meridian (P2: immigration-model-builder)  →  THIS REPO: curate → features → models
Compass  (P3: immigration-insights-app)   →  public web app consuming Meridian artifacts
```

---

## Tech Stack
- **Python**: 3.12 (`/opt/homebrew/opt/python@3.12/bin/python3.12`)
- **Key packages**: pandas ≥2.0, pyarrow ≥12.0, pytest 9.0.2, pdfplumber, openpyxl, pydantic, pyyaml
- **No virtual environment** — uses system Python with pip packages
- **Data format**: Parquet (facts & dims), JSON (model weights), YAML (configs)
- **Execution model**: CLI entrypoints, manual execution only, no schedulers/watchers

---

## Key Paths
| What | Path |
|------|------|
| Project root | `/Users/vrathod1/dev/NorthStar/immigration-model-builder` |
| Raw downloads (Horizon / P1) | `/Users/vrathod1/dev/NorthStar/fetch-immigration-data/downloads` |
| Curated tables | `artifacts/tables/` (~41 parquet files/dirs) |
| Model artifacts | `artifacts/models/` |
| Metrics & reports | `artifacts/metrics/` |
| Tests | `tests/` (449 tests across 20+ files) |
| Pipeline config | `configs/paths.yaml` (data_root + artifacts_root) |
| Schemas | `configs/schemas.yml` |
| Full pipeline | `scripts/build_all.sh` |
| Main report | `artifacts/metrics/FINAL_SINGLE_REPORT.md` |

---

## Current State (Milestone 16 — 2026-02-25)
- **Test pass rate: 99.8%** (449 passed, 0 failed, 1 skipped, 3 deselected)
- **3-tier QA**: Golden snapshot regression (7 tests), data sanity suite (47 tests), pytest-cov (11.4% line coverage)
- **All 3 CRITICAL data quality findings resolved** (PERM columns, LCA aliases, SOC dimension)
- dim_employer.parquet: 243,694 rows (patched from fact_perm)
- dim_soc.parquet: 1,801 codes (1,396 SOC-2018 + 405 SOC-2010 legacy)
- fact_perm/: 1,675,051 rows, 19 FY partitions (FY2008–FY2026); key columns: job_title 99.7%, soc_code_raw 98.1%, naics_code 99.7%
- fact_cutoffs/: 13,915 rows, 280 partitions (visa bulletin cutoff dates 2011–2026)
- fact_lca/: 9,558,695 rows, 19 FY partitions; job_title 100%, naics_code 92.9% (has schema merge error on `fiscal_year` int64 vs dict)
- employer_features.parquet: 70,401 rows, 25 feature columns
- employer_friendliness_scores_ml.parquet: 1,695 rows (ML-based EFS)
- pd_forecasts.parquet: 1,344 rows (56 series × 24 months, exponential-weighted seasonal model)
- worksite_geo_metrics.parquet: 159,627 rows (4 grains incl. city; competitiveness_ratio 79.7%)
- soc_demand_metrics.parquet: 4,241 rows (3 windows × 2 datasets)
- employer_monthly_metrics.parquet: 224,114 rows
- processing_times_trends.parquet: 35 rows (FY2014–FY2025 quarterly USCIS I-485)
- 3 integration tests marked `@pytest.mark.slow_integration` (auto-skipped)
- Known issues documented in "Unable to Fix" section of FINAL_SINGLE_REPORT.md

---

## Pipeline Stages (scripts/build_all.sh)

```bash
# Stage 1: Curate raw data → canonical tables
python3 -m src.curate.run_curate --paths configs/paths.yaml

# Stage 1b: Patch dim_employer (MUST run after curate — see gotchas)
python3 scripts/patch_dim_employer_from_fact_perm.py

# Stage 2: Feature engineering
python3 -m src.features.run_features --paths configs/paths.yaml

# Stage 3: Model training
python3 -m src.models.run_models --paths configs/paths.yaml
```

Other useful commands:
```bash
# Run tests (slow_integration auto-skipped via pytest.ini)
python3 -m pytest tests/ -q

# Run with JUnit XML output
python3 -m pytest tests/ --junitxml=artifacts/metrics/all_tests_final.xml -q

# Run ONLY slow_integration tests (20+ min each — re-runs full curate pipeline)
python3 -m pytest tests/ -m slow_integration -q

# Parse JUnit XML
python3 scripts/_parse_junit.py artifacts/metrics/all_tests_final.xml

# Makefile shortcuts
make curate     # run_curate only
make test       # pytest -q
make clean      # wipe artifacts/
make audit-all  # input + output audits

# Incremental build (detect P1 changes → rebuild affected P2 artifacts)
bash scripts/build_incremental.sh              # plan only
bash scripts/build_incremental.sh --execute    # detect + rebuild
bash scripts/build_incremental.sh --init       # initialize manifest
bash scripts/build_incremental.sh --full       # full rebuild + save

# P1 readiness check (run after any P1 fetch to detect new data)
python3 scripts/check_p1_readiness.py          # full readiness report
python3 scripts/check_p1_readiness.py --fix    # auto-rebuild if changes found
```

---

## Handling New Horizon (P1) Data or New Data Sources

**After any Horizon fetch/update, run this workflow:**
```bash
python3 scripts/check_p1_readiness.py       # 1. Check what changed
bash scripts/build_incremental.sh --execute  # 2. Rebuild affected artifacts
python3 -m pytest tests/ -q                  # 3. Validate
```

**If Horizon adds a completely new data source** (new directory):
1. Add `DATASET_PATTERNS` entry in `src/incremental/change_detector.py`
2. Create builder script: `scripts/build_fact_<name>.py`
3. Add `DEPENDENCY_GRAPH` entry mapping dataset → artifacts + rebuild commands
4. Run builder, add tests, re-init manifest: `bash scripts/build_incremental.sh --init`

**Currently tracked but no Meridian builder yet (future work):**
- `USCIS_H1B_Employer_Hub/` → H1B_EMPLOYER_HUB (14 CSVs, FY2010–FY2023)
- `USCIS_Processing_Times/` → USCIS_PROC_TIMES (processing time snapshots)
- `BLS/` → BLS_CES (Current Employment Statistics JSON)
- `DOL_Record_Layouts/` → DOL_RECORD_LAYOUTS (reference metadata, no artifacts)

---

## Artifact Inventory (artifacts/tables/)

### Dimension Tables
| Table | Rows | Notes |
|-------|------|-------|
| dim_employer.parquet | 243,694 | Patched from fact_perm (build_dim_employer only produces ~19K) |
| dim_soc.parquet | 1,801 | 1,396 SOC-2018 + 405 SOC-2010 legacy codes |
| dim_country.parquet | 249 | ISO 3166-1 countries |
| dim_area.parquet | 587 | BLS area codes for OEWS |
| dim_visa_class.parquet | 6 | EB visa categories |
| dim_visa_ceiling.parquet | 14 | Annual visa allocation limits |

### Fact Tables
| Table | Rows | Notes |
|-------|------|-------|
| fact_perm/ (partitioned) | 1,675,051 | 19 FY partitions, PERM labor certifications; job_title 99.7%, soc_code_raw 98.1%, naics_code 99.7% |
| fact_perm_all.parquet | 1,674,724 | Flat denormalized copy |
| fact_perm_unique_case/ | 1,668,587 | NOT truly deduplicated — 20% dupe case_numbers |
| fact_cutoffs/ (partitioned) | 13,915 | Visa bulletin cutoff dates, 280 partitions |
| fact_cutoffs_all.parquet | 8,315 | Deduplicated presentation copy |
| fact_lca/ (partitioned) | 9,558,695 | H-1B LCA filings; job_title 100%, naics 92.9%; has schema merge error (fiscal_year type mismatch) |
| fact_oews/ (partitioned) | 446,432 | BLS OEWS wage data, 2 year partitions |
| fact_oews.parquet | 446,432 | Flat copy |
| fact_niv_issuance.parquet | 501,033 | DOS nonimmigrant visa issuances |
| fact_visa_issuance.parquet | 28,531 | DOS immigrant visa issuances |
| fact_visa_applications.parquet | 35,759 | DOS visa applications |
| fact_dhs_admissions.parquet | 45 | DHS admissions summary |
| fact_uscis_approvals.parquet | 146 | USCIS form approval counts |
| fact_warn_events.parquet | 985 | WARN Act layoff events |
| fact_waiting_list.parquet | 9 | DOS waiting list summary |

### Feature Tables
| Table | Rows | Notes |
|-------|------|-------|
| employer_features.parquet | 70,401 | 25 columns, 36-month rolling window |
| salary_benchmarks.parquet | 224,047 | SOC × area median & P75 wages |
| employer_monthly_metrics.parquet | 224,114 | Monthly employer aggregates |
| employer_risk_features.parquet | 668 | Risk signals for high-volume employers |
| soc_demand_metrics.parquet | 4,241 | SOC-level demand aggregates (3 windows × 2 datasets) |
| visa_demand_metrics.parquet | 537,735 | Category × country demand metrics |
| worksite_geo_metrics.parquet | 159,627 | Geographic distribution (4 grains incl. city; CR 79.7%) |
| category_movement_metrics.parquet | 8,315 | Visa bulletin movement trends |
| backlog_estimates.parquet | 8,315 | Estimated backlogs by cat × country |
| fact_cutoff_trends.parquet | 8,315 | Cutoff date movement trends |
| processing_times_trends.parquet | 35 | FY2014–FY2025 quarterly USCIS I-485 performance |

### Model Outputs
| Table | Rows | Notes |
|-------|------|-------|
| employer_friendliness_scores.parquet | 70,401 | Rules-based EFS (0–100) |
| employer_friendliness_scores_ml.parquet | 1,695 | ML-based EFS for top employers |
| employer_scores.parquet | 0 | Legacy stub (superseded by EFS) |
| pd_forecasts.parquet | 1,344 | 56 series × 24 months (exponential-weighted seasonal model) |
| oews_wages.parquet | 0 | Legacy stub (data is in fact_oews) |
| visa_bulletin.parquet | 0 | Legacy stub (data is in fact_cutoffs) |

### Stub / Empty Tables (Expected)
- fact_trac_adjudications.parquet (0 rows — TRAC requires subscription)
- fact_acs_wages.parquet (0 rows — Census API HTTP 404)

---

## Test Architecture

### pytest.ini Configuration
```ini
markers = slow_integration: runs full curate pipeline (20+ min)
addopts = -m "not slow_integration"
```

### Test Directory Structure
```
tests/
├── test_smoke.py                          # 1 test — basic imports
├── test_fact_perm.py                      # 8 tests — fact_perm schema, PK, partitions
├── test_fact_cutoffs.py                   # 3 tests (1 slow_integration)
├── test_dim_soc.py                        # 4 tests (1 slow_integration)
├── test_dim_country.py                    # 2 tests (1 slow_integration)
├── test_dim_employer.py                   # shape/schema tests
├── test_dim_area.py, test_dim_visa_class.py
├── test_fact_oews.py                      # OEWS schema tests
├── test_dry_run.py, test_paths_check.py   # pipeline infra tests
├── test_coverage_expectations.py          # expected file counts
├── datasets/                              # 6 files, ~115 tests
│   ├── test_schema_and_pk_core.py         # Schema + PK for core tables
│   ├── test_schema_and_pk_new.py          # Schema + PK for P2 gap tables
│   ├── test_referential_integrity.py      # FK join rates (employer_id, soc_code, country)
│   ├── test_coverage_files.py             # Row count ≥ file count × threshold
│   ├── test_value_ranges_and_continuity.py # Non-negative, backlog caps, FY spans
│   └── test_remaining_artifacts.py        # efs_ml, fact_perm_unique_case, 4 stubs
├── models/                                # 2 files, ~36 tests
│   ├── test_integration_e2e_sanity.py     # E2E integration checks
│   └── test_model_usage_matrix.py         # Usage registry validation
├── p2_hardening/                          # 2 files, ~94 tests
│   ├── test_schema_and_pk.py              # Comprehensive schema/PK for all P2 artifacts
│   └── test_ranges_and_integrity.py       # RI, ranges, statistical checks
├── p2_gap_curation/                       # Gap table tests
└── p3_metrics/                            # 6 files — metric table tests
    ├── test_category_movement_metrics.py
    ├── test_employer_monthly_metrics.py
    ├── test_fact_cutoff_trends.py
    ├── test_salary_benchmarks.py
    ├── test_soc_demand_metrics.py
    └── test_worksite_geo_metrics.py
```

### Key Test Thresholds
| Dataset | Metric | Threshold | Rationale |
|---------|--------|-----------|-----------|
| dim_employer | Row count | ≥ 60,000 | Requires fact_perm patch |
| employer_features | employer_id coverage | ≥ 40% | Must match dim_employer |
| DOS visa tables | Country RI | 50–70% | DOS/FAO naming ≠ ISO-3166 |
| soc_demand_metrics | SOC RI | 80% | Legacy SOC-2000/2010 codes |
| fact_perm_unique_case | PK uniqueness | ≥ 70% | Multi-year refilings |
| TRAC / ACS | Row count | 0 acceptable | Stubs, no data source |

---

## Critical Gotchas / Traps

1. **dim_employer gets overwritten**: `build_dim_employer.py` only reads 2 FYs × 50K rows → produces ~19K rows. MUST run `scripts/patch_dim_employer_from_fact_perm.py` after curate. This is now in `build_all.sh` as Stage 1b.

2. **3 slow_integration tests re-run the full curate pipeline** (~20+ min each via subprocess). They are `test_dim_soc_builder_creates_file`, `test_dim_country_builder_creates_file`, `test_fact_cutoffs_loader_creates_directory`. These will also **overwrite dim_employer** back to 19K rows. They are auto-skipped via pytest.ini.

3. **fact_lca/ has a schema merge error**: `pd.read_parquet('artifacts/tables/fact_lca')` fails with "Field fiscal_year has incompatible types: int64 vs dictionary". Individual partition files can be read separately.

4. **fact_perm_unique_case is NOT deduplicated**: Despite the name, it has ~20% duplicate case_numbers (multi-year refilings across FY disclosure files).

5. **case_status values have mixed casing**: Raw DOL data uses both uppercase ('CERTIFIED') and title-case ('Certified') across fiscal years. Tests normalize to uppercase.

6. **Legacy stub tables are expected to be empty**: employer_scores, oews_wages, visa_bulletin all have 0 rows — superseded by other artifacts. (pd_forecasts is no longer a stub as of M9.)

7. **OEWS 2024 is synthetic**: Official file got HTTP 403; using a fallback derived from 2023 data for coverage.

8. **conftest.py activates chat_tap**: The root conftest.py imports `src.utils.chat_tap` for commentary capture. This is non-blocking (wrapped in try/except).

9. **Incremental builds use file manifests**: `artifacts/metrics/p1_manifest.json` stores fingerprints (size + mtime) for 1,196 Horizon (P1) files across 18 datasets. Run `bash scripts/build_incremental.sh --init` to reset. New datasets need their directory pattern added to `DATASET_PATTERNS` and dependencies registered in `DEPENDENCY_GRAPH` in `src/incremental/change_detector.py`. The `classify_dataset()` function uses longest-match-wins to avoid collisions (e.g. `DOL_Record_Layouts/LCA/` matches DOL_RECORD_LAYOUTS, not LCA).

10. **Horizon readiness check before pipeline runs**: Always run `python3 scripts/check_p1_readiness.py` after any Horizon fetch/update. It reports: (a) all Horizon files classified by dataset, (b) any UNKNOWN files needing DATASET_PATTERNS entries, (c) which datasets have Meridian builders vs which are future work, (d) change detection vs saved manifest. Use `--fix` to auto-rebuild.

11. **4 Horizon datasets tracked but have no Meridian builder yet**: H1B_EMPLOYER_HUB (14 CSVs), USCIS_PROC_TIMES (2 files), BLS_CES (4 JSONs), DOL_RECORD_LAYOUTS (15 PDFs, metadata only). These are classified and their changes are detected, but no artifacts are built. When builders are created, add dependency graph entries.

---

## Source Code Architecture

```
src/
├── curate/                                # Raw → canonical parsers
│   ├── run_curate.py                      # CLI orchestrator (5 dims + 3 facts + post-build expansion)
│   ├── build_dim_employer.py              # Employer dim (limited: 2 FY × 50K rows)
│   ├── build_dim_soc.py                   # SOC-2018 dim with crosswalks
│   ├── build_dim_country.py               # ISO-3166 country dim
│   ├── build_dim_area.py                  # BLS area codes
│   ├── build_dim_visa_class.py            # EB visa categories
│   ├── build_fact_perm.py                 # PERM Excel → partitioned fact
│   ├── build_fact_oews.py                 # BLS OEWS wages
│   ├── visa_bulletin_loader.py            # PDF → fact_cutoffs (pdfplumber)
│   ├── lca_loader.py                      # H-1B LCA Excel parser
│   ├── perm_loader.py                     # PERM record layout parser
│   └── oews_loader.py                     # OEWS Excel parser
├── features/
│   ├── run_features.py                    # CLI orchestrator
│   ├── employer_features.py               # PERM aggregates → employer features (25 cols)
│   └── salary_benchmarks.py               # OEWS + PERM → salary percentiles
├── models/
│   ├── run_models.py                      # CLI orchestrator
│   ├── employer_score.py                  # Rules-based EFS (0–100)
│   ├── employer_score_ml.py               # ML-based EFS (logistic regression)
│   └── pd_forecast.py                     # Priority date forecast (exp-weighted seasonal, 56 series)
├── utils/
│   ├── usage_registry.py                  # Task/artifact usage tracking
│   ├── chat_tap.py                        # Commentary capture for pytest
│   └── transcript.py                      # Transcript utilities
├── incremental/
│   └── change_detector.py                 # Manifest-based P1 change detection & rebuild planning
├── io/readers.py                          # Config loading, path resolution
├── normalize/mappings.py                  # SOC crosswalks, employer normalization
├── validate/dq_checks.py                  # Data quality check helpers
└── export/package_artifacts.py            # Bundle packaging for Compass (P3)
```

### Key Scripts (scripts/)
| Script | Purpose |
|--------|---------|
| `build_all.sh` | Full pipeline: curate → patch → features → models |
| `build_incremental.sh` | Incremental build: detect P1 changes → rebuild affected artifacts |
| `check_p1_readiness.py` | P1 readiness audit: classify files, detect changes, report coverage gaps |
| `patch_dim_employer_from_fact_perm.py` | Expand dim_employer from all fact_perm partitions |
| `patch_fact_perm_wages.py` | Fix wage columns in fact_perm |
| `rebuild_employer_features.py` | Rebuild employer_features.parquet |
| `expand_dim_soc_legacy.py` | Add legacy SOC codes to dim_soc |
| `run_full_qa.py` | Comprehensive QA runner |
| `generate_final_report.py` | Generate FINAL_SINGLE_REPORT.md |
| `append_tests_and_usage_to_report.py` | Append test results to report |
| `_parse_junit.py` | Parse JUnit XML test output |
| `fix1_perm_reconcile.py` through `fix5_oews_robustness.py` | Data fix scripts |
| `make_*.py` | Build derived metric tables |
| `build_fact_*.py` | Build P2 gap fact tables |
| `audit_*.py` | Input/output audit runners |

---

## Editorial & Formatting Rules

When editing documentation files in this project:
- Use **Markdown tables** with aligned pipes for data summaries
- Include row counts with commas (e.g., `227,076`)
- Always include `Generated: YYYY-MM-DDTHH:MM:SSZ` timestamps in reports
- Test results format: `X passed, Y failed, Z skipped, W deselected`
- Pass rates as percentages with 1 decimal: `99.4%`
- Parquet file references include row count and column count
- Reference scripts with backtick-wrapped paths: `scripts/patch_dim_employer_from_fact_perm.py`
- PROGRESS.md entries should include: date, objective, work performed, files modified, current state
- FINAL_SINGLE_REPORT.md updates should maintain section numbering consistency

---

## Important Files to Read for Full Context
1. **`PROGRESS.md`** — Full milestone history (Milestones 1–12), ~2,100 lines
2. **`artifacts/metrics/FINAL_SINGLE_REPORT.md`** — Comprehensive data quality report, ~1,230 lines
3. **`pytest.ini`** — Test configuration with slow_integration marker
4. **`scripts/build_all.sh`** — Full pipeline with dim_employer patch
5. **`conftest.py`** — Root pytest config (chat_tap activation)
6. **`configs/paths.yaml`** — Data root and artifacts root paths
7. **`.copilot-context.md`** — Legacy context file (outdated — use this file instead)
8. **`src/incremental/change_detector.py`** — Incremental change detection engine (manifest, dependency graph, rebuild planner)
9. **`scripts/build_incremental.sh`** — Incremental build shell wrapper
10. **`scripts/check_p1_readiness.py`** — P1 readiness audit script (run after any P1 fetch/update)
