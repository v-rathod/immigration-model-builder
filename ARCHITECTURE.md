# Meridian (P2) — Technical Architecture

> **Project:** immigration-model-builder  
> **Role:** Analytics backbone — curates, engineers features, trains models  
> **Last Updated:** March 5, 2026

---

## Prerequisite Reading

Before working on this project, read these documents in order:

1. **`/Users/vrathod1/dev/NorthStar/NORTHSTAR_VISION.md`** — Program vision, 3-project architecture, guardrails
2. **`.github/copilot-instructions.md`** — Detailed P2 context (414 lines): pipeline stages, schemas, gotchas, test thresholds
3. **`.copilot-context.md`** — Supplementary context with module organization
4. **`PROGRESS.md`** — Session-by-session work history
5. **`artifacts/metrics/FINAL_SINGLE_REPORT.md`** — Current artifact inventory, row counts, data quality

---

## Architecture Overview

```
┌──────────────────────────────────────────────────────────────────────┐
│                P1 Horizon: downloads/ (read-only)                    │
│  PDFs, Excel, CSV from 15 government sources (~3-5 GB)              │
└──────────────────────────┬───────────────────────────────────────────┘
                           │ reads directly (never copies)
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                    STAGE 1: CURATE (run_curate.py)                   │
│                                                                      │
│  Raw file parsers:                     Canonical outputs:            │
│  ┌──────────────────────┐             ┌──────────────────────┐      │
│  │ visa_bulletin_loader │────────────▶│ fact_cutoffs         │      │
│  │ perm_loader          │────────────▶│ fact_perm (1.7M)     │      │
│  │ lca_loader           │────────────▶│ fact_lca (9.6M)      │      │
│  │ oews_loader          │────────────▶│ fact_oews (446K)     │      │
│  │ build_dim_*          │────────────▶│ dim_* (6 tables)     │      │
│  └──────────────────────┘             └──────────────────────┘      │
│                                                                      │
│  Additional builders (Stage 1b-1c):                                  │
│  ┌──────────────────────────────┐     ┌──────────────────────┐      │
│  │ patch_dim_employer           │────▶│ dim_employer (243K)  │      │
│  │ expand_dim_soc_legacy        │────▶│ dim_soc (1,801)      │      │
│  │ build_fact_h1b_employer_hub  │────▶│ fact_h1b_employer_hub│      │
│  │ build_fact_bls_ces           │────▶│ fact_bls_ces         │      │
│  │ build_fact_iv_post           │────▶│ fact_iv_post (163K)  │      │
│  │ build_fact_uscis_approvals   │────▶│ fact_uscis_approvals │      │
│  └──────────────────────────────┘     └──────────────────────┘      │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                  STAGE 2: FEATURES (run_features.py)                 │
│                                                                      │
│  Feature engineers:                    Feature outputs:              │
│  ┌──────────────────────────┐         ┌──────────────────────────┐  │
│  │ employer_features        │────────▶│ employer_features        │  │
│  │ employer_monthly_metrics │────────▶│ employer_monthly_metrics │  │
│  │ employer_risk_features   │────────▶│ employer_risk_features   │  │
│  │ salary_benchmarks        │────────▶│ salary_benchmarks        │  │
│  │ worksite_geo_metrics     │────────▶│ worksite_geo_metrics     │  │
│  │ soc_demand_metrics       │────────▶│ soc_demand_metrics       │  │
│  │ visa_demand_metrics      │────────▶│ visa_demand_metrics      │  │
│  │ backlog_estimates        │────────▶│ backlog_estimates        │  │
│  │ category_movement        │────────▶│ category_movement_metrics│  │
│  │ processing_times_trends  │────────▶│ processing_times_trends  │  │
│  └──────────────────────────┘         └──────────────────────────┘  │
│                                                                      │
│  Stage 2b (salary profiles):                                         │
│  ┌──────────────────────────┐         ┌──────────────────────────┐  │
│  │make_employer_salary_profs│────────▶│ employer_salary_profiles │  │
│  │                          │────────▶│ employer_salary_yearly   │  │
│  │                          │────────▶│ soc_salary_market        │  │
│  └──────────────────────────┘         └──────────────────────────┘  │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   STAGE 3: MODELS (run_models.py)                    │
│                                                                      │
│  Model trainers:                       Model outputs:               │
│  ┌──────────────────────────┐         ┌──────────────────────────┐  │
│  │ pd_forecast              │────────▶│ pd_forecasts.json        │  │
│  │                          │────────▶│ pd_forecast_model.json   │  │
│  │ employer_score (rules)   │────────▶│ employer_friendliness_   │  │
│  │ employer_score (ML)      │────────▶│   scores.parquet (70K)   │  │
│  │                          │────────▶│ employer_friendliness_   │  │
│  │                          │         │   scores_ml.parquet      │  │
│  └──────────────────────────┘         └──────────────────────────┘  │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────────────────────────────────────┐
│                   STAGE 4: EXPORT (rag_builder + qa_generator)       │
│                                                                      │
│  ┌──────────────────────────┐         ┌──────────────────────────┐  │
│  │ rag_builder.py           │────────▶│ all_chunks.json (341)    │  │
│  │ (reads 36 source tables) │         │ catalog.json (49 items)  │  │
│  │                          │         │ build_summary.json       │  │
│  │ qa_generator.py          │────────▶│ qa_cache.json (684)      │  │
│  │ (10 topic categories)    │         │                          │  │
│  └──────────────────────────┘         └──────────────────────────┘  │
└──────────────────────────┬───────────────────────────────────────────┘
                           │
                           ▼
                  P3 Compass reads via sync_p2_data.py
                  (Parquet → JSON conversion)
```

---

## Artifact Inventory Summary

| Category | Count | Examples |
|----------|-------|---------|
| Dimensions | 6 | dim_employer (243K), dim_soc (1,801), dim_country (249) |
| Fact tables | 18 | fact_perm (1.7M), fact_lca (9.6M), fact_oews (446K) |
| Feature tables | 14 | employer_features, salary_benchmarks, soc_demand_metrics |
| Model outputs | 3 | pd_forecasts, employer_friendliness_scores, salary_profiles |
| RAG artifacts | 4 | 341 chunks, 684 QA pairs, catalog, build summary |
| **Total rows** | **18.5M+** | |

---

## P2 → P3 Data Contract

When modifying P2 artifacts, these rules are **non-negotiable**:

### DO NOT:
- Rename columns consumed by P3 (check P3's `src/types/p2-artifacts.ts`)
- Change primary key structure of dimension tables
- Remove or rename RAG topic names (10 topics)
- Change QA/chunk JSON schema

### DO:
- Add corresponding RAG chunks when adding new tables
- Register new tables in `qa_generator.py` topic list
- Notify P3 sync script about new artifacts
- Add TypeScript interfaces in P3

---

## Incremental Build System

```
P1 downloads/ ──▶ change_detector.py ──▶ DEPENDENCY_GRAPH ──▶ rebuild commands
                       │
                       ▼
              p1_manifest.json (file fingerprints)
```

The incremental system detects which P1 files changed since last build, maps them through a dependency graph to determine which P2 artifacts need rebuilding, and executes only the necessary build commands.

```bash
# Check what changed
bash scripts/build_incremental.sh

# Detect + rebuild
bash scripts/build_incremental.sh --execute

# Full rebuild + save new manifest
bash scripts/build_incremental.sh --full
```

---

## Testing Strategy

- **Framework**: pytest with custom markers
- **Tests**: 349+ across multiple categories
- **Slow tests**: `@pytest.mark.slow_integration` auto-skipped (20+ min each)
- **Categories**: smoke, schema/PK, referential integrity, value ranges, E2E integration

```bash
# Run tests (skips slow_integration)
python3 -m pytest tests/ -q

# Run everything including slow tests
python3 -m pytest tests/ -q -m ""
```

---

## Source Code Organization

```
src/
├── curate/          # Raw → canonical parsers (dims + facts)
├── features/        # Feature engineering from curated tables
├── models/          # Model training & scoring
├── export/          # RAG chunk + QA pair generation for P3
├── utils/           # Shared utilities (usage_registry, chat_tap)
├── incremental/     # Manifest-based P1 change detection
├── io/              # Config loading, path resolution
├── normalize/       # SOC crosswalks, employer normalization
└── validate/        # Data quality check helpers

scripts/             # Standalone builder scripts + utilities
configs/             # Pipeline config (paths, schemas, categories, objectives)
artifacts/           # Output directory (Parquet, JSON, metrics)
tests/               # pytest test suite (349+ tests)
```

---

## Key Commands

```bash
# Full pipeline (4 stages)
bash scripts/build_all.sh

# Individual stages
python3 -m src.curate.run_curate --paths configs/paths.yaml
python3 -m src.features.run_features --paths configs/paths.yaml
python3 -m src.models.run_models --paths configs/paths.yaml
python3 -m src.export.rag_builder && python3 -m src.export.qa_generator

# Tests
python3 -m pytest tests/ -q

# P1 readiness check
python3 scripts/check_p1_readiness.py

# Generate report
python3 scripts/generate_final_report.py
```

---

## Guardrails

1. **Read P1 data in-place** — Never copy files from `downloads/` into P2
2. **Parquet for everything** — All curated tables use Parquet format
3. **Track provenance** — Every table must record source files and ingestion timestamps
4. **No network calls** — P2 never fetches data from the internet; all data comes from P1
5. **Deterministic builds** — Same P1 input always produces identical P2 output
6. **RAG must be rebuilt after table changes** — Run rag_builder + qa_generator after any artifact update
7. **Preserve the P3 data contract** — Don't break column names, PKs, or RAG schemas
