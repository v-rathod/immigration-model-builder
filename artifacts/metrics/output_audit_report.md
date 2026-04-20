# Output Audit Report

Generated: 2026-02-22 23:43:03

---

## Overall Summary

| Table | Exists | Rows | Required Cols | PK Unique | Status |
|-------|--------|------|---------------|-----------|--------|
| dim_country | ✓ | 249 | ✓ OK | ✓ | ✓ PASS |
| fact_cutoffs | ✓ | 8,315 | ✓ OK | ✓ | ✓ PASS |
| dim_soc | ✓ | 1,396 | ✓ OK | ✓ | ✓ PASS |
| dim_area | ✓ | 587 | ✓ OK | ✓ | ✓ PASS |
| dim_visa_class | ✓ | 6 | ✓ OK | ✓ | ✓ PASS |
| dim_employer | ✓ | 227,076 | ✓ OK | ✓ | ✓ PASS |
| fact_perm | ✓ | 1,674,724 | ✓ OK | ✓ | ✓ PASS |
| fact_oews | ✓ | 446,432 | ✓ OK | ✓ | ✓ PASS |
| fact_lca | ✓ | 9,558,695 | ✓ OK | N/A | ✓ PASS |

---

## dim_country

**Description:** Canonical list of countries/territories for chargeability and joins.

**Row Count:** 249

**Required Columns:** ✓ All 6 present

**Actual Columns (sample 6):** `country_name`, `iso2`, `iso3`, `region`, `source_file`, `ingested_at`

**Primary Key:** `iso3` - ✓ Unique

---

## fact_cutoffs

**Description:** Monthly Visa Bulletin cut-off dates by employment category and chargeability.

**Row Count:** 8,315

**Required Columns:** ✓ All 10 present

**Actual Columns (sample 10):** `chart`, `category`, `country`, `cutoff_date`, `status_flag`, `source_file`, `page_ref`, `ingested_at`, `bulletin_year`, `bulletin_month`

**Primary Key:** `bulletin_year, bulletin_month, chart, category, country` - ✓ Unique

**Partitions:**

- Column: `bulletin_year`
- Values: 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026
- Column: `bulletin_month`
- Values: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

---

## dim_soc

**Description:** SOC 2018 normalized occupation dimension used for joins across PERM/LCA/OEWS.

**Row Count:** 1,396

**Required Columns:** ✓ All 12 present

**Actual Columns (sample 12):** `soc_code`, `soc_title`, `soc_version`, `soc_major_group`, `soc_minor_group`, `soc_broad_group`, `from_version`, `from_code`, `mapping_confidence`, `is_aggregated`, `source_file`, `ingested_at`

**Primary Key:** `soc_code` - ✓ Unique

---

## dim_area

**Description:** OEWS geographic areas (national, state, metropolitan, nonmetropolitan) used to join wage percentiles and normalize PERM/LCA worksites.

**Row Count:** 587

**Required Columns:** ✓ All 10 present

**Actual Columns (sample 10):** `area_code`, `area_title`, `area_type`, `state_abbr`, `state_fips`, `cbsa_code`, `metro_status`, `ref_year`, `source_file`, `ingested_at`

**Primary Key:** `area_code` - ✓ Unique

---

## dim_visa_class

**Description:** Canonical employment-based visa families and subcategories used across DOS, USCIS, PERM/LCA joins.

**Row Count:** 6

**Required Columns:** ✓ All 9 present

**Actual Columns (sample 9):** `family_code`, `family_name`, `sub_code`, `sub_name`, `is_employment`, `is_grouped`, `notes`, `source_file`, `ingested_at`

**Primary Key:** `family_code, sub_code` - ✓ Unique

---

## dim_employer

**Description:** Canonical employer identities for joins across PERM/LCA/H1B facts and employer-level features.

**Row Count:** 227,076

**Required Columns:** ✓ All 6 present

**Actual Columns (sample 6):** `employer_id`, `employer_name`, `aliases`, `domain`, `source_files`, `ingested_at`

**Primary Key:** `employer_id` - ✓ Unique

---

## fact_perm

**Description:** PERM labor certification applications with outcomes, employer, occupation, and worksite dimensions.

**Row Count:** 1,674,724

**Required Columns:** ✓ All 19 present

**Actual Columns (sample 20):** `case_number`, `case_status`, `received_date`, `decision_date`, `employer_id`, `employer_name`, `soc_code`, `area_code`, `employer_country`, `job_title`, `wage_offer_from`, `wage_offer_to`, `wage_offer_unit`, `worksite_city`, `worksite_state`, `worksite_postal`, `is_fulltime`, `source_file`, `ingested_at`, `fiscal_year`

**Primary Key:** `case_number, fiscal_year` - ✓ Unique

**Partitions:**

- Column: `fiscal_year`
- Values: 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026

---

## fact_oews

**Description:** OEWS wage percentiles by occupation and area for salary benchmarking.

**Row Count:** 446,432

**Required Columns:** ✓ All 18 present

**Actual Columns (sample 18):** `area_code`, `soc_code`, `tot_emp`, `h_mean`, `a_mean`, `h_median`, `a_median`, `h_pct10`, `h_pct25`, `h_pct75`, `h_pct90`, `a_pct10`, `a_pct25`, `a_pct75`, `a_pct90`, `source_file`, `ingested_at`, `ref_year`

**Primary Key:** `area_code, soc_code, ref_year` - ✓ Unique

**Partitions:**

- Column: `ref_year`
- Values: 2023, 2024

---

## fact_lca

**Description:** LCA (H-1B) labor condition applications with employer, occupation, wage, and worksite dimensions.

**Row Count:** 9,558,695

**Required Columns:** ✓ All 23 present

**Actual Columns (sample 20):** `case_number`, `case_status`, `visa_class`, `received_date`, `decision_date`, `employer_name_raw`, `employer_id`, `soc_code`, `soc_title`, `job_title`, `is_fulltime`, `wage_rate_from`, `wage_rate_to`, `prevailing_wage`, `wage_unit`, `pw_unit`, `worksite_city`, `worksite_state`, `worksite_postal`, `naics_code`

**Partitions:**

- Column: `fiscal_year`
- Values: 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026

---

## Notes

- **Required Columns**: Checks if all fields defined in schema are present in output.
- **PK Unique**: For dimensions, verifies primary key has no duplicates.
- **Partitions**: For facts, lists detected partition columns and values.
- Exit code 1 if any required column is missing OR a dim PK uniqueness check fails.

