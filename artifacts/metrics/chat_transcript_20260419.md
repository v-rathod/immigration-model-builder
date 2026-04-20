# Chat Transcript

### New transcript started 2026-03-31T03:45:34.947456+00:00 (reason=daily)

### [2026-03-31T03:45:34.947884+00:00] *System*

 [bootstrap]  
SESSION_START session=20260331T034534Z pid=84772 python=3.12.3

### [2026-03-31T03:45:34.977137+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-q', '--tb=short']

### [2026-03-31T03:45:37.211088+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-31T03:45:37.244777+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-31T03:45:37.289013+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-31T03:45:37.319648+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-31T03:45:37.353067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-31T03:45:37.398454+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-31T03:45:37.438332+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-31T03:45:37.707631+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-31T03:45:37.741901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-31T03:45:37.772049+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-31T03:45:37.802775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-31T03:45:37.832796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-31T03:45:37.866967+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-31T03:45:37.896221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-31T03:45:37.927887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-31T03:45:37.964841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-31T03:45:38.003227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-31T03:45:38.051963+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-31T03:45:38.082212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-31T03:45:38.112133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-31T03:45:38.142014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-31T03:45:38.206028+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-31T03:45:38.243184+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-31T03:45:38.273112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-31T03:45:38.302001+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-31T03:45:38.327780+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-31T03:45:38.357727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-31T03:45:38.385345+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-31T03:45:38.414548+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-31T03:45:38.446576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-31T03:45:38.476123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-31T03:45:38.503636+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-31T03:45:38.611702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-31T03:45:38.690088+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-31T03:45:38.890469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-31T03:45:39.030817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-31T03:45:39.108065+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-31T03:45:39.139098+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-31T03:45:39.168417+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-31T03:45:39.198397+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-31T03:45:39.227350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-31T03:45:39.253621+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-31T03:45:39.280043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-31T03:45:39.307390+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-31T03:45:39.333855+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-31T03:45:39.417152+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-31T03:45:39.497280+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-31T03:45:39.528840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-31T03:45:39.603247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-31T03:45:39.955403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-31T03:45:40.391010+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-31T03:45:40.430874+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-31T03:45:40.486515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-31T03:45:40.527350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-31T03:45:40.567718+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-31T03:45:40.628020+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact
tests/datasets/test_schema_and_pk_core.py:129: in test_row_count_exact
    assert len(df) == 8060, f"fact_cutoffs_all: {len(df)} rows ≠ 8060"
E   AssertionError: fact_cutoffs_all: 8115 rows ≠ 8060
E   assert 8115 == 8060
E    +  where 8115 = len(     chart category  ... bulletin_year bulletin_month\n0      FAD      EB1  ...          2011              7\n1      FAD...  EB4  ...          2026       

### [2026-03-31T03:45:40.655862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-31T03:45:40.703839+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-31T03:45:40.775048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-31T03:45:40.821175+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-31T03:45:40.850425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-31T03:45:40.879382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-31T03:45:40.912248+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-31T03:45:40.941988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-31T03:45:40.976832+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-31T03:45:41.009058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-31T03:45:41.038669+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-31T03:45:41.067672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-31T03:45:41.098517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-31T03:45:41.125844+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-31T03:45:41.153878+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-31T03:45:41.182539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-31T03:45:41.211727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-31T03:45:41.239631+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-31T03:45:41.268149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-31T03:45:41.296836+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-31T03:45:41.326567+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-31T03:45:41.354840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-31T03:45:41.382330+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-31T03:45:41.411010+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-31T03:45:41.438789+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-31T03:45:41.475133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-31T03:45:41.503706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-31T03:45:41.536110+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-31T03:45:41.577829+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-31T03:45:41.635505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-31T03:45:41.668613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-31T03:45:41.702001+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-31T03:45:41.749227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-31T03:45:41.791046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-31T03:45:41.818807+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-31T03:45:41.855769+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-31T03:45:41.883483+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-31T03:45:41.910074+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-31T03:45:41.936692+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-31T03:45:41.964231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-31T03:45:41.993498+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-31T03:45:42.023341+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-31T03:45:42.053994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-31T03:45:42.090298+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-31T03:45:42.119907+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-31T03:45:42.149845+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-31T03:45:42.178693+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-31T03:45:42.206113+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-31T03:45:42.233337+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-31T03:45:42.261200+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-31T03:45:42.290474+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-31T03:45:42.321041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-31T03:45:42.360981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-31T03:45:42.389741+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-31T03:45:42.418044+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-31T03:45:42.446125+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-31T03:45:42.473424+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-31T03:45:42.500862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-31T03:45:42.528431+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-31T03:45:42.555901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-31T03:45:42.838438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-31T03:45:42.879647+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-31T03:45:42.926578+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-31T03:45:42.954986+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-31T03:45:42.982090+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-31T03:45:43.010408+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-31T03:45:43.043719+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-31T03:45:43.098204+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-31T03:45:43.127951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-31T03:45:43.159819+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-31T03:45:43.187807+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-31T03:45:43.218618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-31T03:45:43.244726+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-31T03:45:43.282958+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-31T03:45:43.379420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-31T03:45:43.421935+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-31T03:45:43.460501+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged
tests/models/test_integration_e2e_sanity.py:141: in test_fact_cutoffs_unchanged
    assert len(df) == 8_060, f"fact_cutoffs_all regressed to {len(df)} rows"
E   AssertionError: fact_cutoffs_all regressed to 8115 rows
E   assert 8115 == 8060
E    +  where 8115 = len(     chart category  ... bulletin_year bulletin_month\n0      FAD      EB1  ...          2011              7\n1      FAD...  EB4  ... 

### [2026-03-31T03:45:43.490413+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-31T03:45:43.528576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-31T03:45:43.558584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-31T03:45:43.596025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-31T03:45:43.667201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-31T03:45:43.693928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-31T03:45:43.719722+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-31T03:45:43.754271+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-31T03:45:43.787142+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-31T03:45:43.815732+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-31T03:45:43.842182+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-31T03:45:43.870112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-31T03:45:43.897906+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-31T03:45:43.925384+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-31T03:45:43.953589+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraArtifactExists::test_parquet_exists

### [2026-03-31T03:45:43.981662+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraArtifactExists::test_model_json_exists

### [2026-03-31T03:45:44.008963+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraArtifactExists::test_original_untouched

### [2026-03-31T03:45:44.057849+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_base_columns_present

### [2026-03-31T03:45:44.086374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_retrograde_columns_present

### [2026-03-31T03:45:44.117761+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_row_count_matches_original

### [2026-03-31T03:45:44.152103+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_series_count

### [2026-03-31T03:45:44.188327+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_months_ahead_range

### [2026-03-31T03:45:44.217025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestRetrogradeColumns::test_retrograde_prob_range

### [2026-03-31T03:45:44.248612+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestRetrogradeColumns::test_expected_setback_non_negative

### [2026-03-31T03:45:44.277693+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestRetrogradeColumns::test_risk_adjusted_velocity_non_negative

### [2026-03-31T03:45:44.321872+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestRetrogradeColumns::test_no_null_retrograde_cols

### [2026-03-31T03:45:44.389270+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraVsOriginal::test_mcra_cutoffs_slower_than_optimistic

### [2026-03-31T03:45:44.420620+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraVsOriginal::test_same_series_set

### [2026-03-31T03:45:44.449523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_model_type

### [2026-03-31T03:45:44.476203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_version

### [2026-03-31T03:45:44.502910+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_mc_simulations

### [2026-03-31T03:45:44.529455+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_series_have_retro_params

### [2026-03-31T03:45:44.556539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_retro_monthly_prob_keys

### [2026-03-31T03:45:44.585154+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_eb2_ind_has_retrograde_data

### [2026-03-31T03:45:44.624544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-31T03:45:44.653902+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-31T03:45:44.683153+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-31T03:45:44.711551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-31T03:45:44.737537+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-31T03:45:44.767076+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-31T03:45:44.805201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-31T03:45:44.833900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-31T03:45:44.861657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-31T03:45:44.888755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-31T03:45:44.917737+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-31T03:45:44.954335+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-31T03:45:44.982378+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-31T03:45:45.013696+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-31T03:45:45.043375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-31T03:45:45.081550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-31T03:45:45.110414+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-31T03:45:45.143575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-31T03:45:45.171276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-31T03:45:45.206315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-31T03:45:45.235177+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-31T03:45:45.262924+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-31T03:45:45.289828+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-31T03:45:45.317853+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-31T03:45:45.367014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-31T03:45:45.395146+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-31T03:45:45.422535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-31T03:45:45.449449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-31T03:45:45.479501+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-31T03:45:45.509629+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-31T03:45:45.545005+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-31T03:45:45.575895+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-31T03:45:45.617176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-31T03:45:45.645181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-31T03:45:45.673407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-31T03:45:45.702102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-31T03:45:45.730714+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-31T03:45:45.760311+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-31T03:45:45.789235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-31T03:45:45.818900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-31T03:45:45.871600+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-31T03:45:45.899833+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-31T03:45:45.927895+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-31T03:45:45.956022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-31T03:45:45.983262+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-31T03:45:46.011396+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-31T03:45:46.038877+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-31T03:45:46.069879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-31T03:45:46.117937+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-31T03:45:46.146855+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-31T03:45:46.174789+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-31T03:45:46.202831+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-31T03:45:46.232844+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-31T03:45:46.263221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-31T03:45:46.294002+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-31T03:45:46.334656+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-31T03:45:46.369290+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-31T03:45:46.401637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-31T03:45:46.442043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-31T03:45:46.478694+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-31T03:45:46.513468+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-31T03:45:46.542824+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-31T03:45:46.577854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-31T03:45:46.613601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-31T03:45:46.648551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-31T03:45:46.684411+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-31T03:45:46.719789+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-31T03:45:46.759847+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-31T03:45:46.798504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-31T03:45:47.594188+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-31T03:45:47.651487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-31T03:45:47.693509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-31T03:45:47.728119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-31T03:45:47.755739+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-31T03:45:47.783513+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-31T03:45:47.811214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-31T03:45:47.840516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-31T03:45:47.874822+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-31T03:45:47.906220+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-31T03:45:47.944048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-31T03:45:47.978601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-31T03:45:48.007178+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-31T03:45:48.036280+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-31T03:45:48.063943+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-31T03:45:48.089975+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-31T03:45:48.118776+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-31T03:45:48.148410+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-31T03:45:48.177111+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-31T03:45:48.206194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-31T03:45:48.236458+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-31T03:45:48.266002+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-31T03:45:48.310484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-31T03:45:48.344046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-31T03:45:48.377764+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows
tests/p2_hardening/test_schema_and_pk.py:195: in test_fact_cutoffs_all_rows
    assert _row_count("fact_cutoffs_all") == 8_060
E   AssertionError: assert 8115 == 8060
E    +  where 8115 = _row_count('fact_cutoffs_all')

### [2026-03-31T03:45:48.408266+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows
tests/p2_hardening/test_schema_and_pk.py:198: in test_fact_cutoff_trends_rows
    assert _row_count("fact_cutoff_trends") == 8_060
E   AssertionError: assert 8115 == 8060
E    +  where 8115 = _row_count('fact_cutoff_trends')

### [2026-03-31T03:45:48.441635+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows
tests/p2_hardening/test_schema_and_pk.py:201: in test_category_movement_metrics_rows
    assert _row_count("category_movement_metrics") == 8_060
E   AssertionError: assert 6605 == 8060
E    +  where 6605 = _row_count('category_movement_metrics')

### [2026-03-31T03:45:48.468474+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-31T03:45:48.495782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-31T03:45:48.523504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-31T03:45:48.551131+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-31T03:45:48.578080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-31T03:45:48.604442+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-31T03:45:48.636598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-31T03:45:48.667254+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-31T03:45:48.693655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-31T03:45:48.725099+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-31T03:45:48.754854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-31T03:45:48.806325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-31T03:45:48.838787+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-31T03:45:48.865816+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-31T03:45:48.892089+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-31T03:45:48.919365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-31T03:45:48.945225+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-31T03:45:48.984696+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-31T03:45:49.017408+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-31T03:45:49.051535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-31T03:45:49.089129+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-31T03:45:49.123198+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-31T03:45:49.156947+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-31T03:45:49.193094+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-31T03:45:49.226897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-31T03:45:49.259381+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-31T03:45:49.294686+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-31T03:45:49.328507+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-31T03:45:49.363964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-31T03:45:49.398335+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-31T03:45:49.432507+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-31T03:45:49.464741+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-31T03:45:49.502116+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-31T03:45:49.533270+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-31T03:45:49.565081+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-31T03:45:49.592231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-31T03:45:49.620675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-31T03:45:49.649581+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-31T03:45:49.685735+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-31T03:45:49.717886+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-31T03:45:49.752031+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-31T03:45:49.791260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-31T03:45:49.818831+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-31T03:45:49.851176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-31T03:45:49.883742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-31T03:45:49.927455+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-31T03:45:49.955853+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-31T03:45:49.986353+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-31T03:45:50.014481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-31T03:45:50.041061+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-31T03:45:50.067785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-31T03:45:50.094990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-31T03:45:50.127193+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-31T03:45:50.154726+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-31T03:45:50.182266+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-31T03:45:50.210519+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-31T03:45:50.240548+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-31T03:45:50.268259+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-31T03:45:50.294963+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-31T03:45:50.322367+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-31T03:45:50.349160+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-31T03:45:50.380223+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-31T03:45:50.405266+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-31T03:45:50.430065+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-31T03:45:50.459040+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-31T03:45:50.486388+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-31T03:45:50.519005+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-31T03:45:50.551085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-31T03:45:50.582637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-31T03:45:50.612609+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-31T03:45:50.643008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-31T03:45:50.674171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-31T03:45:50.704817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-31T03:45:50.735462+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-31T03:45:50.762087+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-31T03:45:50.793223+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-31T03:45:50.825148+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-31T03:45:50.856423+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-31T03:45:50.884767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-31T03:45:50.912476+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-31T03:45:50.939618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-31T03:45:50.966278+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-31T03:45:50.995499+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-31T03:45:51.026102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-31T03:45:51.525768+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-31T03:45:52.087053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-31T03:45:52.571905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-31T03:45:52.601981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-31T03:45:52.628348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-31T03:45:52.655123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-31T03:45:52.681557+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-31T03:45:52.709034+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-31T03:45:52.736999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-31T03:45:52.774073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-31T03:45:52.802882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-31T03:45:52.830064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-31T03:45:52.859247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-31T03:45:52.888593+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-31T03:45:52.917724+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-31T03:45:52.959742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-31T03:45:52.994894+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-31T03:45:53.026749+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-31T03:45:53.054627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-31T03:45:53.095179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-31T03:45:53.125550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-31T03:45:53.157401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-31T03:45:53.261453+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-31T03:45:53.344509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-31T03:45:53.740071+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-31T03:45:53.787900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-31T03:45:53.823679+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-31T03:45:53.870757+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-31T03:45:53.897986+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-31T03:45:53.923674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-31T03:45:53.966270+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-31T03:45:53.994657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-31T03:45:54.022861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-31T03:45:54.052079+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-31T03:45:54.079536+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-31T03:45:54.105383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-31T03:45:54.132264+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-31T03:45:54.166605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-31T03:45:54.192828+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-31T03:45:54.220369+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-31T03:45:54.418028+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-31T03:45:54.459572+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-31T03:45:54.489290+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-31T03:45:54.524609+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-31T03:45:54.558342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-31T03:45:54.587757+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-31T03:45:54.623025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-31T03:45:54.653444+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-31T03:45:54.686648+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-31T03:45:54.718737+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-31T03:46:32.584369+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-31T03:46:32.637329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-31T03:46:32.683920+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-31T03:46:32.716634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-31T03:46:32.743956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-31T03:46:32.769550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-31T03:46:32.812072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-31T03:46:32.864537+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-31T03:46:32.905398+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-31T03:46:32.943806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-31T03:46:32.985275+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-31T03:46:33.017346+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-31T03:46:33.049948+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-31T03:46:33.079848+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-31T03:46:33.118007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-31T03:46:33.146684+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-31T03:46:33.174598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-31T03:46:33.202170+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-31T03:46:34.407969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-31T03:47:47.269716+00:00] *System*

 [bootstrap]  
SESSION_START session=20260331T034747Z pid=93005 python=3.12.3

### [2026-03-31T03:47:47.296767+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-q', '--tb=short']

### [2026-03-31T03:47:47.843625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-31T03:47:47.877225+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-31T03:47:47.920654+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-31T03:47:47.950428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-31T03:47:47.985295+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-31T03:47:48.031627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-31T03:47:48.068375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-31T03:47:48.328274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-31T03:47:48.357348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-31T03:47:48.386558+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-31T03:47:48.415473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-31T03:47:48.444585+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-31T03:47:48.475840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-31T03:47:48.503840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-31T03:47:48.532700+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-31T03:47:48.567372+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-31T03:47:48.600080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-31T03:47:48.651384+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-31T03:47:48.686861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-31T03:47:48.715019+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-31T03:47:48.742896+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-31T03:47:48.805628+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-31T03:47:48.839082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-31T03:47:48.870939+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-31T03:47:48.900655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-31T03:47:48.927621+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-31T03:47:48.957143+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-31T03:47:48.986260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-31T03:47:49.015280+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-31T03:47:49.043837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-31T03:47:49.071603+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-31T03:47:49.098648+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-31T03:47:49.204723+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-31T03:47:49.289867+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-31T03:47:49.492785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-31T03:47:49.626564+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-31T03:47:49.708107+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-31T03:47:49.736440+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-31T03:47:49.766064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-31T03:47:49.792975+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-31T03:47:49.820507+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-31T03:47:49.847148+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-31T03:47:49.878714+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-31T03:47:49.905846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-31T03:47:49.931453+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-31T03:47:50.010646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-31T03:47:50.092838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-31T03:47:50.124368+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-31T03:47:50.208590+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-31T03:47:50.716900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-31T03:47:51.131908+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-31T03:47:51.189019+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-31T03:47:51.247567+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-31T03:47:51.324804+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-31T03:47:51.368659+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-31T03:47:51.422721+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact
tests/datasets/test_schema_and_pk_core.py:129: in test_row_count_exact
    assert len(df) == 8060, f"fact_cutoffs_all: {len(df)} rows ≠ 8060"
E   AssertionError: fact_cutoffs_all: 8115 rows ≠ 8060
E   assert 8115 == 8060
E    +  where 8115 = len(     chart category  ... bulletin_year bulletin_month\n0      FAD      EB1  ...          2011              7\n1      FAD...  EB4  ...          2026       

### [2026-03-31T03:47:51.450185+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-31T03:47:51.478524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-31T03:47:51.552972+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-31T03:47:51.580227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-31T03:47:51.607555+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-31T03:47:51.635805+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-31T03:47:51.668305+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-31T03:47:51.686825+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-31T03:47:51.697506+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-31T03:47:51.729622+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-31T03:47:51.733148+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-31T03:47:51.756698+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-31T03:47:51.769857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-31T03:47:51.785070+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-31T03:47:51.814942+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-31T03:47:51.843485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-31T03:47:51.870840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-31T03:47:51.897562+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-31T03:47:51.911401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-31T03:47:51.923611+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-31T03:47:51.952286+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-31T03:47:51.979514+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-31T03:47:52.006428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-31T03:47:52.033587+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-31T03:47:52.041072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-31T03:47:52.061668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-31T03:47:52.090243+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-31T03:47:52.118970+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-31T03:47:52.147674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-31T03:47:52.175528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-31T03:47:52.205360+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-31T03:47:52.232869+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-31T03:47:52.259959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-31T03:47:52.292209+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-31T03:47:52.337676+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-31T03:47:52.383316+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-31T03:47:52.416740+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-31T03:47:52.449459+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-31T03:47:52.505240+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-31T03:47:52.494973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-31T03:47:52.535008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-31T03:47:52.578109+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-31T03:47:52.607811+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-31T03:47:52.635523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-31T03:47:52.671944+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-31T03:47:52.706780+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-31T03:47:52.735330+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-31T03:47:52.761900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-31T03:47:52.763495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-31T03:47:52.789638+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-31T03:47:52.829369+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-31T03:47:52.862132+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-31T03:47:52.889885+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-31T03:47:52.919664+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-31T03:47:52.947364+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-31T03:47:52.974802+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-31T03:47:53.006106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-31T03:47:53.013232+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-31T03:47:53.053650+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-31T03:47:53.033615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-31T03:47:53.081665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-31T03:47:53.062914+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-31T03:47:53.092078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-31T03:47:53.126282+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-31T03:47:53.119508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-31T03:47:53.146532+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-31T03:47:53.181397+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-31T03:47:53.212043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-31T03:47:53.214572+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-31T03:47:53.237394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-31T03:47:53.238604+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-31T03:47:53.263449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-31T03:47:53.264388+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-31T03:47:53.291121+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-31T03:47:53.318453+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-31T03:47:53.525807+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-31T03:47:53.551974+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-31T03:47:53.577274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-31T03:47:53.583327+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-31T03:47:53.622273+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-31T03:47:53.614864+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-31T03:47:53.670868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-31T03:47:53.660674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-31T03:47:53.689151+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-31T03:47:53.700515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-31T03:47:53.732757+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-31T03:47:53.718820+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-31T03:47:53.766730+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-31T03:47:53.751431+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-31T03:47:53.787703+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-31T03:47:53.794485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-31T03:47:53.836926+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-31T03:47:53.847104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-31T03:47:53.878627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-31T03:47:53.865351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-31T03:47:53.897201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-31T03:47:53.898962+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-31T03:47:53.925492+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-31T03:47:53.954967+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-31T03:47:53.991991+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-31T03:47:54.024499+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-31T03:47:54.029887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-31T03:47:54.138424+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-31T03:47:54.182419+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-31T03:47:54.216261+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged
tests/models/test_integration_e2e_sanity.py:141: in test_fact_cutoffs_unchanged
    assert len(df) == 8_060, f"fact_cutoffs_all regressed to {len(df)} rows"
E   AssertionError: fact_cutoffs_all regressed to 8115 rows
E   assert 8115 == 8060
E    +  where 8115 = len(     chart category  ... bulletin_year bulletin_month\n0      FAD      EB1  ...          2011              7\n1      FAD...  EB4  ... 

### [2026-03-31T03:47:54.246078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-31T03:47:54.255897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-31T03:47:54.312688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-31T03:47:54.282618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-31T03:47:54.327133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-31T03:47:54.374704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-31T03:47:54.378688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-31T03:47:54.400656+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-31T03:47:54.441289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-31T03:47:54.491336+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-31T03:47:54.448720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-31T03:47:54.498486+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-31T03:47:54.549866+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-31T03:47:54.564828+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-31T03:47:54.576385+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-31T03:47:54.604433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-31T03:47:54.651416+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-31T03:47:54.682242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-31T03:47:54.715007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-31T03:47:54.748058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-31T03:47:54.777526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraArtifactExists::test_parquet_exists

### [2026-03-31T03:47:54.804017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraArtifactExists::test_model_json_exists

### [2026-03-31T03:47:54.830019+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraArtifactExists::test_original_untouched

### [2026-03-31T03:47:54.859923+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_base_columns_present

### [2026-03-31T03:47:54.900396+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_retrograde_columns_present

### [2026-03-31T03:47:54.932222+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_row_count_matches_original

### [2026-03-31T03:47:54.963239+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_series_count

### [2026-03-31T03:47:54.991905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraSchema::test_months_ahead_range

### [2026-03-31T03:47:55.023500+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestRetrogradeColumns::test_retrograde_prob_range

### [2026-03-31T03:47:55.070601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestRetrogradeColumns::test_expected_setback_non_negative

### [2026-03-31T03:47:55.109083+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestRetrogradeColumns::test_risk_adjusted_velocity_non_negative

### [2026-03-31T03:47:55.139120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-31T03:47:55.138526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestRetrogradeColumns::test_no_null_retrograde_cols

### [2026-03-31T03:47:55.206602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraVsOriginal::test_mcra_cutoffs_slower_than_optimistic

### [2026-03-31T03:47:55.251431+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraVsOriginal::test_same_series_set

### [2026-03-31T03:47:55.283703+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_model_type

### [2026-03-31T03:47:55.318652+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_version

### [2026-03-31T03:47:55.352145+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_mc_simulations

### [2026-03-31T03:47:55.382888+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_series_have_retro_params

### [2026-03-31T03:47:55.409987+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_retro_monthly_prob_keys

### [2026-03-31T03:47:55.436543+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_pd_forecast_retrograde.py::TestMcraModelJson::test_eb2_ind_has_retrograde_data

### [2026-03-31T03:47:55.476702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-31T03:47:55.504922+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-31T03:47:55.533171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-31T03:47:55.560318+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-31T03:47:55.587721+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-31T03:47:55.618455+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-31T03:47:55.663903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-31T03:47:55.695682+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-31T03:47:55.724995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-31T03:47:55.731352+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression
tests/test_golden_snapshot.py:120: in test_no_row_count_regression
    assert not failures, (
E   AssertionError: Row count regressions detected:
E       • category_movement_metrics: golden=8,060 → current=6,605 (dropped 18.1%)
E   assert not ['category_movement_metrics: golden=8,060 → current=6,605 (dropped 18.1%)']

### [2026-03-31T03:47:55.751229+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-31T03:47:55.799311+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-31T03:47:55.837161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-31T03:47:55.867868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-31T03:47:55.900584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-31T03:47:55.934219+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-31T03:47:55.969021+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-31T03:47:55.998374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-31T03:47:56.026218+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-31T03:47:56.058100+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-31T03:47:56.097024+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-31T03:47:56.125387+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-31T03:47:56.153435+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-31T03:47:56.181531+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-31T03:47:56.212498+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-31T03:47:56.265460+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-31T03:47:56.305276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-31T03:47:56.355004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-31T03:47:56.335276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-31T03:47:56.363828+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-31T03:47:56.413981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-31T03:47:56.447460+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-31T03:47:56.478612+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-31T03:47:56.510037+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-31T03:47:56.548886+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-31T03:47:56.577035+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-31T03:47:56.605009+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-31T03:47:56.632506+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-31T03:47:56.659618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-31T03:47:56.686527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-31T03:47:56.713165+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-31T03:47:56.740389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-31T03:47:56.794094+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-31T03:47:56.829494+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-31T03:47:56.860893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-31T03:47:56.901223+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped

### [2026-03-31T03:47:56.890748+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-31T03:47:56.919117+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-31T03:47:56.952209+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-31T03:47:56.989498+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-31T03:47:57.017963+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-31T03:47:57.055204+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-31T03:47:57.089899+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-31T03:47:57.129468+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-31T03:47:57.158176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-31T03:47:57.186720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-31T03:47:57.222487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-31T03:47:57.253158+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-31T03:47:57.282943+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-31T03:47:57.314025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-31T03:47:57.344226+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-31T03:47:57.381936+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-31T03:47:57.432051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-31T03:47:57.473713+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-31T03:47:57.503688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-31T03:47:57.540694+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-31T03:47:57.528606+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-31T03:47:57.562556+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-31T03:47:57.616094+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-31T03:47:57.652996+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-31T03:47:57.688914+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-31T03:47:57.733838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-31T03:47:57.775971+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-31T03:47:58.112645+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-31T03:47:58.596309+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-31T03:47:58.631411+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-31T03:47:58.661985+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-31T03:47:58.668964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-31T03:47:58.697410+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-31T03:47:58.718808+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-31T03:47:58.727852+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-31T03:47:58.756968+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-31T03:47:58.785148+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-31T03:47:58.800871+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-31T03:47:58.813367+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-31T03:47:58.811620+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-31T03:47:58.840269+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-31T03:47:58.845355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-31T03:47:58.867220+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-31T03:47:58.870676+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-31T03:47:58.898425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-31T03:47:58.893429+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-31T03:47:58.920724+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-31T03:47:58.926479+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-31T03:47:58.949886+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-31T03:47:58.950908+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-31T03:47:58.976817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-31T03:47:58.977827+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-31T03:47:59.003334+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-31T03:47:59.008654+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-31T03:47:59.029646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-31T03:47:59.065041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-31T03:47:59.059584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-31T03:47:59.086966+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-31T03:47:59.088794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-31T03:47:59.115213+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-31T03:47:59.115859+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-31T03:47:59.142837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-31T03:47:59.143969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-31T03:47:59.171380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-31T03:47:59.172794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-31T03:47:59.198582+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-31T03:47:59.199818+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-31T03:47:59.234793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-31T03:47:59.225394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-31T03:47:59.251758+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-31T03:47:59.255718+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-31T03:47:59.277691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-31T03:47:59.281248+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows
tests/p2_hardening/test_schema_and_pk.py:195: in test_fact_cutoffs_all_rows
    assert _row_count("fact_cutoffs_all") == 8_060
E   AssertionError: assert 8115 == 8060
E    +  where 8115 = _row_count('fact_cutoffs_all')

### [2026-03-31T03:47:59.303601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-31T03:47:59.306173+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows
tests/p2_hardening/test_schema_and_pk.py:198: in test_fact_cutoff_trends_rows
    assert _row_count("fact_cutoff_trends") == 8_060
E   AssertionError: assert 8115 == 8060
E    +  where 8115 = _row_count('fact_cutoff_trends')

### [2026-03-31T03:47:59.337005+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows
tests/p2_hardening/test_schema_and_pk.py:201: in test_category_movement_metrics_rows
    assert _row_count("category_movement_metrics") == 8_060
E   AssertionError: assert 6605 == 8060
E    +  where 6605 = _row_count('category_movement_metrics')

### [2026-03-31T03:47:59.331115+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-31T03:47:59.358400+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-31T03:47:59.358358+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-31T03:47:59.385325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-31T03:47:59.385420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-31T03:47:59.412524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-31T03:47:59.412110+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-31T03:47:59.438415+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-31T03:47:59.438598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-31T03:47:59.464998+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-31T03:47:59.465220+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-31T03:47:59.492161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-31T03:47:59.491943+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-31T03:47:59.518672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-31T03:47:59.518523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-31T03:47:59.545112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-31T03:47:59.545386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-31T03:47:59.572898+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-31T03:47:59.571066+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-31T03:47:59.600627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-31T03:47:59.597975+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-31T03:47:59.624022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-31T03:47:59.628401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-31T03:47:59.650121+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-31T03:47:59.675512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-31T03:47:59.686522+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-31T03:47:59.706579+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-31T03:47:59.701393+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-31T03:47:59.728401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-31T03:47:59.727743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-31T03:47:59.754012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-31T03:47:59.753820+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-31T03:47:59.781076+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-31T03:47:59.780964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-31T03:47:59.807775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-31T03:47:59.807842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-31T03:47:59.838517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-31T03:47:59.834228+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-31T03:47:59.861344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-31T03:47:59.865177+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-31T03:47:59.899657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-31T03:47:59.889907+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-31T03:47:59.916868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-31T03:47:59.923182+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-31T03:47:59.944454+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-31T03:47:59.950798+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-31T03:47:59.971833+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-31T03:47:59.978450+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-31T03:47:59.999389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-31T03:48:00.006814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-31T03:48:00.023679+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-31T03:48:00.024854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-31T03:48:00.025745+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-31T03:48:00.026643+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-31T03:48:00.027392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-31T03:48:00.030659+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-31T03:48:00.033025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-31T03:48:00.064526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-31T03:48:00.058608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-31T03:48:00.095823+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-31T03:48:00.087071+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-31T03:48:00.115669+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-31T03:48:00.122170+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-31T03:48:00.143823+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-31T03:48:00.154419+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-31T03:48:00.177976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-31T03:48:00.172121+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-31T03:48:00.196460+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-31T03:48:00.197946+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-31T03:48:00.198835+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-31T03:48:00.201875+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-31T03:48:00.203993+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-31T03:48:00.226101+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-31T03:48:00.226963+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-31T03:48:00.227782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-31T03:48:00.228793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-31T03:48:00.229518+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-31T03:48:00.230395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-31T03:48:00.231217+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-31T03:48:00.231923+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-31T03:48:00.232159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-31T03:48:00.232959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-31T03:48:00.238484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-31T03:48:00.238135+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-31T03:48:00.271958+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-31T03:48:00.266104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-31T03:48:00.300251+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-31T03:48:00.294317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-31T03:48:00.322527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-31T03:48:00.321542+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-31T03:48:00.347660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-31T03:48:00.348289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-31T03:48:00.375181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-31T03:48:00.376580+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-31T03:48:00.411750+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-31T03:48:00.403436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-31T03:48:00.430153+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-31T03:48:00.435222+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-31T03:48:00.465187+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-31T03:48:00.457075+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-31T03:48:00.494043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-31T03:48:00.522749+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-31T03:48:00.566960+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-31T03:48:00.555743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-31T03:48:00.587258+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-31T03:48:00.627517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-31T03:48:00.633291+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-31T03:48:00.657550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-31T03:48:00.655777+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-31T03:48:00.686999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-31T03:48:00.684351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-31T03:48:00.710755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-31T03:48:00.712547+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-31T03:48:00.737426+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-31T03:48:00.738968+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-31T03:48:00.764985+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-31T03:48:00.766645+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-31T03:48:00.792800+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-31T03:48:00.795263+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-31T03:48:00.827022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-31T03:48:00.826237+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-31T03:48:00.860693+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-31T03:48:00.861341+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-31T03:48:00.889461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-31T03:48:00.888633+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-31T03:48:00.924440+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-31T03:48:00.924004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-31T03:48:00.963245+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-31T03:48:00.961330+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-31T03:48:00.994906+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-31T03:48:00.997773+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-31T03:48:01.029176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-31T03:48:01.031770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-31T03:48:01.061356+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-31T03:48:01.058686+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-31T03:48:01.093012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-31T03:48:01.087743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-31T03:48:01.126316+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-31T03:48:01.129299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-31T03:48:01.161317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-31T03:48:01.158770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-31T03:48:01.204560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-31T03:48:01.192289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-31T03:48:01.225524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-31T03:48:01.227333+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-31T03:48:01.258153+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-31T03:48:01.255654+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-31T03:48:01.287880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-31T03:48:01.290769+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-31T03:48:01.320449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-31T03:48:01.321406+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-31T03:48:01.351493+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-31T03:48:01.354375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-31T03:48:01.386762+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-31T03:48:01.383166+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-31T03:48:01.417271+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-31T03:48:01.414707+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-31T03:48:01.446118+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-31T03:48:01.449042+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-31T03:48:01.486595+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-31T03:48:01.482057+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-31T03:48:01.518135+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-31T03:48:01.517103+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-31T03:48:01.547814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-31T03:48:01.547267+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-31T03:48:01.578067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-31T03:48:01.584351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-31T03:48:01.611078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-31T03:48:01.608131+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-31T03:48:01.637757+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-31T03:48:01.641655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-31T03:48:01.670060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-31T03:48:01.669981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-31T03:48:01.700249+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-31T03:48:01.700217+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-31T03:48:01.730108+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-31T03:48:01.729615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-31T03:48:01.758089+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-31T03:48:01.758046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-31T03:48:01.787361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-31T03:48:01.787742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-31T03:48:01.815477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-31T03:48:01.816172+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-31T03:48:01.842978+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-31T03:48:01.869966+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-31T03:48:01.896493+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-31T03:48:01.922594+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-31T03:48:01.952950+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-31T03:48:01.978383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-31T03:48:02.007442+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-31T03:48:02.035143+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-31T03:48:02.071798+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-31T03:48:02.103450+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-31T03:48:02.133348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-31T03:48:02.163920+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-31T03:48:02.200509+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk
tests/test_rag_quality.py:120: in test_cutoffs_count_in_visa_bulletin_chunk
    assert f"{actual:,}" in vb_chunks[0]["text"], \
E   AssertionError: Visa bulletin summary should mention 8,115 records
E   assert '8,115' in 'Visa Bulletin History (fact_cutoffs_all):\nTotal records: 8,060\nColumns: chart, category, country, cutoff_date, stat...ulletin PDFs (~180 bulletins parsed)\nUse: Historical cuto

### [2026-03-31T03:48:02.228886+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-31T03:48:02.291619+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-31T03:48:02.283919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-31T03:48:02.312014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-31T03:48:02.338826+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-31T03:48:02.365865+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-31T03:48:02.392044+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-31T03:48:02.420357+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-31T03:48:02.448043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-31T03:48:02.474288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-31T03:48:02.500353+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-31T03:48:02.529793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-31T03:48:02.560338+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts
tests/test_rag_quality.py:336: in test_qa_cache_not_older_than_key_artifacts
    assert len(stale) == 0, \
E   AssertionError: qa_cache.json is older than: ['employer_friendliness_scores.parquet', 'pd_forecasts.parquet']. Re-run qa_generator.
E   assert 2 == 0
E    +  where 2 = len(['employer_friendliness_scores.parquet', 'pd_forecasts.parquet'])

### [2026-03-31T03:48:02.586465+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-31T03:48:02.626089+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-31T03:48:02.664499+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-31T03:48:02.691225+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-31T03:48:02.717623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-31T03:48:02.726548+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-31T03:48:02.746424+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-31T03:48:02.773008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-31T03:48:02.799403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-31T03:48:02.828542+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-31T03:48:02.976492+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-31T03:48:03.187069+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-31T03:48:03.216463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-31T03:48:03.242826+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-31T03:48:03.271025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-31T03:48:03.299321+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-31T03:48:03.326471+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-31T03:48:03.354893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-31T03:48:03.391390+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-31T03:48:03.419929+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-31T03:48:03.447103+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-31T03:48:03.474509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-31T03:48:03.501459+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-31T03:48:03.528156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-31T03:48:03.561755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-31T03:48:03.590342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-31T03:48:03.617517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-31T03:48:03.642669+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-31T03:48:03.678015+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-31T03:48:03.707156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-31T03:48:03.735767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-31T03:48:03.835938+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-31T03:48:03.916062+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-31T03:48:04.315224+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-31T03:48:04.362120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-31T03:48:04.404363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-31T03:48:04.448295+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-31T03:48:04.474596+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-31T03:48:04.502186+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-31T03:48:04.542505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-31T03:48:04.572070+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-31T03:48:04.599582+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-31T03:48:04.627576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-31T03:48:04.659161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-31T03:48:04.686179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-31T03:48:04.711573+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-31T03:48:04.739232+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-31T03:48:04.764705+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-31T03:48:04.790770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-31T03:48:04.980014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-31T03:48:05.019102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-31T03:48:05.048638+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-31T03:48:05.085177+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-31T03:48:05.118959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-31T03:48:05.151517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-31T03:48:05.187262+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-31T03:48:05.214487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-31T03:48:05.243887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-31T03:48:05.271553+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-31T03:48:42.004288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-31T03:48:42.031485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-31T03:48:42.066294+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-31T03:48:42.094813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-31T03:48:42.122861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-31T03:48:42.148068+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-31T03:48:42.185051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-31T03:48:42.231768+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-31T03:48:42.269853+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-31T03:48:42.307585+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-31T03:48:42.345754+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-31T03:48:42.377366+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-31T03:48:42.405243+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-31T03:48:42.431985+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-31T03:48:42.465380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-31T03:48:42.494206+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-31T03:48:42.523150+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-31T03:48:42.553067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-31T03:48:43.123821+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-31T03:49:58.490393+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-31T03:49:58.553863+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-31T03:49:58.593095+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-31T03:49:58.745325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-31T03:49:58.887077+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-31T03:49:59.193471+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-31T03:49:59.361669+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-31T03:49:59.624244+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-31T03:49:59.872966+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-31T03:49:59.920487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-31T03:49:59.965887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-31T03:50:00.023321+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-31T03:50:00.126314+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-31T03:50:00.152264+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-31T03:50:00.177472+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-31T03:50:00.482073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-31T03:50:00.509168+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-31T03:50:00.540081+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-31T03:50:00.580709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-31T03:50:00.636549+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-31T03:50:00.676996+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-31T03:50:00.718212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-31T03:50:00.760255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-31T03:50:00.801363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-31T03:50:00.844033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-31T03:50:00.884435+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-31T03:50:00.910876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-31T03:50:00.986276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-31T03:50:01.179673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-31T03:50:01.249706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-31T03:50:01.326832+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-31T03:50:01.401719+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-31T03:50:01.494435+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-31T03:50:01.592100+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-31T03:50:02.254969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-31T03:50:02.914322+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression
tests/test_golden_snapshot.py:120: in test_no_row_count_regression
    assert not failures, (
E   AssertionError: Row count regressions detected:
E       • category_movement_metrics: golden=8,060 → current=6,605 (dropped 18.1%)
E   assert not ['category_movement_metrics: golden=8,060 → current=6,605 (dropped 18.1%)']

### [2026-03-31T03:50:03.528735+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-31T03:50:04.198563+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped

### [2026-03-31T03:50:04.849060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-31T03:50:05.483433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-31T03:50:06.044321+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-31T03:50:06.106516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-31T03:50:06.217613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-31T03:50:06.244940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-31T03:50:06.274165+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-31T03:50:06.301733+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-31T03:50:06.329655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-31T03:50:06.357563+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-31T03:50:06.384880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-31T03:50:06.411740+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-31T03:50:06.443068+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-31T03:50:06.506512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-31T03:50:06.534684+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-31T03:50:06.562632+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-31T03:50:06.590308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-31T03:50:06.618668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-31T03:50:06.645660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-31T03:50:06.672267+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-31T03:50:06.699102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-31T03:50:06.726115+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-31T03:50:06.752264+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-31T03:50:06.778667+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-31T03:50:06.804295+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-31T03:50:06.829979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-31T03:50:06.855517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-31T03:50:06.882028+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-31T03:50:06.907720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-31T03:50:06.933274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-31T03:50:06.958979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-31T03:50:06.984389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-31T03:50:07.010655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-31T03:50:07.036258+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-31T03:50:07.061642+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-31T03:50:07.087375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-31T03:50:07.114052+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-31T03:50:07.140119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-31T03:50:07.166585+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-31T03:50:07.194087+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-31T03:50:07.221731+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-31T03:50:07.249131+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-31T03:50:07.277798+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-31T03:50:07.313301+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-31T03:50:07.341073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-31T03:50:07.368985+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-31T03:50:07.395896+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-31T03:50:07.422356+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-31T03:50:07.447999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-31T03:50:07.473165+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-31T03:50:07.498667+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-31T03:50:07.524820+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-31T03:50:07.552169+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-31T03:50:07.578811+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-31T03:50:07.605969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-31T03:50:07.632956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-31T03:50:07.659655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-31T03:50:07.685210+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-31T03:50:07.710673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-31T03:50:07.736813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-31T03:50:07.762789+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-31T03:50:07.788145+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-31T03:50:07.813571+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-31T03:50:07.839195+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-31T03:50:07.868129+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-31T03:50:07.893592+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-31T03:50:07.919742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-31T03:50:07.946289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-31T03:50:07.972542+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-31T03:50:07.999090+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-31T03:50:08.024954+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-31T03:50:08.051394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-31T03:50:08.077677+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-31T03:50:08.108544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-31T03:50:08.134219+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-31T03:50:08.160606+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-31T03:50:08.186197+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-31T03:50:08.211488+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-31T03:50:08.238854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-31T03:50:08.426129+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-31T03:50:08.453639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-31T03:50:08.480418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-31T03:50:08.589930+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-31T03:50:08.660352+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-31T03:50:08.686112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-31T03:50:08.714514+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-31T03:50:08.742028+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-31T03:50:08.769794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-31T03:50:08.796495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-31T03:50:08.823520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-31T03:50:08.851570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-31T03:50:08.879794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-31T03:50:08.906930+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-31T03:50:08.934994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-31T03:50:08.961933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-31T03:50:08.989069+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-31T03:50:09.017078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-31T03:50:09.049118+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-31T03:50:09.078194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-31T03:50:09.106004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-31T03:50:09.133982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-31T03:50:09.165418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-31T03:50:09.194377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-31T03:50:09.222199+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-31T03:50:09.247904+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-31T03:50:09.274674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-31T03:50:09.300386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-31T03:50:09.327066+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-31T03:50:09.352470+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-31T03:50:09.378782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-31T03:50:09.405709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-31T03:50:09.441458+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-31T03:50:09.471260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-31T03:50:09.498141+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-31T03:50:09.525559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-31T03:50:09.552478+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-31T03:50:09.579104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-31T03:50:09.605208+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-31T03:50:09.631709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-31T03:50:09.657921+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-31T03:50:09.686074+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-31T03:50:09.711962+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-31T03:50:09.737579+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-31T03:50:09.763738+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-31T03:50:09.789369+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-31T03:50:09.815829+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-31T03:50:09.842655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-31T03:50:09.868947+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-31T03:50:09.894563+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-31T03:50:09.920780+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-31T03:50:09.956089+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-31T03:50:09.988260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-31T03:50:10.018112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-31T03:50:10.049691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-31T03:50:10.089612+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk
tests/test_rag_quality.py:120: in test_cutoffs_count_in_visa_bulletin_chunk
    assert f"{actual:,}" in vb_chunks[0]["text"], \
E   AssertionError: Visa bulletin summary should mention 8,115 records
E   assert '8,115' in 'Visa Bulletin History (fact_cutoffs_all):\nTotal records: 8,060\nColumns: chart, category, country, cutoff_date, stat...ulletin PDFs (~180 bulletins parsed)\nUse: Historical cuto

### [2026-03-31T03:50:10.117466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-31T03:50:10.180185+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-31T03:50:10.206787+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-31T03:50:10.232201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-31T03:50:10.258058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-31T03:50:10.283642+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-31T03:50:10.311505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-31T03:50:10.336856+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-31T03:50:10.361960+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-31T03:50:10.387066+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-31T03:50:10.413547+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-31T03:50:10.442976+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts
tests/test_rag_quality.py:336: in test_qa_cache_not_older_than_key_artifacts
    assert len(stale) == 0, \
E   AssertionError: qa_cache.json is older than: ['employer_friendliness_scores.parquet', 'pd_forecasts.parquet']. Re-run qa_generator.
E   assert 2 == 0
E    +  where 2 = len(['employer_friendliness_scores.parquet', 'pd_forecasts.parquet'])

### [2026-03-31T03:50:10.468332+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-31T03:50:10.509527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-31T03:50:10.548711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-31T03:50:10.577771+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-31T03:50:10.604693+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-31T03:50:10.633814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-31T03:50:10.660584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-31T03:50:10.687237+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-31T03:50:10.714342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-31T03:50:10.814035+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-31T03:53:30.079424+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_run_noop

### [2026-03-31T03:53:30.113491+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=612 failed=8 exit=1

### [2026-03-31T03:55:43.567255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_run_noop

### [2026-03-31T03:55:43.635739+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=612 failed=8 exit=1

