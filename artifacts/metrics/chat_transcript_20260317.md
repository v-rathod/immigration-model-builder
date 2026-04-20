# Chat Transcript

### New transcript started 2026-03-13T16:42:50.650761+00:00 (reason=daily)

### [2026-03-13T16:42:50.651186+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T164250Z pid=33548 python=3.12.3

### [2026-03-13T16:42:50.683012+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-x', '-q', '--tb=short']

### [2026-03-13T16:42:56.327332+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-13T16:42:56.372341+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-13T16:42:56.426978+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-13T16:42:56.459823+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-13T16:42:56.494065+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-13T16:42:56.539307+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-13T16:42:56.576005+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-13T16:42:56.857675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-13T16:42:56.896499+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-13T16:42:56.901660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-13T16:42:56.906722+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-13T16:42:56.912979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-13T16:42:56.920857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-13T16:42:56.924507+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-13T16:42:56.928107+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-13T16:42:56.936929+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-13T16:42:56.971539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-13T16:42:57.021606+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-13T16:42:57.054288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-13T16:42:57.084569+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-13T16:42:57.113908+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-13T16:42:57.179391+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-13T16:42:57.218109+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-13T16:42:57.247615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-13T16:42:57.276324+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-13T16:42:57.302608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-13T16:42:57.333523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-13T16:42:57.360293+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-13T16:42:57.390437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-13T16:42:57.424729+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-13T16:42:57.454041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-13T16:42:57.482447+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-13T16:42:57.583477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-13T16:42:57.660409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-13T16:42:57.837813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-13T16:42:57.975215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-13T16:42:58.032834+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-13T16:42:58.040806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-13T16:42:58.071448+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-13T16:42:58.093257+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-13T16:42:58.111162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-13T16:42:58.142559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-13T16:42:58.171386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-13T16:42:58.200317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-13T16:42:58.227601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-13T16:42:58.314880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-13T16:42:58.398991+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-13T16:42:58.431519+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-13T16:42:58.510263+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-13T16:42:58.901624+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-13T16:42:59.348662+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-13T16:42:59.390153+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-13T16:42:59.447785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-13T16:42:59.489404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-13T16:42:59.531289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-13T16:42:59.563817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-13T16:42:59.591852+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-13T16:42:59.640876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-13T16:42:59.710336+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-13T16:42:59.737933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-13T16:42:59.767338+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-13T16:42:59.797231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-13T16:42:59.832540+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-13T16:42:59.862418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-13T16:42:59.898423+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-13T16:42:59.927716+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-13T16:42:59.956862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-13T16:42:59.986934+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-13T16:43:00.019820+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-13T16:43:00.048797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-13T16:43:00.076585+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-13T16:43:00.103033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-13T16:43:00.130467+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-13T16:43:00.157815+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-13T16:43:00.182645+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-13T16:43:00.185208+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-13T16:43:00.189187+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-13T16:43:00.191461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-13T16:43:00.220243+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-13T16:43:00.245575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-13T16:43:00.248390+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-13T16:43:00.253448+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-13T16:43:00.256211+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-13T16:43:00.258868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-13T16:43:00.291551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-13T16:43:00.323495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-13T16:43:00.330646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-13T16:43:00.338181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-13T16:43:00.360212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-13T16:43:00.378203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-13T16:43:00.381813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-13T16:43:00.386580+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-13T16:43:00.392483+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-13T16:43:00.399547+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-13T16:43:00.404132+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-13T16:43:00.406772+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-13T16:43:00.408592+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-13T16:43:00.410816+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-13T16:43:00.413239+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-13T16:43:00.415972+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-13T16:43:00.445818+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-13T16:43:00.473825+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-13T16:43:00.501046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-13T16:43:00.529270+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-13T16:43:00.559297+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-13T16:43:00.587051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-13T16:43:00.615088+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-13T16:43:00.675505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-13T16:43:00.711888+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-13T16:43:00.741466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-13T16:43:00.768753+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-13T16:43:00.795634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-13T16:43:00.822691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-13T16:43:00.853744+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-13T16:43:00.884481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-13T16:43:00.913086+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-13T16:43:01.206379+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-13T16:43:01.248369+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-13T16:43:01.295120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-13T16:43:01.324660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-13T16:43:01.352883+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-13T16:43:01.383284+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-13T16:43:01.416539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-13T16:43:01.474558+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-13T16:43:01.479890+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-13T16:43:01.483857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-13T16:43:01.487108+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-13T16:43:01.488613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-13T16:43:01.489509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-13T16:43:01.499396+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-13T16:43:01.571060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-13T16:43:01.613494+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-13T16:43:01.642680+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-13T16:43:01.671691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-13T16:43:01.721313+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-13T16:43:01.754497+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-13T16:43:01.781481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-13T16:43:01.808552+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-13T16:43:01.836645+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-13T16:43:01.863869+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-13T16:43:01.902098+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-13T16:43:01.929623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-13T16:43:01.958130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-13T16:43:01.986648+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-13T16:43:02.014469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-13T16:43:02.043319+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-13T16:43:02.073612+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-13T16:43:02.124267+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-13T16:43:02.152702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-13T16:43:02.182709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-13T16:43:02.212461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-13T16:43:02.239998+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-13T16:43:02.275487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-13T16:43:02.326379+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-13T16:43:02.354593+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-13T16:43:02.382157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-13T16:43:02.411336+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-13T16:43:02.440139+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-13T16:43:02.473060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-13T16:43:02.501022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-13T16:43:02.527874+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-13T16:43:02.555943+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-13T16:43:02.585012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-13T16:43:02.587664+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-13T16:43:02.589754+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-13T16:43:02.591995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-13T16:43:02.601033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-13T16:43:02.604193+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-13T16:43:02.644927+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-13T16:43:02.674900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-13T16:43:02.681048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-13T16:43:02.731282+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-13T16:43:02.760388+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-13T16:43:02.788561+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-13T16:43:02.817333+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-13T16:43:02.846835+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-13T16:43:02.876157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-13T16:43:02.904509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T16:43:02.933035+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T16:43:02.968096+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T16:43:02.996289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T16:43:03.024436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T16:43:03.057574+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T16:43:03.088446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T16:43:03.160598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T16:43:03.187510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T16:43:03.218087+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T16:43:03.280919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T16:43:03.309484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T16:43:03.337729+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T16:43:03.365469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T16:43:03.394515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T16:43:03.423322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T16:43:03.452557+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T16:43:03.480044+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T16:43:03.514543+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T16:43:03.517191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T16:43:03.519090+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T16:43:03.521430+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T16:43:03.523004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T16:43:03.524565+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T16:43:03.526639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-13T16:43:03.529491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-13T16:43:03.538475+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-13T16:43:03.569597+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-13T16:43:03.607325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-13T16:43:03.641598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-13T16:43:03.674061+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-13T16:43:03.701616+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-13T16:43:03.742298+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-13T16:43:03.774374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-13T16:43:03.783383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-13T16:43:03.792473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-13T16:43:03.803071+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-13T16:43:03.821770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-13T16:43:03.837819+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-13T16:43:04.617564+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-13T16:43:04.646067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-13T16:43:04.677401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-13T16:43:04.708583+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-13T16:43:04.736046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-13T16:43:04.768892+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-13T16:43:04.796380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-13T16:43:04.838731+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-13T16:43:04.875508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-13T16:43:04.903982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-13T16:43:04.911720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-13T16:43:04.946875+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-13T16:43:04.952889+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-13T16:43:04.987565+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-13T16:43:05.014576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-13T16:43:05.041618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-13T16:43:05.070014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-13T16:43:05.099276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-13T16:43:05.128351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-13T16:43:05.156652+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-13T16:43:05.186130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-13T16:43:05.214197+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-13T16:43:05.253876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-13T16:43:05.285526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-13T16:43:05.313705+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-13T16:43:05.341240+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-13T16:43:05.369231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-13T16:43:05.395766+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-13T16:43:05.423636+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-13T16:43:05.453001+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-13T16:43:05.480344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-13T16:43:05.508178+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-13T16:43:05.536655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-13T16:43:05.562884+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-13T16:43:05.592227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-13T16:43:05.620457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-13T16:43:05.651411+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-13T16:43:05.682969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-13T16:43:05.741104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-13T16:43:05.775955+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-13T16:43:05.801628+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-13T16:43:05.826210+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-13T16:43:05.850394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-13T16:43:05.875779+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-13T16:43:05.907869+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-13T16:43:05.938156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-13T16:43:05.970568+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-13T16:43:05.978331+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-13T16:43:05.985696+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-13T16:43:05.993207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-13T16:43:06.002596+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-13T16:43:06.010272+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-13T16:43:06.018171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-13T16:43:06.027861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-13T16:43:06.035987+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-13T16:43:06.069942+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-13T16:43:06.079156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-13T16:43:06.086444+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-13T16:43:06.097519+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-13T16:43:06.131544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-13T16:43:06.165028+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-13T16:43:06.197469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-13T16:43:06.224481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-13T16:43:06.251625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-13T16:43:06.278203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-13T16:43:06.310556+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-13T16:43:06.342846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-13T16:43:06.374334+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-13T16:43:06.410352+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-13T16:43:06.438509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-13T16:43:06.473524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-13T16:43:06.508586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-13T16:43:06.548647+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-13T16:43:06.576408+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-13T16:43:06.609245+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-13T16:43:06.635637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-13T16:43:06.662729+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-13T16:43:06.691741+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-13T16:43:06.718515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-13T16:43:06.756584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-13T16:43:06.787003+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-13T16:43:06.817222+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-13T16:43:06.848233+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-13T16:43:06.882641+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-13T16:43:06.909614+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-13T16:43:06.938027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-13T16:43:06.965857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-13T16:43:06.994776+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-13T16:43:07.026329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-13T16:43:07.051668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-13T16:43:07.081541+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-13T16:43:07.111422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-13T16:43:07.135653+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-13T16:43:07.138295+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-13T16:43:07.141339+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-13T16:43:07.143854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-13T16:43:07.146004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-13T16:43:07.148428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-13T16:43:07.150808+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-13T16:43:07.154053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-13T16:43:07.156754+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-13T16:43:07.180743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-13T16:43:07.185066+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-13T16:43:07.187608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-13T16:43:07.190586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-13T16:43:07.192580+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-13T16:43:07.201430+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-13T16:43:07.206392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-13T16:43:07.214127+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-13T16:43:07.224477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-13T16:43:07.227673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-13T16:43:08.015918+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-13T16:43:08.543834+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-13T16:43:09.007205+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-13T16:43:09.062995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-13T16:43:09.099671+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-13T16:43:09.129997+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-13T16:43:09.155523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-13T16:43:09.180076+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-13T16:43:09.207009+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-13T16:43:09.254965+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-13T16:43:09.283544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-13T16:43:09.311203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-13T16:43:09.339523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-13T16:43:09.367602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-13T16:43:09.390487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-13T16:43:09.399813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-13T16:43:09.406457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-13T16:43:09.409603+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-13T16:43:09.410549+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-13T16:43:09.419666+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-13T16:43:09.424559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-13T16:43:09.432457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-13T16:43:09.587108+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-13T16:43:09.670433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-13T16:43:10.066750+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-13T16:43:10.116450+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-13T16:43:10.154041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-13T16:43:10.183871+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-13T16:43:10.209103+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-13T16:43:10.235765+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-13T16:43:10.286909+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-13T16:43:10.314505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-13T16:43:10.340902+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-13T16:43:10.369634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-13T16:43:10.398471+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-13T16:43:10.425936+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-13T16:43:10.452982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-13T16:43:10.482637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-13T16:43:10.509068+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-13T16:43:10.535782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-13T16:43:10.752912+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-13T16:43:10.792778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-13T16:43:10.796610+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-13T16:43:10.805698+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-13T16:43:10.814087+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-13T16:43:10.816775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-13T16:43:10.825167+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-13T16:43:10.856842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-13T16:43:10.888625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-13T16:43:10.918633+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-13T16:43:48.649879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-13T16:43:48.681265+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-13T16:43:48.717963+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-13T16:43:48.748074+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-13T16:43:48.775844+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-13T16:43:48.801395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-13T16:43:48.843499+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-13T16:43:48.895470+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-13T16:43:48.937249+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-13T16:43:48.977975+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-13T16:43:49.017052+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-13T16:43:49.048354+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-13T16:43:49.076776+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-13T16:43:49.104371+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-13T16:43:49.141746+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-13T16:43:49.169615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-13T16:43:49.197746+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-13T16:43:49.225248+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-13T16:43:50.692364+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-13T16:45:06.914528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-13T16:45:06.977302+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-13T16:45:07.017517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-13T16:45:07.176988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-13T16:45:07.329231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-13T16:45:07.621420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-13T16:45:07.794398+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-13T16:45:08.070652+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-13T16:45:08.343655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-13T16:45:08.395685+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-13T16:45:08.450843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-13T16:45:08.486095+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-13T16:45:08.589372+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-13T16:45:08.616309+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-13T16:45:08.643209+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-13T16:45:08.936560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-13T16:45:08.962485+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-13T16:45:08.988976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-13T16:45:09.044325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-13T16:45:09.112710+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-13T16:45:09.155918+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-13T16:45:09.199792+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-13T16:45:09.245705+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-13T16:45:09.288520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-13T16:45:09.332869+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-13T16:45:09.374764+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-13T16:45:09.401926+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-13T16:45:09.556012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-13T16:45:09.755683+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-13T16:45:09.801166+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-13T16:45:09.882457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-13T16:45:09.950788+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-13T16:45:10.021667+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-13T16:45:10.116320+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-13T16:45:10.823220+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-13T16:45:11.482926+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression

### [2026-03-13T16:45:12.174976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-13T16:45:12.802722+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped

### [2026-03-13T16:45:13.530161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-13T16:45:14.182568+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-13T16:45:14.729731+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-13T16:45:14.783504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-13T16:45:14.881865+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-13T16:45:14.911030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-13T16:45:14.937977+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-13T16:45:14.962918+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-13T16:45:14.988995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-13T16:45:15.015346+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-13T16:45:15.042646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-13T16:45:15.069234+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-13T16:45:15.108388+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-13T16:45:15.180414+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-13T16:45:15.209811+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-13T16:45:15.254053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-13T16:45:15.308610+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-13T16:45:15.377478+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-13T16:45:15.407160+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-13T16:45:15.434929+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-13T16:45:15.460878+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-13T16:45:15.486933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-13T16:45:15.513065+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-13T16:45:15.539359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-13T16:45:15.565868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-13T16:45:15.591576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-13T16:45:15.618678+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-13T16:45:15.647548+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-13T16:45:15.676234+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-13T16:45:15.704322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-13T16:45:15.732707+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-13T16:45:15.762612+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-13T16:45:15.791284+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-13T16:45:15.819043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-13T16:45:15.848646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-13T16:45:15.876694+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-13T16:45:15.904125+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-13T16:45:15.932437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-13T16:45:15.959605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-13T16:45:15.986442+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-13T16:45:16.012727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-13T16:45:16.040249+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-13T16:45:16.067160+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-13T16:45:16.092787+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-13T16:45:16.118763+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-13T16:45:16.146179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-13T16:45:16.173952+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-13T16:45:16.201572+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-13T16:45:16.229389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-13T16:45:16.257223+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-13T16:45:16.286066+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-13T16:45:16.311785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-13T16:45:16.337894+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-13T16:45:16.370814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-13T16:45:16.400049+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-13T16:45:16.400843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-13T16:45:16.401559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-13T16:45:16.402243+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-13T16:45:16.403034+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-13T16:45:16.403919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-13T16:45:16.404634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-13T16:45:16.405329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-13T16:45:16.406169+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-13T16:45:16.407039+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-13T16:45:16.407743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-13T16:45:16.408453+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-13T16:45:16.409281+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-13T16:45:16.410061+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-13T16:45:16.410734+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-13T16:45:16.411537+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-13T16:45:16.412273+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-13T16:45:16.412940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-13T16:45:16.413919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-13T16:45:16.415028+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-13T16:45:16.415707+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-13T16:45:16.416341+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-13T16:45:16.417043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-13T16:45:16.417849+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-13T16:45:16.418489+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-13T16:45:16.419091+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-13T16:45:16.419727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-13T16:45:16.420765+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-13T16:45:16.506332+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-13T16:45:16.617395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-13T16:45:16.644772+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-13T16:45:16.687608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-13T16:45:16.716282+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-13T16:45:16.744235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-13T16:45:16.772242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-13T16:45:16.801249+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-13T16:45:16.830002+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-13T16:45:16.861307+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-13T16:45:16.892194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-13T16:45:16.923606+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-13T16:45:16.954385+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-13T16:45:16.984760+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-13T16:45:17.016660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-13T16:45:17.050409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-13T16:45:17.084004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-13T16:45:17.115005+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-13T16:45:17.146183+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-13T16:45:17.184462+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-13T16:45:17.211489+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-13T16:45:17.241220+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-13T16:45:17.268129+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-13T16:45:17.296274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-13T16:45:17.325764+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-13T16:45:17.353235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-13T16:45:17.380089+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-13T16:45:17.410175+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-13T16:45:17.436463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-13T16:45:17.467935+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-13T16:45:17.503803+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-13T16:45:17.544947+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-13T16:45:17.573717+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-13T16:45:17.616988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-13T16:45:17.617879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-13T16:45:17.618983+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-13T16:45:17.620380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-13T16:45:17.621215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-13T16:45:17.624475+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-13T16:45:17.625532+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-13T16:45:17.626450+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-13T16:45:17.627247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-13T16:45:17.627957+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-13T16:45:17.628787+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-13T16:45:17.629610+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-13T16:45:17.630320+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-13T16:45:17.631260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-13T16:45:17.632497+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-13T16:45:17.645051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-13T16:45:17.650535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-13T16:45:17.653987+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-13T16:45:17.659453+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-13T16:45:17.661590+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-13T16:45:17.663559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-13T16:45:17.702876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-13T16:45:17.730033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-13T16:45:17.756717+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-13T16:45:17.783224+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-13T16:45:17.809954+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-13T16:45:17.838957+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-13T16:45:17.865636+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-13T16:45:17.891441+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-13T16:45:17.918607+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-13T16:45:17.946554+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-13T16:45:17.974149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts

### [2026-03-13T16:45:18.001717+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-13T16:45:18.047609+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-13T16:45:18.087264+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-13T16:45:18.112504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-13T16:45:18.136927+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-13T16:45:18.164745+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-13T16:45:18.189559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-13T16:45:18.214431+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-13T16:45:18.239124+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-13T16:45:18.721542+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-13T16:47:56.498233+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=592 failed=0 exit=2

### [2026-03-13T16:47:57.353796+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T164757Z pid=96431 python=3.12.3

### [2026-03-13T16:47:57.378888+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-x', '-q', '--tb=short']

### [2026-03-13T16:47:58.094438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-13T16:47:58.125510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-13T16:47:58.166753+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-13T16:47:58.195130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-13T16:47:58.225351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-13T16:47:58.271808+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-13T16:47:58.310480+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-13T16:47:58.559314+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-13T16:47:58.588891+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-13T16:47:58.621161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-13T16:47:58.651015+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-13T16:47:58.677681+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-13T16:47:58.687148+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-13T16:47:58.689473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-13T16:47:58.692166+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-13T16:47:58.700723+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-13T16:47:58.707939+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-13T16:47:58.731657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-13T16:47:58.734425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-13T16:47:58.737320+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-13T16:47:58.740209+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-13T16:47:58.776135+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-13T16:47:58.810171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-13T16:47:58.839524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-13T16:47:58.864521+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-13T16:47:58.865520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-13T16:47:58.867861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-13T16:47:58.869508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-13T16:47:58.871467+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-13T16:47:58.873653+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-13T16:47:58.875580+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-13T16:47:58.876698+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-13T16:47:58.947761+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-13T16:47:59.030136+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-13T16:47:59.235118+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-13T16:47:59.363295+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-13T16:47:59.439866+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-13T16:47:59.471691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-13T16:47:59.499115+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-13T16:47:59.525169+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-13T16:47:59.551655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-13T16:47:59.576043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-13T16:47:59.601365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-13T16:47:59.629082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-13T16:47:59.658304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-13T16:47:59.746241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-13T16:47:59.816922+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-13T16:47:59.847704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-13T16:47:59.905983+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-13T16:48:00.254348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-13T16:48:00.669203+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=592 failed=0 exit=2

### [2026-03-13T16:48:01.126140+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T164801Z pid=97310 python=3.12.3

### [2026-03-13T16:48:01.126957+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-x', '-q', '--tb=line']

### [2026-03-13T16:48:01.585980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-13T16:48:01.652668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-13T16:48:01.705420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-13T16:48:01.734484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-13T16:48:01.765203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-13T16:48:01.811666+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-13T16:48:01.845838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-13T16:48:02.114693+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-13T16:48:02.118718+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-13T16:48:02.156786+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-13T16:48:02.185348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-13T16:48:02.210743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-13T16:48:02.242944+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-13T16:48:02.267372+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-13T16:48:02.270107+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-13T16:48:02.278481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-13T16:48:02.310790+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-13T16:48:02.358709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-13T16:48:02.387221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-13T16:48:02.414434+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-13T16:48:02.441743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-13T16:48:02.502452+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-13T16:48:02.535856+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-13T16:48:02.563441+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-13T16:48:02.593623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-13T16:48:02.619268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-13T16:48:02.645864+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-13T16:48:02.671507+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-13T16:48:02.697877+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-13T16:48:02.728392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-13T16:48:02.757532+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-13T16:48:02.784122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-13T16:48:02.896932+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-13T16:48:02.978527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-13T16:48:03.169951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-13T16:48:03.286380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-13T16:48:03.365457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-13T16:48:03.391371+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-13T16:48:03.393560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-13T16:48:03.395936+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-13T16:48:03.398055+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-13T16:48:03.399982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-13T16:48:03.401952+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-13T16:48:03.430712+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-13T16:48:03.457831+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-13T16:48:03.536816+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-13T16:48:03.610097+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-13T16:48:03.641995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-13T16:48:03.721786+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-13T16:48:04.053893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-13T16:48:04.622301+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=592 failed=0 exit=2

### [2026-03-13T16:48:05.005088+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T164805Z pid=98261 python=3.12.3

### [2026-03-13T16:48:05.032199+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-q', '--tb=line']

### [2026-03-13T16:48:05.544099+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-13T16:48:05.578259+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-13T16:48:05.621559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-13T16:48:05.625477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-13T16:48:05.633030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-13T16:48:05.653938+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-13T16:48:05.690830+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-13T16:48:05.954766+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-13T16:48:05.982041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-13T16:48:06.008425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-13T16:48:06.034067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-13T16:48:06.062682+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-13T16:48:06.094931+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-13T16:48:06.122182+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-13T16:48:06.149447+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-13T16:48:06.185414+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-13T16:48:06.218614+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-13T16:48:06.272081+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-13T16:48:06.297936+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-13T16:48:06.324249+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-13T16:48:06.351687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-13T16:48:06.410569+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-13T16:48:06.443436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-13T16:48:06.473284+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-13T16:48:06.501030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-13T16:48:06.528258+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-13T16:48:06.558798+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-13T16:48:06.586812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-13T16:48:06.619849+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-13T16:48:06.648791+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-13T16:48:06.678366+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-13T16:48:06.705759+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-13T16:48:06.800699+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-13T16:48:06.881241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-13T16:48:07.084904+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-13T16:48:07.216837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-13T16:48:07.295945+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-13T16:48:07.323711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-13T16:48:07.350764+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-13T16:48:07.376686+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-13T16:48:07.401876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-13T16:48:07.426636+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-13T16:48:07.451192+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-13T16:48:07.476347+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-13T16:48:07.504006+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-13T16:48:07.586736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-13T16:48:07.662710+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-13T16:48:07.695135+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-13T16:48:07.773194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-13T16:48:08.133568+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-13T16:48:08.560688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-13T16:48:08.601352+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-13T16:48:08.657740+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-13T16:48:08.697937+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-13T16:48:08.739701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-13T16:48:08.773608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-13T16:48:08.802666+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-13T16:48:08.831726+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-13T16:48:08.906638+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-13T16:48:08.938770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-13T16:48:08.941737+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-13T16:48:08.944651+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-13T16:48:08.950956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-13T16:48:08.953427+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-13T16:48:08.959768+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-13T16:48:08.962191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-13T16:48:08.964846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-13T16:48:08.966830+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-13T16:48:08.969326+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-13T16:48:08.970826+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-13T16:48:08.972501+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-13T16:48:08.974077+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-13T16:48:08.975764+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-13T16:48:08.977103+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-13T16:48:08.978646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-13T16:48:08.980655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-13T16:48:08.984266+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-13T16:48:08.986255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-13T16:48:08.988422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-13T16:48:08.991627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-13T16:48:08.993805+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-13T16:48:08.998376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-13T16:48:09.001177+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-13T16:48:09.004186+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-13T16:48:09.011887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-13T16:48:09.040781+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-13T16:48:09.070638+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-13T16:48:09.078498+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-13T16:48:09.100067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-13T16:48:09.138300+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-13T16:48:09.140792+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-13T16:48:09.143399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-13T16:48:09.145659+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-13T16:48:09.147918+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-13T16:48:09.149925+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-13T16:48:09.152017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-13T16:48:09.153652+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-13T16:48:09.155631+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-13T16:48:09.157391+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-13T16:48:09.159590+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-13T16:48:09.161163+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-13T16:48:09.163282+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-13T16:48:09.165133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-13T16:48:09.166679+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-13T16:48:09.168389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-13T16:48:09.170328+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-13T16:48:09.172485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-13T16:48:09.175190+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-13T16:48:09.209123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-13T16:48:09.236761+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-13T16:48:09.263238+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-13T16:48:09.288673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-13T16:48:09.313691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-13T16:48:09.340288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-13T16:48:09.370321+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-13T16:48:09.397298+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-13T16:48:09.674519+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-13T16:48:09.717602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-13T16:48:09.763526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-13T16:48:09.789487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-13T16:48:09.815323+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-13T16:48:09.843970+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-13T16:48:09.878372+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-13T16:48:09.934207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-13T16:48:09.961462+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-13T16:48:09.990385+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-13T16:48:10.018514+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-13T16:48:10.048422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-13T16:48:10.049764+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-13T16:48:10.058540+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-13T16:48:10.142106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-13T16:48:10.178323+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-13T16:48:10.206324+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-13T16:48:10.235553+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-13T16:48:10.269486+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-13T16:48:10.298377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-13T16:48:10.320915+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-13T16:48:10.322227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-13T16:48:10.323363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-13T16:48:10.324373+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-13T16:48:10.331250+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-13T16:48:10.332578+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-13T16:48:10.333723+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-13T16:48:10.334433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-13T16:48:10.335437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-13T16:48:10.336418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-13T16:48:10.337342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-13T16:48:10.346994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-13T16:48:10.374515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-13T16:48:10.401722+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-13T16:48:10.427963+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-13T16:48:10.451795+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-13T16:48:10.479598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-13T16:48:10.519649+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-13T16:48:10.546473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-13T16:48:10.573292+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-13T16:48:10.601342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-13T16:48:10.631189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-13T16:48:10.674847+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-13T16:48:10.701697+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-13T16:48:10.729683+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-13T16:48:10.756919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-13T16:48:10.797216+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-13T16:48:10.826206+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-13T16:48:10.855748+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-13T16:48:10.885877+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-13T16:48:10.930828+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-13T16:48:10.958412+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-13T16:48:10.986569+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-13T16:48:10.989390+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-13T16:48:10.999354+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-13T16:48:11.186825+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=592 failed=0 exit=2

### [2026-03-13T16:48:20.397249+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T164820Z pid=2273 python=3.12.3

### [2026-03-13T16:48:20.397875+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-q', '--tb=line', '--no-header']

### [2026-03-13T16:48:20.857830+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-13T16:48:20.889901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-13T16:48:20.931739+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-13T16:48:20.959408+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-13T16:48:20.989821+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-13T16:48:21.035379+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-13T16:48:21.070966+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-13T16:48:21.335457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-13T16:48:21.374436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-13T16:48:21.583338+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-13T16:48:21.613583+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-13T16:48:21.646843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-13T16:48:21.683634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-13T16:48:21.712475+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-13T16:48:21.742924+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-13T16:48:21.785460+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-13T16:48:21.820276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-13T16:48:21.872924+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-13T16:48:21.900806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-13T16:48:21.927832+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-13T16:48:21.953671+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-13T16:48:22.024591+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-13T16:48:22.057512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-13T16:48:22.085459+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-13T16:48:22.113979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-13T16:48:22.141030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-13T16:48:22.167437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-13T16:48:22.193219+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-13T16:48:22.219899+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-13T16:48:22.246165+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-13T16:48:22.273846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-13T16:48:22.415085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-13T16:48:22.496442+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-13T16:48:22.557435+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-13T16:48:22.943406+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-13T16:48:23.077700+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-13T16:48:23.155481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-13T16:48:23.184247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-13T16:48:23.210787+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-13T16:48:23.236720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-13T16:48:23.262184+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-13T16:48:23.288567+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-13T16:48:23.314796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-13T16:48:23.341026+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-13T16:48:23.369683+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-13T16:48:23.464851+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-13T16:48:23.551811+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-13T16:48:23.598437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-13T16:48:23.653289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-13T16:48:23.981404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-13T16:48:24.462948+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-13T16:48:24.503058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-13T16:48:24.557902+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-13T16:48:24.779095+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-13T16:48:24.796649+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-13T16:48:24.800063+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-13T16:48:24.804117+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-13T16:48:24.834361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-13T16:48:24.900774+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-13T16:48:24.929186+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-13T16:48:24.957232+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-13T16:48:24.986182+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-13T16:48:25.017468+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-13T16:48:25.045610+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-13T16:48:25.077324+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-13T16:48:25.105714+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-13T16:48:25.135252+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-13T16:48:25.164434+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-13T16:48:25.194725+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-13T16:48:25.224750+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-13T16:48:25.255194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-13T16:48:25.282957+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-13T16:48:25.314216+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-13T16:48:25.341976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-13T16:48:25.371373+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-13T16:48:25.400797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-13T16:48:25.437404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-13T16:48:25.467971+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-13T16:48:25.513866+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-13T16:48:25.562664+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-13T16:48:25.593053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-13T16:48:25.629221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-13T16:48:25.660311+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-13T16:48:25.692194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-13T16:48:25.727582+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-13T16:48:25.806337+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-13T16:48:25.850334+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-13T16:48:25.902725+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-13T16:48:25.946191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-13T16:48:25.961370+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-13T16:48:25.964039+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-13T16:48:25.966898+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-13T16:48:25.968761+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-13T16:48:25.970829+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-13T16:48:25.972705+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-13T16:48:25.974814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-13T16:48:25.976447+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-13T16:48:25.978504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-13T16:48:25.980303+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-13T16:48:25.982696+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-13T16:48:25.984618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-13T16:48:25.986393+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-13T16:48:25.988401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-13T16:48:25.989903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-13T16:48:25.991636+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-13T16:48:25.993438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-13T16:48:25.995797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-13T16:48:25.998895+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-13T16:48:26.007030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-13T16:48:26.009122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-13T16:48:26.010879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-13T16:48:26.012719+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-13T16:48:26.014472+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-13T16:48:26.043274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-13T16:48:26.071480+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-13T16:48:26.099029+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-13T16:48:26.380857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-13T16:48:26.701861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-13T16:48:26.747168+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-13T16:48:26.775378+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-13T16:48:26.802287+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-13T16:48:26.831395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-13T16:48:26.868726+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-13T16:48:26.922747+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-13T16:48:26.950822+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-13T16:48:26.983430+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-13T16:48:26.988350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-13T16:48:26.993138+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-13T16:48:27.274134+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-13T16:48:27.313908+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-13T16:48:27.403848+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-13T16:48:27.444674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-13T16:48:27.473535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-13T16:48:27.503621+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-13T16:48:27.536488+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-13T16:48:27.563786+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-13T16:48:27.589340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-13T16:48:27.614151+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-13T16:48:27.640881+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-13T16:48:27.670059+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-13T16:48:27.711844+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-13T16:48:27.737492+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-13T16:48:27.764615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-13T16:48:27.791380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-13T16:48:27.821518+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-13T16:48:27.848384+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-13T16:48:27.880133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-13T16:48:27.932916+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-13T16:48:28.188300+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-13T16:48:28.219687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-13T16:48:28.247990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-13T16:48:28.274757+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-13T16:48:28.304643+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-13T16:48:28.341806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-13T16:48:28.369745+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-13T16:48:28.397473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-13T16:48:28.425168+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-13T16:48:28.452048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-13T16:48:28.488508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-13T16:48:28.517967+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-13T16:48:28.547780+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-13T16:48:28.576644+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-13T16:48:28.619665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-13T16:48:28.646841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-13T16:48:28.676495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-13T16:48:28.703796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-13T16:48:28.744853+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-13T16:48:28.772136+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-13T16:48:28.801147+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-13T16:48:28.830796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-13T16:48:28.861830+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-13T16:48:28.930574+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-13T16:48:28.958711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-13T16:48:28.984323+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-13T16:48:29.011296+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-13T16:48:29.038469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-13T16:48:29.070056+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-13T16:48:29.098757+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T16:48:29.128448+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T16:48:29.170559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T16:48:29.198812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T16:48:29.226935+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T16:48:29.268798+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T16:48:29.310785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T16:48:29.313213+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T16:48:29.316295+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T16:48:29.319384+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T16:48:29.347872+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T16:48:29.351657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T16:48:29.354047+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T16:48:29.356573+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T16:48:29.358196+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T16:48:29.360018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T16:48:29.361712+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T16:48:29.363401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T16:48:29.370365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T16:48:29.372530+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T16:48:29.374675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T16:48:29.376728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T16:48:29.378200+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T16:48:29.380041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T16:48:29.382068+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-13T16:48:29.384546+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-13T16:48:29.388222+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-13T16:48:29.393062+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-13T16:48:29.402194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-13T16:48:29.410561+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-13T16:48:29.443564+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-13T16:48:29.471964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-13T16:48:29.505327+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-13T16:48:29.539980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-13T16:48:29.574013+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-13T16:48:29.606833+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-13T16:48:29.639076+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-13T16:48:29.678584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-13T16:48:29.824385+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-13T16:48:30.927531+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-13T16:48:30.953614+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-13T16:48:30.984887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-13T16:48:31.016560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-13T16:48:31.043582+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-13T16:48:31.069457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-13T16:48:31.095976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-13T16:48:31.122380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-13T16:48:31.161777+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-13T16:48:31.195355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-13T16:48:31.226382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-13T16:48:31.259240+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-13T16:48:31.287712+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-13T16:48:31.314477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-13T16:48:31.338997+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-13T16:48:31.363487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-13T16:48:31.396318+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-13T16:48:31.428562+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-13T16:48:31.457852+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-13T16:48:31.487959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-13T16:48:31.518238+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-13T16:48:31.572602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-13T16:48:31.584700+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-13T16:48:31.591176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-13T16:48:31.592788+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-13T16:48:31.593785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-13T16:48:31.594696+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-13T16:48:31.595665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-13T16:48:31.596509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-13T16:48:31.597359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-13T16:48:31.598669+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-13T16:48:31.599598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-13T16:48:31.600582+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-13T16:48:31.601446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-13T16:48:31.602375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-13T16:48:31.603545+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-13T16:48:31.609597+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-13T16:48:31.613224+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-13T16:48:31.637074+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-13T16:48:31.643301+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-13T16:48:31.644659+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-13T16:48:31.645355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-13T16:48:31.646000+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-13T16:48:31.646748+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-13T16:48:31.651312+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-13T16:48:31.655563+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-13T16:48:31.662324+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-13T16:48:31.668538+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-13T16:48:31.674527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-13T16:48:31.680634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-13T16:48:31.687390+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-13T16:48:31.694709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-13T16:48:31.701064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-13T16:48:31.709954+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-13T16:48:31.716654+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-13T16:48:31.752658+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-13T16:48:31.785048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-13T16:48:31.816310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-13T16:48:31.847758+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-13T16:48:31.879615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-13T16:48:31.910463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-13T16:48:31.941611+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-13T16:48:31.969362+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-13T16:48:32.295421+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-13T16:48:32.463946+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-13T16:48:32.505319+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-13T16:48:32.543212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-13T16:48:32.573657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-13T16:48:32.612709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-13T16:48:32.642570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-13T16:48:32.674102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-13T16:48:32.682080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-13T16:48:32.705765+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-13T16:48:32.711895+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-13T16:48:32.724003+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-13T16:48:32.727644+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-13T16:48:32.729462+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-13T16:48:32.731784+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-13T16:48:32.734670+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-13T16:48:32.737570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-13T16:48:32.740705+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-13T16:48:32.743701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-13T16:48:32.751893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-13T16:48:32.787120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-13T16:48:32.815432+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-13T16:48:32.843466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-13T16:48:32.871908+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-13T16:48:32.898466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-13T16:48:32.928009+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-13T16:48:32.954189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-13T16:48:32.980161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-13T16:48:33.009806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-13T16:48:33.036258+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-13T16:48:33.244957+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-13T16:48:33.271924+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-13T16:48:33.299246+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-13T16:48:33.326699+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-13T16:48:33.353549+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-13T16:48:33.380263+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-13T16:48:33.410974+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-13T16:48:33.440996+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-13T16:48:33.467163+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-13T16:48:33.499293+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-13T16:48:33.532484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-13T16:48:33.564381+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-13T16:48:33.590350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-13T16:48:33.617822+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-13T16:48:33.645566+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-13T16:48:33.671822+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-13T16:48:33.700510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-13T16:48:33.727585+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-13T16:48:34.269801+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-13T16:48:34.693529+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-13T16:48:35.203438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-13T16:48:35.233817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-13T16:48:35.260271+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-13T16:48:35.288407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-13T16:48:35.315613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-13T16:48:35.342778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-13T16:48:35.370790+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-13T16:48:35.407096+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-13T16:48:35.434908+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-13T16:48:35.463606+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-13T16:48:35.492496+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-13T16:48:35.522241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-13T16:48:35.553912+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-13T16:48:35.593727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-13T16:48:35.626161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-13T16:48:35.657021+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-13T16:48:35.683329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-13T16:48:35.718093+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-13T16:48:35.748270+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-13T16:48:35.779402+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-13T16:48:35.890623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-13T16:48:35.975626+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-13T16:48:36.371791+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-13T16:48:36.691000+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-13T16:48:36.730094+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-13T16:48:36.760851+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-13T16:48:36.786742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-13T16:48:36.812913+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-13T16:48:36.856554+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-13T16:48:36.884513+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-13T16:48:36.910484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-13T16:48:36.940533+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-13T16:48:36.970257+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-13T16:48:36.996955+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-13T16:48:37.024936+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-13T16:48:37.058167+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-13T16:48:37.085449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-13T16:48:37.111008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-13T16:48:37.321836+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-13T16:48:37.361268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-13T16:48:37.390836+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-13T16:48:37.426221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-13T16:48:37.459716+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-13T16:48:37.488694+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-13T16:48:37.523038+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-13T16:48:37.675792+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-13T16:48:37.709147+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-13T16:48:37.740210+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-13T16:49:15.626716+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-13T16:49:15.654297+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-13T16:49:15.689410+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-13T16:49:15.718541+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-13T16:49:15.746895+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-13T16:49:15.771798+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-13T16:49:15.811905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-13T16:49:15.860204+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-13T16:49:15.898285+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-13T16:49:15.936710+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-13T16:49:15.976403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-13T16:49:16.008100+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-13T16:49:16.035948+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-13T16:49:16.063702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-13T16:49:16.098701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-13T16:49:16.126812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-13T16:49:16.155334+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-13T16:49:16.185030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-13T16:49:16.789118+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-13T16:50:32.857083+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-13T16:50:32.891557+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-13T16:50:32.935500+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-13T16:50:33.084623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-13T16:50:33.233577+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-13T16:50:33.525299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-13T16:50:33.691928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-13T16:50:33.951529+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-13T16:50:34.201468+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-13T16:50:34.251323+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-13T16:50:34.297950+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-13T16:50:34.353460+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-13T16:50:34.449520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-13T16:50:34.475938+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-13T16:50:34.502235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-13T16:50:34.856446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-13T16:50:34.884193+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-13T16:50:34.910715+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-13T16:50:34.955040+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-13T16:50:35.010233+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-13T16:50:35.220368+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-13T16:50:35.265317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-13T16:50:35.311703+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-13T16:50:35.353551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-13T16:50:35.396781+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-13T16:50:35.438096+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-13T16:50:35.463560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-13T16:50:35.540268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-13T16:50:35.740059+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-13T16:50:35.801769+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-13T16:50:35.883570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-13T16:50:35.953348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-13T16:50:36.023855+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-13T16:50:36.120035+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-13T16:50:36.742708+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-13T16:50:37.394191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression

### [2026-03-13T16:50:37.982681+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-13T16:50:38.630121+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped

### [2026-03-13T16:50:39.315466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-13T16:50:39.933268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-13T16:50:40.461185+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-13T16:50:40.511646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-13T16:50:40.612274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-13T16:50:40.639448+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-13T16:50:40.668301+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-13T16:50:40.695583+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-13T16:50:40.721576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-13T16:50:40.747820+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-13T16:50:40.776242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-13T16:50:40.802345+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-13T16:50:40.838933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-13T16:50:40.905157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-13T16:50:40.932099+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-13T16:50:40.958082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-13T16:50:40.983719+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-13T16:50:41.010452+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-13T16:50:41.035876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-13T16:50:41.062410+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-13T16:50:41.088570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-13T16:50:41.113881+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-13T16:50:41.142055+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-13T16:50:41.170162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-13T16:50:41.196509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-13T16:50:41.222575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-13T16:50:41.249377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-13T16:50:41.276483+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-13T16:50:41.303523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-13T16:50:41.334189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-13T16:50:41.335001+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-13T16:50:41.335882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-13T16:50:41.336697+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-13T16:50:41.337642+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-13T16:50:41.338363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-13T16:50:41.339352+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-13T16:50:41.340276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-13T16:50:41.341606+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-13T16:50:41.342349+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-13T16:50:41.343280+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-13T16:50:41.344359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-13T16:50:41.345376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-13T16:50:41.346245+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-13T16:50:41.347097+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-13T16:50:41.347841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-13T16:50:41.348881+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-13T16:50:41.349851+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-13T16:50:41.350759+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-13T16:50:41.351501+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-13T16:50:41.352177+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-13T16:50:41.352817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-13T16:50:41.353649+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-13T16:50:41.354350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-13T16:50:41.355109+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-13T16:50:41.355813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-13T16:50:41.356541+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-13T16:50:41.357196+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-13T16:50:41.358030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-13T16:50:41.358860+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-13T16:50:41.359778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-13T16:50:41.360639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-13T16:50:41.361702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-13T16:50:41.362386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-13T16:50:41.363103+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-13T16:50:41.363799+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-13T16:50:41.364489+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-13T16:50:41.365397+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-13T16:50:41.366467+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-13T16:50:41.367324+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-13T16:50:41.368195+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-13T16:50:41.369044+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-13T16:50:41.370046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-13T16:50:41.370655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-13T16:50:41.371650+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-13T16:50:41.372622+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-13T16:50:41.373758+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-13T16:50:41.374918+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-13T16:50:41.375926+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-13T16:50:41.377088+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-13T16:50:41.377933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-13T16:50:41.379037+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-13T16:50:41.380070+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-13T16:50:41.446179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-13T16:50:41.510048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-13T16:50:41.514124+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-13T16:50:41.523026+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-13T16:50:41.572428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-13T16:50:41.601004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-13T16:50:41.630689+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-13T16:50:41.662854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-13T16:50:41.692472+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-13T16:50:41.720953+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-13T16:50:41.749358+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-13T16:50:41.777375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-13T16:50:41.805536+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-13T16:50:41.836977+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-13T16:50:41.869020+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-13T16:50:41.899765+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-13T16:50:41.932728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-13T16:50:41.963350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-13T16:50:41.993775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-13T16:50:42.031711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-13T16:50:42.297014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-13T16:50:42.329634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-13T16:50:42.358198+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-13T16:50:42.388162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-13T16:50:42.414008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-13T16:50:42.440401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-13T16:50:42.467115+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-13T16:50:42.494437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-13T16:50:42.520936+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-13T16:50:42.549079+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-13T16:50:42.571704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-13T16:50:42.572503+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-13T16:50:42.573713+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-13T16:50:42.574883+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-13T16:50:42.575589+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-13T16:50:42.576549+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-13T16:50:42.577594+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-13T16:50:42.578363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-13T16:50:42.581304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-13T16:50:42.582231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-13T16:50:42.583184+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-13T16:50:42.584056+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-13T16:50:42.584834+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-13T16:50:42.585544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-13T16:50:42.586288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-13T16:50:42.587014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-13T16:50:42.587676+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-13T16:50:42.588709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-13T16:50:42.600310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-13T16:50:42.606048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-13T16:50:42.610641+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-13T16:50:42.616661+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-13T16:50:42.619379+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-13T16:50:42.666486+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-13T16:50:42.717890+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-13T16:50:42.741995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-13T16:50:42.743004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-13T16:50:42.743877+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-13T16:50:42.744710+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-13T16:50:42.747614+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-13T16:50:42.748337+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-13T16:50:42.749091+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-13T16:50:42.749810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-13T16:50:42.750719+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-13T16:50:42.751770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts

### [2026-03-13T16:50:42.752477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-13T16:50:42.767201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-13T16:50:42.805882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-13T16:50:42.832204+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-13T16:50:42.858159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-13T16:50:42.886725+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-13T16:50:42.912976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-13T16:50:42.939299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-13T16:50:42.965959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-13T16:50:43.065899+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-13T16:56:19.002824+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_run_noop

### [2026-03-13T16:56:19.005986+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=592 failed=0 exit=0

### [2026-03-13T16:57:13.751320+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T165713Z pid=12351 python=3.12.3

### [2026-03-13T16:57:14.119271+00:00] > **Agent**

 [features]  
run_features START

### [2026-03-13T17:01:28.328275+00:00] > **Agent**

 [features]  
run_features COMPLETE

### [2026-03-13T17:01:36.161245+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T170136Z pid=65876 python=3.12.3

### [2026-03-13T17:01:36.732198+00:00] > **Agent**

 [models]  
run_models START

### [2026-03-13T17:01:38.959918+00:00] > **Agent**

 [models]  
run_models COMPLETE

### [2026-03-13T17:02:45.834666+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T170245Z pid=80103 python=3.12.3

### [2026-03-13T17:02:45.861949+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-q', '--tb=line', '--no-header']

### [2026-03-13T17:02:46.429382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-13T17:02:46.477911+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-13T17:02:46.522256+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-13T17:02:46.552318+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-13T17:02:46.615469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-13T17:02:46.670362+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-13T17:02:46.742300+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-13T17:02:47.023898+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-13T17:02:47.054456+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-13T17:02:47.063861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-13T17:02:47.192887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-13T17:02:47.223510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-13T17:02:47.290559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-13T17:02:47.360066+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-13T17:02:47.446568+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-13T17:02:47.519297+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-13T17:02:47.621156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-13T17:02:47.762076+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-13T17:02:47.842474+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-13T17:02:47.920898+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-13T17:02:47.998539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-13T17:02:48.077411+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-13T17:02:48.215094+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-13T17:02:48.245724+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-13T17:02:48.336573+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-13T17:02:48.419934+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-13T17:02:48.485937+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-13T17:02:48.528194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-13T17:02:48.556934+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-13T17:02:48.584723+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-13T17:02:48.614732+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-13T17:02:48.646616+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-13T17:02:48.748626+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-13T17:02:48.828126+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-13T17:02:49.091096+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-13T17:02:49.252782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-13T17:02:49.421741+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-13T17:02:49.475556+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-13T17:02:49.504326+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-13T17:02:49.658351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-13T17:02:49.731933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-13T17:02:49.782594+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-13T17:02:49.858418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-13T17:02:49.923629+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-13T17:02:49.971684+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-13T17:02:50.105895+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-13T17:02:50.217446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-13T17:02:50.336528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-13T17:02:50.420217+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-13T17:02:50.836318+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-13T17:02:51.266004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-13T17:02:51.339917+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-13T17:02:51.435383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-13T17:02:51.510546+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-13T17:02:51.581985+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-13T17:02:51.640798+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-13T17:02:51.699753+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-13T17:02:51.757651+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-13T17:02:51.841246+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-13T17:02:51.868598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-13T17:02:51.871743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-13T17:02:51.875102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-13T17:02:51.881625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-13T17:02:51.885739+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-13T17:02:51.893381+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-13T17:02:51.930183+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-13T17:02:51.966228+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-13T17:02:52.034275+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-13T17:02:52.109964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-13T17:02:52.177261+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-13T17:02:52.219028+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-13T17:02:52.248990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-13T17:02:52.280900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-13T17:02:52.313051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-13T17:02:52.345345+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-13T17:02:52.403980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-13T17:02:52.458286+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-13T17:02:52.489514+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-13T17:02:52.661973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-13T17:02:52.718031+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-13T17:02:52.805988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-13T17:02:52.911668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-13T17:02:53.045327+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-13T17:02:53.140163+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-13T17:02:53.182747+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-13T17:02:53.276567+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-13T17:02:53.329888+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-13T17:02:53.367737+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-13T17:02:53.419050+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-13T17:02:53.504629+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-13T17:02:53.566085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-13T17:02:53.658300+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-13T17:02:53.694101+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-13T17:02:53.804007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-13T17:02:53.926940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-13T17:02:53.982854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-13T17:02:54.073654+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-13T17:02:54.141530+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-13T17:02:54.226630+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-13T17:02:54.263512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-13T17:02:54.272577+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-13T17:02:54.312646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-13T17:02:54.393884+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-13T17:02:54.429700+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-13T17:02:54.576164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-13T17:02:54.765736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-13T17:02:54.805598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-13T17:02:54.885666+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-13T17:02:54.983486+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-13T17:02:55.038673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-13T17:02:55.070959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-13T17:02:55.115771+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-13T17:02:55.270255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-13T17:02:55.374821+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-13T17:02:55.482842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-13T17:02:55.552193+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-13T17:02:55.844029+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-13T17:02:55.917496+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-13T17:02:55.988317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-13T17:02:56.020386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-13T17:02:56.049470+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-13T17:02:56.110079+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-13T17:02:56.150185+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-13T17:02:56.301998+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-13T17:02:56.400977+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-13T17:02:56.472310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-13T17:02:56.528607+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-13T17:02:56.605171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-13T17:02:56.641984+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-13T17:02:56.681873+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-13T17:02:56.759215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-13T17:02:56.847269+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-13T17:02:56.888964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-13T17:02:56.984593+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-13T17:02:57.063981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-13T17:02:57.139242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-13T17:02:57.198511+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-13T17:02:57.231575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-13T17:02:57.262991+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-13T17:02:57.332392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-13T17:02:57.425055+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-13T17:02:57.472461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-13T17:02:57.538982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-13T17:02:57.627676+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-13T17:02:57.699877+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-13T17:02:57.758058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-13T17:02:57.786851+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-13T17:02:57.921113+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-13T17:02:57.928007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-13T17:02:58.047553+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-13T17:02:58.085520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-13T17:02:58.121225+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-13T17:02:58.156491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-13T17:02:58.209959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-13T17:02:58.279204+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-13T17:02:58.313332+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-13T17:02:58.370217+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-13T17:02:58.437894+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-13T17:02:58.497799+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-13T17:02:58.545742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-13T17:02:58.623159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-13T17:02:58.702869+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-13T17:02:58.776691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-13T17:02:58.832252+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-13T17:02:58.883934+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-13T17:02:58.926414+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-13T17:02:58.989608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-13T17:02:59.040535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-13T17:02:59.076926+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-13T17:02:59.106143+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-13T17:02:59.138094+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-13T17:02:59.164514+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-13T17:02:59.175919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-13T17:02:59.190196+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-13T17:02:59.207682+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-13T17:02:59.297658+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-13T17:02:59.335372+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-13T17:02:59.404053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T17:02:59.466684+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T17:02:59.544421+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T17:02:59.576211+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T17:02:59.650681+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T17:02:59.730611+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T17:02:59.767689+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T17:02:59.833736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T17:02:59.869711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T17:02:59.983772+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T17:03:00.141017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T17:03:00.281046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T17:03:00.416775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T17:03:00.582618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T17:03:00.656051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T17:03:00.699161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T17:03:00.818500+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T17:03:00.975060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T17:03:01.046764+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T17:03:01.083804+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T17:03:01.122653+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T17:03:01.176564+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T17:03:01.211800+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T17:03:01.339794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T17:03:01.442757+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-13T17:03:01.562910+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-13T17:03:01.620031+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-13T17:03:01.661601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-13T17:03:01.700209+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-13T17:03:01.710897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-13T17:03:01.721868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-13T17:03:01.725245+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-13T17:03:01.735136+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-13T17:03:01.745390+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-13T17:03:01.756552+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-13T17:03:01.772272+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-13T17:03:01.784565+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-13T17:03:01.829971+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-13T17:03:01.901333+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-13T17:03:02.752874+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-13T17:03:02.795810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-13T17:03:02.834294+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-13T17:03:02.874443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-13T17:03:03.008061+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-13T17:03:03.074742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-13T17:03:03.111723+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-13T17:03:03.142077+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-13T17:03:03.177448+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-13T17:03:03.209441+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-13T17:03:03.241304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-13T17:03:03.276530+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-13T17:03:03.306113+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-13T17:03:03.334937+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-13T17:03:03.401679+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-13T17:03:03.461317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-13T17:03:03.537993+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-13T17:03:03.584144+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-13T17:03:03.744516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-13T17:03:03.811241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-13T17:03:03.880001+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-13T17:03:03.929675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-13T17:03:03.976934+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-13T17:03:04.005816+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-13T17:03:04.007263+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-13T17:03:04.008731+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-13T17:03:04.010306+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-13T17:03:04.013255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-13T17:03:04.015735+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-13T17:03:04.017860+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-13T17:03:04.020102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-13T17:03:04.021788+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-13T17:03:04.023247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-13T17:03:04.024787+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-13T17:03:04.026164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-13T17:03:04.027350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-13T17:03:04.031633+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-13T17:03:04.036793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-13T17:03:04.065983+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-13T17:03:04.099665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-13T17:03:04.124274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-13T17:03:04.125156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-13T17:03:04.125931+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-13T17:03:04.126564+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-13T17:03:04.131752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-13T17:03:04.136438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-13T17:03:04.143474+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-13T17:03:04.150052+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-13T17:03:04.156597+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-13T17:03:04.162999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-13T17:03:04.170673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-13T17:03:04.177100+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-13T17:03:04.182695+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-13T17:03:04.192215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-13T17:03:04.198415+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-13T17:03:04.207708+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-13T17:03:04.214755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-13T17:03:04.220868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-13T17:03:04.227218+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-13T17:03:04.233179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-13T17:03:04.239395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-13T17:03:04.245560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-13T17:03:04.247058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-13T17:03:04.248492+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-13T17:03:04.250304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-13T17:03:04.256189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-13T17:03:04.261727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-13T17:03:04.267819+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-13T17:03:04.278346+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-13T17:03:04.303036+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-13T17:03:04.308148+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-13T17:03:04.312790+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-13T17:03:04.324353+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-13T17:03:04.356169+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-13T17:03:04.386275+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-13T17:03:04.412570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-13T17:03:04.440469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-13T17:03:04.483010+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-13T17:03:04.549600+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-13T17:03:04.603659+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-13T17:03:04.640518+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-13T17:03:04.698990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-13T17:03:04.767521+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-13T17:03:04.828437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-13T17:03:04.864690+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-13T17:03:04.891998+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-13T17:03:04.942711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-13T17:03:05.006624+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-13T17:03:05.068217+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-13T17:03:05.103914+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-13T17:03:05.137842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-13T17:03:05.183951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-13T17:03:05.185531+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-13T17:03:05.189384+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-13T17:03:05.194528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-13T17:03:05.205751+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-13T17:03:05.278534+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-13T17:03:05.327383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-13T17:03:05.371303+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-13T17:03:05.425084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-13T17:03:05.454765+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-13T17:03:05.519064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-13T17:03:05.570044+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-13T17:03:05.602302+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-13T17:03:05.635524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-13T17:03:05.664509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-13T17:03:05.719346+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-13T17:03:05.772658+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-13T17:03:05.815345+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-13T17:03:05.847710+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-13T17:03:05.877104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-13T17:03:06.587425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-13T17:03:07.049819+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-13T17:03:07.615497+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-13T17:03:07.645894+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-13T17:03:07.672664+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-13T17:03:07.702755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-13T17:03:07.730355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-13T17:03:07.759625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-13T17:03:07.787929+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-13T17:03:07.826236+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-13T17:03:07.857188+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-13T17:03:07.887588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-13T17:03:07.931562+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-13T17:03:07.961158+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-13T17:03:07.994794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-13T17:03:08.036110+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-13T17:03:08.074019+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-13T17:03:08.139727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-13T17:03:08.172613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-13T17:03:08.217146+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-13T17:03:08.252561+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-13T17:03:08.284946+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-13T17:03:08.394277+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-13T17:03:08.476250+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-13T17:03:08.945058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-13T17:03:09.014796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-13T17:03:09.056401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-13T17:03:09.088354+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-13T17:03:09.116903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-13T17:03:09.146019+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-13T17:03:09.187593+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-13T17:03:09.217259+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-13T17:03:09.247837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-13T17:03:09.276039+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-13T17:03:09.303993+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-13T17:03:09.356987+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-13T17:03:09.385018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-13T17:03:09.436818+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-13T17:03:09.479017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-13T17:03:09.526528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-13T17:03:09.812323+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-13T17:03:09.861399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-13T17:03:09.899403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-13T17:03:09.966641+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-13T17:03:10.030736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-13T17:03:10.060294+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-13T17:03:10.136826+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-13T17:03:10.220160+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-13T17:03:10.259673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-13T17:03:10.311542+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-13T17:03:47.426063+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-13T17:03:47.429496+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-13T17:03:47.439723+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-13T17:03:47.473058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-13T17:03:47.502479+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-13T17:03:47.529715+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-13T17:03:47.569964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-13T17:03:47.620006+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-13T17:03:47.661160+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-13T17:03:47.700808+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-13T17:03:47.741695+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-13T17:03:47.773972+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-13T17:03:47.803215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-13T17:03:47.832389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-13T17:03:47.866754+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-13T17:03:47.894725+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-13T17:03:47.922304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-13T17:03:47.949310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-13T17:03:48.544516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-13T17:05:04.744680+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-13T17:05:04.835943+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-13T17:05:04.907356+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-13T17:05:05.093428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-13T17:05:05.410625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-13T17:05:05.728243+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-13T17:05:05.951338+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-13T17:05:06.264048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-13T17:05:06.599680+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-13T17:05:06.699013+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-13T17:05:06.774790+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-13T17:05:06.833503+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-13T17:05:06.934751+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-13T17:05:06.964027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-13T17:05:06.992107+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-13T17:05:07.449995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-13T17:05:07.486445+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-13T17:05:07.515657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-13T17:05:07.562544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-13T17:05:07.626809+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-13T17:05:07.647954+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-13T17:05:07.667979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-13T17:05:07.712489+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-13T17:05:07.757398+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-13T17:05:07.803588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-13T17:05:07.845479+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-13T17:05:07.893441+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-13T17:05:08.011968+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-13T17:05:08.205647+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-13T17:05:08.297578+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-13T17:05:08.389255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-13T17:05:08.498967+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-13T17:05:08.597276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-13T17:05:08.696752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-13T17:05:09.429200+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-13T17:05:10.276494+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression

### [2026-03-13T17:05:10.917291+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-13T17:05:11.606535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped

### [2026-03-13T17:05:12.288175+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-13T17:05:12.944386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-13T17:05:13.458133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-13T17:05:13.506775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-13T17:05:13.593767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-13T17:05:13.595270+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-13T17:05:13.598711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-13T17:05:13.600476+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-13T17:05:13.601706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-13T17:05:13.603233+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-13T17:05:13.605038+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-13T17:05:13.606759+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-13T17:05:13.612689+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-13T17:05:13.646395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-13T17:05:13.648095+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-13T17:05:13.650492+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-13T17:05:13.652067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-13T17:05:13.654055+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-13T17:05:13.655517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-13T17:05:13.656409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-13T17:05:13.657274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-13T17:05:13.657992+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-13T17:05:13.658715+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-13T17:05:13.700130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-13T17:05:13.728231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-13T17:05:13.754348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-13T17:05:13.780511+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-13T17:05:13.807445+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-13T17:05:13.843781+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-13T17:05:13.884706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-13T17:05:13.926047+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-13T17:05:13.956289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-13T17:05:13.995478+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-13T17:05:14.040288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-13T17:05:14.085612+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-13T17:05:14.126615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-13T17:05:14.167155+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-13T17:05:14.210793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-13T17:05:14.240416+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-13T17:05:14.274699+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-13T17:05:14.308038+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-13T17:05:14.309033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-13T17:05:14.310068+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-13T17:05:14.310859+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-13T17:05:14.311560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-13T17:05:14.312969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-13T17:05:14.313677+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-13T17:05:14.314414+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-13T17:05:14.315223+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-13T17:05:14.316370+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-13T17:05:14.317109+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-13T17:05:14.317998+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-13T17:05:14.319170+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-13T17:05:14.320792+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-13T17:05:14.321557+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-13T17:05:14.322921+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-13T17:05:14.351123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-13T17:05:14.352009+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-13T17:05:14.352772+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-13T17:05:14.354265+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-13T17:05:14.354964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-13T17:05:14.355771+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-13T17:05:14.357084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-13T17:05:14.358022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-13T17:05:14.358662+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-13T17:05:14.359469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-13T17:05:14.360111+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-13T17:05:14.361067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-13T17:05:14.361816+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-13T17:05:14.362784+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-13T17:05:14.363653+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-13T17:05:14.364433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-13T17:05:14.365212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-13T17:05:14.365902+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-13T17:05:14.366646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-13T17:05:14.367389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-13T17:05:14.368274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-13T17:05:14.368980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-13T17:05:14.369766+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-13T17:05:14.370512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-13T17:05:14.371186+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-13T17:05:14.371742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-13T17:05:14.442597+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-13T17:05:14.515042+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-13T17:05:14.548336+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-13T17:05:14.553103+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-13T17:05:14.558704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-13T17:05:14.564129+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-13T17:05:14.594990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-13T17:05:14.622815+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-13T17:05:14.626632+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-13T17:05:14.630933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-13T17:05:14.638152+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-13T17:05:14.643185+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-13T17:05:14.647250+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-13T17:05:14.651056+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-13T17:05:14.655797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-13T17:05:14.660176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-13T17:05:14.665529+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-13T17:05:14.669214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-13T17:05:14.672813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-13T17:05:14.680416+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-13T17:05:14.710778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-13T17:05:14.740469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-13T17:05:14.766702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-13T17:05:14.793923+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-13T17:05:14.819901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-13T17:05:14.848163+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-13T17:05:14.875138+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-13T17:05:14.902344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-13T17:05:14.930265+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-13T17:05:14.969803+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-13T17:05:15.010294+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-13T17:05:15.049911+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-13T17:05:15.092192+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-13T17:05:15.132164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-13T17:05:15.173239+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-13T17:05:15.215526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-13T17:05:15.248401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-13T17:05:15.284343+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-13T17:05:15.332319+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-13T17:05:15.369753+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-13T17:05:15.405845+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-13T17:05:15.435210+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-13T17:05:15.436257+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-13T17:05:15.437308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-13T17:05:15.465119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-13T17:05:15.496733+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-13T17:05:15.525398+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-13T17:05:15.554522+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-13T17:05:15.608322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-13T17:05:15.654002+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-13T17:05:15.691982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-13T17:05:15.699857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-13T17:05:15.703375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-13T17:05:15.705930+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-13T17:05:15.747625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-13T17:05:15.776325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-13T17:05:15.778733+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-13T17:05:15.779951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-13T17:05:15.781954+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-13T17:05:15.786461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-13T17:05:15.788983+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-13T17:05:15.791662+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-13T17:05:15.793665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-13T17:05:15.795670+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-13T17:05:15.798121+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts
E   AssertionError: qa_cache.json is older than: ['employer_friendliness_scores.parquet', 'pd_forecasts.parquet']. Re-run qa_generator.
    assert 2 == 0
     +  where 2 = len(['employer_friendliness_scores.parquet', 'pd_forecasts.parquet'])

### [2026-03-13T17:05:15.800445+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts
E   AssertionError: all_chunks.json is older than: ['worksite_geo_metrics.parquet']. Re-run rag_builder.
    assert 1 == 0
     +  where 1 = len(['worksite_geo_metrics.parquet'])

### [2026-03-13T17:05:15.816176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-13T17:05:15.829616+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-13T17:05:15.831509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-13T17:05:15.833202+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-13T17:05:15.836171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-13T17:05:15.837084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-13T17:05:15.838260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-13T17:05:15.839288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-13T17:05:15.898440+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-13T17:10:48.077457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_run_noop

### [2026-03-13T17:10:48.108701+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=592 failed=2 exit=1

### [2026-03-13T17:10:55.640393+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T171055Z pid=80166 python=3.12.3

### [2026-03-13T17:10:55.677050+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_rag_quality.py', '-q', '--tb=short', '--no-header']

### [2026-03-13T17:10:56.098596+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-13T17:10:56.134713+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-13T17:10:56.166343+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-13T17:10:56.198543+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-13T17:10:56.226315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-13T17:10:56.253409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-13T17:10:56.326999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-13T17:10:56.354481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-13T17:10:56.378937+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-13T17:10:56.406922+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-13T17:10:56.434013+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-13T17:10:56.462522+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-13T17:10:56.487358+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-13T17:10:56.513133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-13T17:10:56.537374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-13T17:10:56.563702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-13T17:10:56.614018+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts
tests/test_rag_quality.py:336: in test_qa_cache_not_older_than_key_artifacts
    assert len(stale) == 0, \
E   AssertionError: qa_cache.json is older than: ['employer_friendliness_scores.parquet', 'pd_forecasts.parquet']. Re-run qa_generator.
E   assert 2 == 0
E    +  where 2 = len(['employer_friendliness_scores.parquet', 'pd_forecasts.parquet'])

### [2026-03-13T17:10:56.659693+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts
tests/test_rag_quality.py:357: in test_chunks_not_older_than_key_artifacts
    assert len(stale) == 0, \
E   AssertionError: all_chunks.json is older than: ['worksite_geo_metrics.parquet']. Re-run rag_builder.
E   assert 1 == 0
E    +  where 1 = len(['worksite_geo_metrics.parquet'])

### [2026-03-13T17:10:56.701969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-13T17:10:56.742413+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-13T17:10:56.743569+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-13T17:10:56.744700+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-13T17:10:56.747446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-13T17:10:56.748361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-13T17:10:56.750260+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=24 failed=2 exit=1

### [2026-03-13T17:11:43.679597+00:00] *System*

 [bootstrap]  
SESSION_START session=20260313T171143Z pid=90520 python=3.12.3

### [2026-03-13T17:11:43.706557+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-q', '--tb=line', '--no-header']

### [2026-03-13T17:11:44.194024+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-13T17:11:44.225242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-13T17:11:44.267120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-13T17:11:44.297017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-13T17:11:44.328512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-13T17:11:44.376322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-13T17:11:44.410600+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-13T17:11:44.689099+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-13T17:11:44.727984+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-13T17:11:44.730657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-13T17:11:44.732701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-13T17:11:44.735627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-13T17:11:44.741807+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-13T17:11:44.767803+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-13T17:11:44.770960+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-13T17:11:44.781565+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-13T17:11:44.795687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-13T17:11:44.844810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-13T17:11:44.873109+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-13T17:11:44.900998+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-13T17:11:44.928374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-13T17:11:44.988236+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-13T17:11:45.023007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-13T17:11:45.052776+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-13T17:11:45.081341+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-13T17:11:45.107846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-13T17:11:45.135184+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-13T17:11:45.161847+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-13T17:11:45.189893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-13T17:11:45.220074+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-13T17:11:45.249902+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-13T17:11:45.276778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-13T17:11:45.402758+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-13T17:11:45.485053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-13T17:11:45.670993+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-13T17:11:45.805300+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-13T17:11:45.913672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-13T17:11:45.954353+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-13T17:11:45.956340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-13T17:11:45.958893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-13T17:11:45.982897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-13T17:11:45.984839+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-13T17:11:45.987111+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-13T17:11:45.989177+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-13T17:11:45.991084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-13T17:11:46.040382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-13T17:11:46.117360+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-13T17:11:46.148539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-13T17:11:46.225210+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-13T17:11:46.586848+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-13T17:11:47.031602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-13T17:11:47.071660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-13T17:11:47.106399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-13T17:11:47.122517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-13T17:11:47.138128+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-13T17:11:47.140883+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-13T17:11:47.143273+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-13T17:11:47.146358+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-13T17:11:47.187075+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-13T17:11:47.216128+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-13T17:11:47.243545+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-13T17:11:47.271663+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-13T17:11:47.303323+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-13T17:11:47.349178+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-13T17:11:47.380535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-13T17:11:47.409243+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-13T17:11:47.437500+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-13T17:11:47.465755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-13T17:11:47.493825+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-13T17:11:47.521201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-13T17:11:47.547098+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-13T17:11:47.575516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-13T17:11:47.604667+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-13T17:11:47.631994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-13T17:11:47.661040+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-13T17:11:47.691299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-13T17:11:47.722788+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-13T17:11:47.753768+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-13T17:11:47.786382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-13T17:11:47.821241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-13T17:11:47.852499+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-13T17:11:47.889679+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-13T17:11:47.920162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-13T17:11:47.951862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-13T17:11:47.992359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-13T17:11:48.049842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-13T17:11:48.083305+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-13T17:11:48.142923+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-13T17:11:48.200944+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-13T17:11:48.262140+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-13T17:11:48.290227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-13T17:11:48.318535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-13T17:11:48.346130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-13T17:11:48.382354+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-13T17:11:48.410539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-13T17:11:48.450627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-13T17:11:48.477891+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-13T17:11:48.505578+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-13T17:11:48.534496+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-13T17:11:48.563386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-13T17:11:48.592505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-13T17:11:48.623991+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-13T17:11:48.654706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-13T17:11:48.684806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-13T17:11:48.713055+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-13T17:11:48.742315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-13T17:11:48.775157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-13T17:11:48.810488+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-13T17:11:48.857544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-13T17:11:48.885635+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-13T17:11:48.915761+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-13T17:11:48.944813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-13T17:11:48.973701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-13T17:11:49.003868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-13T17:11:49.036063+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-13T17:11:49.065213+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-13T17:11:49.363551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-13T17:11:49.418885+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-13T17:11:49.466026+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-13T17:11:49.495309+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-13T17:11:49.523392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-13T17:11:49.552163+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-13T17:11:49.585759+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-13T17:11:49.638373+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-13T17:11:49.667480+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-13T17:11:49.695986+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-13T17:11:49.724508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-13T17:11:49.751169+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-13T17:11:49.777221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-13T17:11:49.813624+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-13T17:11:49.904184+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-13T17:11:49.945965+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-13T17:11:49.975843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-13T17:11:50.005260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-13T17:11:50.036482+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-13T17:11:50.063183+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-13T17:11:50.088542+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-13T17:11:50.113356+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-13T17:11:50.140473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-13T17:11:50.168910+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-13T17:11:50.207449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-13T17:11:50.235025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-13T17:11:50.262270+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-13T17:11:50.289375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-13T17:11:50.317291+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-13T17:11:50.347039+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-13T17:11:50.374379+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-13T17:11:50.435796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-13T17:11:50.468503+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-13T17:11:50.495201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-13T17:11:50.497892+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-13T17:11:50.499035+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-13T17:11:50.504014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-13T17:11:50.554174+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-13T17:11:50.582167+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-13T17:11:50.609960+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-13T17:11:50.637943+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-13T17:11:50.665173+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-13T17:11:50.697302+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-13T17:11:50.725875+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-13T17:11:50.753501+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-13T17:11:50.782916+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-13T17:11:50.832554+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-13T17:11:50.861135+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-13T17:11:50.889271+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-13T17:11:50.920241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-13T17:11:50.970407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-13T17:11:50.997706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-13T17:11:51.023953+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-13T17:11:51.054305+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-13T17:11:51.082713+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-13T17:11:51.146179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-13T17:11:51.173618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-13T17:11:51.199982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-13T17:11:51.226049+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-13T17:11:51.255756+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-13T17:11:51.286668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-13T17:11:51.314442+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T17:11:51.343928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T17:11:51.385213+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T17:11:51.415143+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T17:11:51.443678+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T17:11:51.475416+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T17:11:51.502749+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T17:11:51.530380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T17:11:51.567639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T17:11:51.574979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T17:11:51.611689+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T17:11:51.614732+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T17:11:51.616994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T17:11:51.619496+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T17:11:51.621532+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T17:11:51.623395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T17:11:51.625177+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-13T17:11:51.626735+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-13T17:11:51.634267+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-13T17:11:51.636245+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-13T17:11:51.637912+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-13T17:11:51.639572+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-13T17:11:51.641090+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-13T17:11:51.642763+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-13T17:11:51.644751+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-13T17:11:51.692266+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-13T17:11:51.722844+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-13T17:11:51.753747+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-13T17:11:51.787805+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-13T17:11:51.822250+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-13T17:11:51.860390+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-13T17:11:51.889252+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-13T17:11:51.923286+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-13T17:11:51.957652+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-13T17:11:51.992983+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-13T17:11:52.026862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-13T17:11:52.059737+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-13T17:11:52.099046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-13T17:11:52.136967+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-13T17:11:52.906301+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-13T17:11:52.910038+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-13T17:11:52.917148+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-13T17:11:52.923200+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-13T17:11:52.952398+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-13T17:11:52.981399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-13T17:11:53.010438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-13T17:11:53.038844+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-13T17:11:53.072842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-13T17:11:53.103348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-13T17:11:53.135240+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-13T17:11:53.168135+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-13T17:11:53.197330+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-13T17:11:53.226843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-13T17:11:53.255456+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-13T17:11:53.282006+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-13T17:11:53.315882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-13T17:11:53.346984+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-13T17:11:53.377857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-13T17:11:53.410015+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-13T17:11:53.442219+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-13T17:11:53.473107+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-13T17:11:53.520062+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-13T17:11:53.553194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-13T17:11:53.579527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-13T17:11:53.606713+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-13T17:11:53.632518+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-13T17:11:53.661413+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-13T17:11:53.690107+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-13T17:11:53.718468+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-13T17:11:53.747119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-13T17:11:53.775381+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-13T17:11:53.801344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-13T17:11:53.828298+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-13T17:11:53.856282+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-13T17:11:53.860656+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-13T17:11:53.877956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-13T17:11:53.899037+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-13T17:11:53.951027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-13T17:11:53.958325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-13T17:11:53.960252+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-13T17:11:53.961593+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-13T17:11:53.968606+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-13T17:11:53.971444+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-13T17:11:53.977421+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-13T17:11:53.985599+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-13T17:11:54.019694+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-13T17:11:54.052842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-13T17:11:54.085762+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-13T17:11:54.118267+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-13T17:11:54.152725+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-13T17:11:54.186575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-13T17:11:54.218841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-13T17:11:54.253615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-13T17:11:54.286030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-13T17:11:54.323112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-13T17:11:54.355439+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-13T17:11:54.385619+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-13T17:11:54.417105+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-13T17:11:54.447515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-13T17:11:54.483710+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-13T17:11:54.514597+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-13T17:11:54.541560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-13T17:11:54.570268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-13T17:11:54.598843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-13T17:11:54.638255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-13T17:11:54.673012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-13T17:11:54.708072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-13T17:11:54.745246+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-13T17:11:54.774183+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-13T17:11:54.804610+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-13T17:11:54.835402+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-13T17:11:54.873473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-13T17:11:54.899504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-13T17:11:54.926509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-13T17:11:54.952871+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-13T17:11:54.980223+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-13T17:11:55.008058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-13T17:11:55.035487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-13T17:11:55.063965+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-13T17:11:55.091670+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-13T17:11:55.118939+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-13T17:11:55.146469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-13T17:11:55.175672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-13T17:11:55.202216+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-13T17:11:55.271678+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-13T17:11:55.303227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-13T17:11:55.331992+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-13T17:11:55.336989+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-13T17:11:55.340634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-13T17:11:55.342978+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-13T17:11:55.347928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-13T17:11:55.349064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-13T17:11:55.351463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-13T17:11:55.354076+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-13T17:11:55.385414+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-13T17:11:55.413337+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-13T17:11:55.441609+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-13T17:11:55.469054+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-13T17:11:55.497592+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-13T17:11:55.525831+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-13T17:11:55.552690+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-13T17:11:55.581556+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-13T17:11:55.610738+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-13T17:11:55.639916+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-13T17:11:55.666529+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-13T17:11:55.694888+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-13T17:11:55.723087+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-13T17:11:55.749210+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-13T17:11:55.778236+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-13T17:11:55.807536+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-13T17:11:56.277122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-13T17:11:56.776675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-13T17:11:57.209535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-13T17:11:57.238196+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-13T17:11:57.264631+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-13T17:11:57.291782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-13T17:11:57.319656+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-13T17:11:57.346145+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-13T17:11:57.372833+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-13T17:11:57.408236+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-13T17:11:57.435889+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-13T17:11:57.462973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-13T17:11:57.491156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-13T17:11:57.534528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-13T17:11:57.577554+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-13T17:11:57.612508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-13T17:11:57.617933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-13T17:11:57.624377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-13T17:11:57.626613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-13T17:11:57.638463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-13T17:11:57.643181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-13T17:11:57.646420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-13T17:11:57.719742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-13T17:11:57.775058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-13T17:11:58.171217+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-13T17:11:58.215891+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-13T17:11:58.250443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-13T17:11:58.279383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-13T17:11:58.304155+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-13T17:11:58.328400+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-13T17:11:58.366702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-13T17:11:58.395674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-13T17:11:58.423688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-13T17:11:58.450867+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-13T17:11:58.480938+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-13T17:11:58.507037+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-13T17:11:58.533082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-13T17:11:58.579704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-13T17:11:58.606303+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-13T17:11:58.633152+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-13T17:11:58.862947+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-13T17:11:58.904513+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-13T17:11:58.934672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-13T17:11:58.969586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-13T17:11:59.003971+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-13T17:11:59.033212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-13T17:11:59.066233+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-13T17:11:59.095085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-13T17:11:59.124255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-13T17:11:59.153853+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-13T17:12:36.082941+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-13T17:12:36.111990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-13T17:12:36.146922+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-13T17:12:36.177497+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-13T17:12:36.206049+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-13T17:12:36.231239+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-13T17:12:36.274072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-13T17:12:36.324978+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-13T17:12:36.365402+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-13T17:12:36.408207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-13T17:12:36.448852+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-13T17:12:36.480973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-13T17:12:36.508544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-13T17:12:36.535953+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-13T17:12:36.571173+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-13T17:12:36.600043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-13T17:12:36.627647+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-13T17:12:36.658358+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-13T17:12:37.231480+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-13T17:13:51.838709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-13T17:13:51.901170+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-13T17:13:51.940791+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-13T17:13:52.098811+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-13T17:13:52.249037+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-13T17:13:52.557576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-13T17:13:52.732845+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-13T17:13:52.988743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-13T17:13:53.240575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-13T17:13:53.289300+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-13T17:13:53.337356+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-13T17:13:53.398539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-13T17:13:53.501646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-13T17:13:53.528821+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-13T17:13:53.555920+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-13T17:13:53.847795+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-13T17:13:53.876315+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-13T17:13:53.901972+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-13T17:13:53.944344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-13T17:13:54.010150+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-13T17:13:54.053321+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-13T17:13:54.096679+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-13T17:13:54.140603+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-13T17:13:54.183079+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-13T17:13:54.226220+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-13T17:13:54.269239+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-13T17:13:54.296864+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-13T17:13:54.388379+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-13T17:13:54.562782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-13T17:13:54.640203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-13T17:13:54.721009+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-13T17:13:54.788896+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-13T17:13:54.860531+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-13T17:13:54.954771+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-13T17:13:55.590677+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-13T17:13:56.206002+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression

### [2026-03-13T17:13:56.835255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-13T17:13:57.440201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped

### [2026-03-13T17:13:58.098291+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-13T17:13:58.715342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-13T17:13:59.239674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-13T17:13:59.305724+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-13T17:13:59.409909+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-13T17:13:59.447587+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-13T17:13:59.480130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-13T17:13:59.505991+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-13T17:13:59.530361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-13T17:13:59.554911+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-13T17:13:59.581940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-13T17:13:59.609078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-13T17:13:59.637611+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-13T17:13:59.675106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-13T17:13:59.676970+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-13T17:13:59.679123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-13T17:13:59.681271+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-13T17:13:59.683659+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-13T17:13:59.685292+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-13T17:13:59.686365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-13T17:13:59.687339+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-13T17:13:59.688544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-13T17:13:59.689945+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-13T17:13:59.690794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-13T17:13:59.692810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-13T17:13:59.693638+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-13T17:13:59.694539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-13T17:13:59.695275+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-13T17:13:59.696351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-13T17:13:59.724794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-13T17:13:59.752526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-13T17:13:59.779559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-13T17:13:59.806060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-13T17:13:59.833123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-13T17:13:59.859725+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-13T17:13:59.887110+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-13T17:13:59.912255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-13T17:13:59.913993+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-13T17:13:59.915325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-13T17:13:59.916232+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-13T17:13:59.917350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-13T17:13:59.918392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-13T17:13:59.919355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-13T17:13:59.920365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-13T17:13:59.921378+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-13T17:13:59.922242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-13T17:13:59.923020+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-13T17:13:59.924128+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-13T17:13:59.925339+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-13T17:13:59.926571+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-13T17:13:59.927428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-13T17:13:59.928172+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-13T17:13:59.928999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-13T17:13:59.929971+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-13T17:13:59.930716+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-13T17:13:59.931345+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-13T17:13:59.932059+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-13T17:13:59.932775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-13T17:13:59.933551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-13T17:13:59.934154+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-13T17:13:59.934878+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-13T17:13:59.935545+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-13T17:13:59.936181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-13T17:13:59.936840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-13T17:13:59.937525+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-13T17:13:59.938199+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-13T17:13:59.938883+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-13T17:13:59.939625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-13T17:13:59.940285+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-13T17:13:59.941167+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-13T17:13:59.942178+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-13T17:13:59.943054+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-13T17:13:59.944075+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-13T17:13:59.972460+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-13T17:14:00.006114+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-13T17:14:00.008545+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-13T17:14:00.011511+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-13T17:14:00.012505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-13T17:14:00.013490+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-13T17:14:00.014404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-13T17:14:00.015587+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-13T17:14:00.017011+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-13T17:14:00.083527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-13T17:14:00.166457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-13T17:14:00.193917+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-13T17:14:00.198728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-13T17:14:00.228110+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-13T17:14:00.252824+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-13T17:14:00.255582+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-13T17:14:00.258817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-13T17:14:00.261419+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-13T17:14:00.289573+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-13T17:14:00.317607+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-13T17:14:00.345063+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-13T17:14:00.347308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-13T17:14:00.349505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-13T17:14:00.351817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-13T17:14:00.354104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-13T17:14:00.357422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-13T17:14:00.361255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-13T17:14:00.365109+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-13T17:14:00.372069+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-13T17:14:00.374284+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-13T17:14:00.376992+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-13T17:14:00.377895+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-13T17:14:00.379215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-13T17:14:00.380159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-13T17:14:00.380911+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-13T17:14:00.385697+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-13T17:14:00.412765+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-13T17:14:00.439018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-13T17:14:00.466825+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-13T17:14:00.490906+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-13T17:14:00.515119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-13T17:14:00.541036+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-13T17:14:00.568269+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-13T17:14:00.596075+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-13T17:14:00.625475+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-13T17:14:00.654446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-13T17:14:00.683500+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-13T17:14:00.711778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-13T17:14:00.737735+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-13T17:14:00.765046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-13T17:14:00.792255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-13T17:14:00.818783+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-13T17:14:00.845277+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-13T17:14:00.871332+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-13T17:14:00.898632+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-13T17:14:00.924727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-13T17:14:00.950092+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-13T17:14:00.991113+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-13T17:14:01.023038+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-13T17:14:01.052199+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-13T17:14:01.084203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-13T17:14:01.110797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-13T17:14:01.117421+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-13T17:14:01.165352+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-13T17:14:01.192595+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-13T17:14:01.219957+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-13T17:14:01.246709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-13T17:14:01.247541+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-13T17:14:01.250450+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-13T17:14:01.251142+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-13T17:14:01.251880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-13T17:14:01.252508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-13T17:14:01.253249+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-13T17:14:01.253896+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts

### [2026-03-13T17:14:01.254673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-13T17:14:01.270374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-13T17:14:01.284058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-13T17:14:01.285149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-13T17:14:01.285921+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-13T17:14:01.288630+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-13T17:14:01.289598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-13T17:14:01.290672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-13T17:14:01.291705+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-13T17:14:01.350188+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-13T17:19:38.713308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_run_noop

### [2026-03-13T17:19:38.745661+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=592 failed=0 exit=0

