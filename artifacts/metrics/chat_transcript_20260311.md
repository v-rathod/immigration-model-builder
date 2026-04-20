# Chat Transcript

### New transcript started 2026-03-06T03:23:57.654149+00:00 (reason=daily)

### [2026-03-06T03:23:57.654527+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T032357Z pid=59121 python=3.12.3

### [2026-03-06T03:23:57.685379+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-x', '-q', '--tb=short']

### [2026-03-06T03:23:58.452697+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-06T03:23:58.488505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-06T03:23:58.533640+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-06T03:23:58.564321+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-06T03:23:58.599066+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-06T03:23:58.646955+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-06T03:23:58.683835+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-06T03:23:58.953806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-06T03:23:58.984293+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-06T03:23:59.014600+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-06T03:23:59.044360+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-06T03:23:59.074510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-06T03:23:59.108121+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-06T03:23:59.136925+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-06T03:23:59.166032+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-06T03:23:59.203497+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-06T03:23:59.237582+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-06T03:23:59.296570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-06T03:23:59.327063+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-06T03:23:59.355952+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-06T03:23:59.385266+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-06T03:23:59.457113+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-06T03:23:59.493873+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-06T03:23:59.525443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-06T03:23:59.557288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-06T03:23:59.586395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-06T03:23:59.615862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-06T03:23:59.645880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-06T03:23:59.676485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-06T03:23:59.707437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-06T03:23:59.737242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-06T03:23:59.764928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-06T03:23:59.855751+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-06T03:23:59.935520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-06T03:24:00.135354+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-06T03:24:00.266649+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-06T03:24:00.350451+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-06T03:24:00.382205+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-06T03:24:00.411900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-06T03:24:00.441996+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-06T03:24:00.470353+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-06T03:24:00.499906+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-06T03:24:00.529292+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-06T03:24:00.558545+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-06T03:24:00.586783+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-06T03:24:00.669093+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-06T03:24:00.756324+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-06T03:24:00.791306+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-06T03:24:00.872370+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-06T03:24:01.221685+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-06T03:24:01.651928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-06T03:24:01.698403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-06T03:24:01.758559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-06T03:24:01.801756+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-06T03:24:01.847623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-06T03:24:01.879544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-06T03:24:01.910038+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-06T03:24:01.941343+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-06T03:24:02.012262+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-06T03:24:02.078465+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-06T03:24:02.109788+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-06T03:24:02.140418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-06T03:24:02.174344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-06T03:24:02.203826+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-06T03:24:02.237964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-06T03:24:02.268349+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-06T03:24:02.298683+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-06T03:24:02.327767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-06T03:24:02.357442+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-06T03:24:02.386361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-06T03:24:02.416659+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-06T03:24:02.446060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-06T03:24:02.475803+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-06T03:24:02.506802+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-06T03:24:02.539198+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-06T03:24:02.571962+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-06T03:24:02.607387+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-06T03:24:02.638706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-06T03:24:02.671467+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-06T03:24:02.707736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-06T03:24:02.740898+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-06T03:24:02.776436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-06T03:24:02.809244+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-06T03:24:02.843607+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-06T03:24:02.886086+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-06T03:24:02.946098+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-06T03:24:02.981927+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-06T03:24:03.017920+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-06T03:24:03.069186+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-06T03:24:03.113880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-06T03:24:03.144897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-06T03:24:03.175766+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-06T03:24:03.204416+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-06T03:24:03.233856+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-06T03:24:03.262856+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-06T03:24:03.292501+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-06T03:24:03.323469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-06T03:24:03.354743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-06T03:24:03.387758+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-06T03:24:03.418639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-06T03:24:03.447755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-06T03:24:03.477151+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-06T03:24:03.510775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-06T03:24:03.541794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-06T03:24:03.570829+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-06T03:24:03.603694+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-06T03:24:03.633426+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-06T03:24:03.668082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-06T03:24:03.708181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-06T03:24:03.737916+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-06T03:24:03.769900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-06T03:24:03.801381+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-06T03:24:03.833806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-06T03:24:03.868299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-06T03:24:03.901812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-06T03:24:03.933599+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-06T03:24:04.223724+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-06T03:24:04.266916+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-06T03:24:04.314729+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-06T03:24:04.344979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-06T03:24:04.374507+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-06T03:24:04.406945+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-06T03:24:04.441256+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-06T03:24:04.497416+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-06T03:24:04.527912+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-06T03:24:04.557431+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-06T03:24:04.587008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-06T03:24:04.618583+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-06T03:24:04.646336+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-06T03:24:04.684698+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-06T03:24:04.776179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-06T03:24:04.818401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-06T03:24:04.848939+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-06T03:24:04.879857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-06T03:24:04.914609+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-06T03:24:04.945933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-06T03:24:04.975203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-06T03:24:05.003206+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-06T03:24:05.033345+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-06T03:24:05.063813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-06T03:24:05.106171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-06T03:24:05.136077+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-06T03:24:05.165245+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-06T03:24:05.194349+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-06T03:24:05.224266+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-06T03:24:05.254841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-06T03:24:05.285228+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-06T03:24:05.323976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-06T03:24:05.354106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-06T03:24:05.383574+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-06T03:24:05.412753+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-06T03:24:05.440377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-06T03:24:05.473366+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-06T03:24:05.515913+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-06T03:24:05.545575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-06T03:24:05.575361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-06T03:24:05.604523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-06T03:24:05.633756+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-06T03:24:05.671710+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-06T03:24:05.701982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-06T03:24:05.733665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-06T03:24:05.768189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-06T03:24:05.806997+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-06T03:24:05.836792+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-06T03:24:05.869690+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-06T03:24:05.900960+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-06T03:24:05.939547+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-06T03:24:05.971952+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-06T03:24:06.004265+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-06T03:24:06.038056+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-06T03:24:06.071032+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-06T03:24:06.127018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-06T03:24:06.158263+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-06T03:24:06.188391+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-06T03:24:06.217603+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-06T03:24:06.248992+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-06T03:24:06.279286+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-06T03:24:06.309184+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T03:24:06.339925+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T03:24:06.375862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T03:24:06.406308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T03:24:06.436825+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T03:24:06.466692+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T03:24:06.495525+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T03:24:06.524568+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T03:24:06.554466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T03:24:06.584504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T03:24:06.648341+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T03:24:06.678815+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T03:24:06.709182+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T03:24:06.738492+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T03:24:06.766731+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T03:24:06.795474+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T03:24:06.827827+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T03:24:06.859083+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T03:24:06.900078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T03:24:06.930924+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T03:24:06.963017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T03:24:06.995275+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T03:24:07.027549+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T03:24:07.058640+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T03:24:07.092042+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-06T03:24:07.125608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-06T03:24:07.160317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-06T03:24:07.195119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-06T03:24:07.233586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-06T03:24:07.270863+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-06T03:24:07.306588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-06T03:24:07.337913+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-06T03:24:07.376021+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-06T03:24:07.412533+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-06T03:24:07.449161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-06T03:24:07.485459+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-06T03:24:07.520882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-06T03:24:07.563791+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-06T03:24:07.605027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-06T03:24:08.379677+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-06T03:24:08.409722+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-06T03:24:08.442395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-06T03:24:08.475106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-06T03:24:08.503787+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-06T03:24:08.533682+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-06T03:24:08.564625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-06T03:24:08.596720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-06T03:24:08.632728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-06T03:24:08.665201+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-06T03:24:08.698900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-06T03:24:08.732984+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-06T03:24:08.764770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-06T03:24:08.794112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-06T03:24:08.821573+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-06T03:24:08.849230+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-06T03:24:08.879298+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-06T03:24:08.913770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-06T03:24:08.945919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-06T03:24:08.977228+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-06T03:24:09.007095+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-06T03:24:09.041322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-06T03:24:09.085058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-06T03:24:09.117607+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-06T03:24:09.146591+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-06T03:24:09.177243+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-06T03:24:09.204498+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-06T03:24:09.234334+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-06T03:24:09.262116+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-06T03:24:09.290438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-06T03:24:09.318666+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-06T03:24:09.346405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-06T03:24:09.376843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-06T03:24:09.404813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-06T03:24:09.432443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-06T03:24:09.463027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-06T03:24:09.494393+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-06T03:24:09.525627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-06T03:24:09.575670+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-06T03:24:09.611691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-06T03:24:09.640871+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-06T03:24:09.669353+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-06T03:24:09.698064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-06T03:24:09.727498+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-06T03:24:09.760862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-06T03:24:09.794180+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-06T03:24:09.830715+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-06T03:24:09.865423+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-06T03:24:09.901022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-06T03:24:09.936980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-06T03:24:09.973769+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-06T03:24:10.008412+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-06T03:24:10.042163+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-06T03:24:10.081405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-06T03:24:10.115894+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-06T03:24:10.153676+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-06T03:24:10.189472+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-06T03:24:10.223957+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-06T03:24:10.257812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-06T03:24:10.293118+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-06T03:24:10.327405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-06T03:24:10.362958+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-06T03:24:10.391195+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-06T03:24:10.418920+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-06T03:24:10.446380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-06T03:24:10.480069+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-06T03:24:10.513681+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-06T03:24:10.550971+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-06T03:24:10.588831+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-06T03:24:10.618473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-06T03:24:10.652014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-06T03:24:10.688074+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-06T03:24:10.729162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-06T03:24:10.759854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-06T03:24:10.789420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-06T03:24:10.817903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-06T03:24:10.847720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-06T03:24:10.877550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-06T03:24:10.910422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-06T03:24:10.939470+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-06T03:24:10.967255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-06T03:24:10.997596+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-06T03:24:11.025325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-06T03:24:11.053516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-06T03:24:11.083139+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-06T03:24:11.115073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-06T03:24:11.146693+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-06T03:24:11.178850+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-06T03:24:11.209823+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-06T03:24:11.239529+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-06T03:24:11.269668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-06T03:24:11.297995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-06T03:24:11.327542+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-06T03:24:11.359566+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-06T03:24:11.390999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-06T03:24:11.418570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-06T03:24:11.447810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-06T03:24:11.476563+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-06T03:24:11.505855+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-06T03:24:11.536581+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-06T03:24:11.568900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-06T03:24:12.063096+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-06T03:24:12.524420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-06T03:24:12.970620+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-06T03:24:13.035560+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-06T03:24:13.063885+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-06T03:24:13.094624+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-06T03:24:13.124406+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-06T03:24:13.154404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-06T03:24:13.184189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-06T03:24:13.220932+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-06T03:24:13.250254+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-06T03:24:13.279116+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-06T03:24:13.307510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-06T03:24:13.334993+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-06T03:24:13.362663+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-06T03:24:13.397551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-06T03:24:13.428371+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-06T03:24:13.459016+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-06T03:24:13.488204+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-06T03:24:13.530706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-06T03:24:13.564319+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-06T03:24:13.594873+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-06T03:24:13.689296+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-06T03:24:13.772465+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-06T03:24:14.153434+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-06T03:24:14.203409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-06T03:24:14.244699+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-06T03:24:14.278129+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-06T03:24:14.306855+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-06T03:24:14.335663+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-06T03:24:14.376882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-06T03:24:14.406499+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-06T03:24:14.435031+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-06T03:24:14.463649+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-06T03:24:14.493227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-06T03:24:14.522153+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-06T03:24:14.552422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-06T03:24:14.587003+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-06T03:24:14.616161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-06T03:24:14.645446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-06T03:24:14.848755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-06T03:24:14.893981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-06T03:24:14.926144+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-06T03:24:14.963078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-06T03:24:14.999586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-06T03:24:15.029056+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-06T03:24:15.063547+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-06T03:24:15.092884+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-06T03:24:15.123085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-06T03:24:15.152856+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-06T03:24:59.128847+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-06T03:24:59.166088+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-06T03:24:59.204355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-06T03:24:59.236909+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-06T03:24:59.270413+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-06T03:24:59.310078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-06T03:24:59.396394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-06T03:24:59.450212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-06T03:24:59.491381+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-06T03:24:59.534489+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-06T03:24:59.578742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-06T03:24:59.613447+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-06T03:24:59.645627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-06T03:24:59.676231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-06T03:24:59.717773+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-06T03:24:59.750145+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-06T03:24:59.781210+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-06T03:24:59.813870+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-06T03:25:01.370053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-06T03:26:15.521718+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=582 failed=0 exit=2

### [2026-03-06T03:26:16.058093+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T032616Z pid=67591 python=3.12.3

### [2026-03-06T03:26:16.087859+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-q', '--tb=short']

### [2026-03-06T03:26:16.651854+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-06T03:26:16.686626+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-06T03:26:16.732516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-06T03:26:16.765008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-06T03:26:16.799307+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-06T03:26:16.847795+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-06T03:26:16.885720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-06T03:26:17.161756+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-06T03:26:17.191342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-06T03:26:17.219869+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-06T03:26:17.248877+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-06T03:26:17.278711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-06T03:26:17.310945+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-06T03:26:17.339491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-06T03:26:17.368903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-06T03:26:17.405461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-06T03:26:17.442381+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-06T03:26:17.499053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-06T03:26:17.528624+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-06T03:26:17.557881+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-06T03:26:17.586852+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-06T03:26:17.652130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-06T03:26:17.696804+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-06T03:26:17.728110+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-06T03:26:17.762176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-06T03:26:17.790759+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-06T03:26:17.820771+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-06T03:26:17.850367+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-06T03:26:17.879878+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-06T03:26:17.908821+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-06T03:26:17.936822+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-06T03:26:17.962956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-06T03:26:18.064663+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-06T03:26:18.148276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-06T03:26:18.354738+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-06T03:26:18.485790+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-06T03:26:18.565329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-06T03:26:18.597189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-06T03:26:18.626563+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-06T03:26:18.657773+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-06T03:26:18.689160+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-06T03:26:18.719648+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-06T03:26:18.751360+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-06T03:26:18.781485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-06T03:26:18.812263+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-06T03:26:18.906538+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-06T03:26:18.992901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-06T03:26:19.026646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-06T03:26:19.107699+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-06T03:26:19.467609+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-06T03:26:19.893484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-06T03:26:19.968180+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-06T03:26:20.027545+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-06T03:26:20.072241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-06T03:26:20.116488+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-06T03:26:20.147834+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-06T03:26:20.177223+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-06T03:26:20.229941+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-06T03:26:20.296340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-06T03:26:20.325934+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-06T03:26:20.355620+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-06T03:26:20.385229+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-06T03:26:20.419133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-06T03:26:20.448529+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-06T03:26:20.482576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-06T03:26:20.511926+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-06T03:26:20.544503+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-06T03:26:20.578888+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-06T03:26:20.612755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-06T03:26:20.646261+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-06T03:26:20.684370+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-06T03:26:20.715469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-06T03:26:20.748514+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-06T03:26:20.779797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-06T03:26:20.810990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-06T03:26:20.839767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-06T03:26:20.872426+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-06T03:26:20.904308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-06T03:26:20.933510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-06T03:26:20.964615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-06T03:26:20.994710+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-06T03:26:21.028850+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-06T03:26:21.182906+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=582 failed=0 exit=2

### [2026-03-06T03:26:21.647146+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T032621Z pid=68066 python=3.12.3

### [2026-03-06T03:26:21.675999+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/p3_metrics/', 'tests/test_employer_name_normalization.py', '-q', '--tb=short']

### [2026-03-06T03:26:22.077042+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-06T03:26:22.130575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-06T03:26:22.164864+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-06T03:26:22.194527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-06T03:26:22.223897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-06T03:26:22.258512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-06T03:26:22.300814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-06T03:26:22.337088+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-06T03:26:22.372362+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-06T03:26:22.408157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-06T03:26:22.442091+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-06T03:26:22.476461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-06T03:26:22.510664+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-06T03:26:22.543674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-06T03:26:22.579743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-06T03:26:22.613833+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-06T03:26:22.694810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-06T03:26:22.730228+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-06T03:26:22.763382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-06T03:26:22.797861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-06T03:26:22.829819+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-06T03:26:22.861235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-06T03:26:22.892527+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-06T03:26:22.919979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-06T03:26:22.947410+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-06T03:26:22.974263+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-06T03:26:23.005045+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-06T03:26:23.035882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-06T03:26:23.066956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-06T03:26:23.104566+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-06T03:26:23.133203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-06T03:26:23.171490+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-06T03:26:23.202611+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-06T03:26:23.250637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-06T03:26:23.280180+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-06T03:26:23.307166+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-06T03:26:23.336046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-06T03:26:23.365342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-06T03:26:23.394277+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-06T03:26:23.425852+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-06T03:26:23.454302+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-06T03:26:23.483236+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-06T03:26:23.514010+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-06T03:26:23.572886+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-06T03:26:23.612804+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-06T03:26:23.765149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-06T03:26:23.905673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-06T03:26:24.207461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-06T03:26:24.371401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-06T03:26:24.641395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-06T03:26:24.893952+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-06T03:26:24.943511+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-06T03:26:24.991996+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-06T03:26:25.050290+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-06T03:26:25.150181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-06T03:26:25.179748+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-06T03:26:25.206751+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-06T03:26:25.234133+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=57 failed=0 exit=0

### [2026-03-06T03:26:33.045551+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T032633Z pid=68996 python=3.12.3

### [2026-03-06T03:26:33.073754+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/p3_metrics/test_salary_benchmarks.py', 'tests/test_employer_name_normalization.py', '-v', '--no-header']

### [2026-03-06T03:26:33.457717+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-06T03:26:33.494805+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-06T03:26:33.523076+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-06T03:26:33.550386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-06T03:26:33.584363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-06T03:26:33.615811+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-06T03:26:33.649940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-06T03:26:33.738394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-06T03:26:33.799899+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-06T03:26:33.841439+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-06T03:26:33.993084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-06T03:26:34.135797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-06T03:26:34.431151+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-06T03:26:34.620399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-06T03:26:34.901716+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-06T03:26:35.151632+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-06T03:26:35.199893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-06T03:26:35.248367+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-06T03:26:35.306667+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-06T03:26:35.403893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-06T03:26:35.440320+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-06T03:26:35.469471+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-06T03:26:35.497986+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=22 failed=0 exit=0

### [2026-03-06T03:27:30.161778+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T032730Z pid=71953 python=3.12.3

### [2026-03-06T03:27:30.190671+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/p3_metrics/test_soc_salary_market.py', '-v']

### [2026-03-06T03:27:30.629363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-06T03:27:30.659290+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-06T03:27:30.689044+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-06T03:27:30.717425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-06T03:27:30.745962+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-06T03:27:30.777111+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-06T03:27:30.808210+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-06T03:27:30.838986+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-06T03:27:30.869286+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-06T03:27:30.900602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-06T03:27:30.931058+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=10 failed=0 exit=0

### [2026-03-06T15:16:17.538279+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T151617Z pid=10430 python=3.12.3

### [2026-03-06T15:16:17.567958+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', '-q']

### [2026-03-06T15:16:20.802839+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-06T15:16:20.839696+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-06T15:16:20.889565+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-06T15:16:20.920676+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-06T15:16:20.955601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-06T15:16:21.001732+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-06T15:16:21.042018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-06T15:16:21.307341+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-06T15:16:21.339662+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-06T15:16:21.370882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-06T15:16:21.402514+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-06T15:16:21.432472+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-06T15:16:21.465017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-06T15:16:21.493974+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-06T15:16:21.526897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-06T15:16:21.565433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-06T15:16:21.601837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-06T15:16:21.664880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-06T15:16:21.695575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-06T15:16:21.724978+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-06T15:16:21.755475+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-06T15:16:21.822565+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-06T15:16:21.858490+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-06T15:16:21.888477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-06T15:16:21.917056+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-06T15:16:21.944442+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-06T15:16:21.974720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-06T15:16:22.005186+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-06T15:16:22.037818+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-06T15:16:22.070483+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-06T15:16:22.099842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-06T15:16:22.128418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-06T15:16:22.228587+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-06T15:16:22.307502+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-06T15:16:22.508632+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-06T15:16:22.647352+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-06T15:16:22.725181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-06T15:16:22.758069+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-06T15:16:22.789813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-06T15:16:22.822072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-06T15:16:22.851609+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-06T15:16:22.878984+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-06T15:16:22.906112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-06T15:16:22.934404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-06T15:16:22.962997+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-06T15:16:23.052682+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-06T15:16:23.133008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-06T15:16:23.166177+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-06T15:16:23.242850+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-06T15:16:23.719737+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-06T15:16:24.144149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-06T15:16:24.186292+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-06T15:16:24.244131+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-06T15:16:24.285935+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-06T15:16:24.353400+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-06T15:16:24.385766+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-06T15:16:24.414888+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-06T15:16:24.444456+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-06T15:16:24.511230+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-06T15:16:24.540768+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-06T15:16:24.570106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-06T15:16:24.599955+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-06T15:16:24.634559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-06T15:16:24.664433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-06T15:16:24.698770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-06T15:16:24.728209+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-06T15:16:24.759450+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-06T15:16:24.788605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-06T15:16:24.818720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-06T15:16:24.849427+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-06T15:16:24.879665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-06T15:16:24.908636+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-06T15:16:24.938010+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-06T15:16:24.967471+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-06T15:16:24.996621+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-06T15:16:25.025601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-06T15:16:25.057089+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-06T15:16:25.085227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-06T15:16:25.115810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-06T15:16:25.148435+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-06T15:16:25.181058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-06T15:16:25.217200+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-06T15:16:25.249572+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-06T15:16:25.280988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-06T15:16:25.320622+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-06T15:16:25.413189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-06T15:16:25.448969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-06T15:16:25.483451+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-06T15:16:25.530944+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-06T15:16:25.572663+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-06T15:16:25.600862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-06T15:16:25.629130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-06T15:16:25.656570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-06T15:16:25.684436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-06T15:16:25.712146+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-06T15:16:25.743432+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-06T15:16:25.773683+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-06T15:16:25.801644+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-06T15:16:25.829466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-06T15:16:25.861548+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-06T15:16:25.891263+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-06T15:16:25.921086+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-06T15:16:25.950592+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-06T15:16:25.981024+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-06T15:16:26.010013+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-06T15:16:26.040053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-06T15:16:26.069272+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-06T15:16:26.097841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-06T15:16:26.134155+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-06T15:16:26.163887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-06T15:16:26.192179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-06T15:16:26.220283+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-06T15:16:26.251004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-06T15:16:26.280992+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-06T15:16:26.312838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-06T15:16:26.345173+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-06T15:16:26.630441+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-06T15:16:26.678464+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-06T15:16:26.724362+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-06T15:16:26.753389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-06T15:16:26.781428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-06T15:16:26.810815+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-06T15:16:26.845846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-06T15:16:26.901047+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-06T15:16:26.930516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-06T15:16:26.958533+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-06T15:16:26.986703+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-06T15:16:27.017877+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-06T15:16:27.046128+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-06T15:16:27.083581+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-06T15:16:27.176363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-06T15:16:27.218261+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-06T15:16:27.248242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-06T15:16:27.278100+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-06T15:16:27.314767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-06T15:16:27.345466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-06T15:16:27.374838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-06T15:16:27.403667+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-06T15:16:27.430492+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-06T15:16:27.458405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-06T15:16:27.499237+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-06T15:16:27.527621+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-06T15:16:27.557078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-06T15:16:27.587827+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-06T15:16:27.618752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-06T15:16:27.648915+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-06T15:16:27.678531+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-06T15:16:27.735687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-06T15:16:27.764802+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-06T15:16:27.793265+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-06T15:16:27.821328+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-06T15:16:27.848512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-06T15:16:27.879203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-06T15:16:27.915812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-06T15:16:27.944524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-06T15:16:27.972751+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-06T15:16:28.004346+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-06T15:16:28.035436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-06T15:16:28.079093+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-06T15:16:28.109130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-06T15:16:28.139603+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-06T15:16:28.168749+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-06T15:16:28.205370+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-06T15:16:28.235190+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-06T15:16:28.264643+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-06T15:16:28.295388+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-06T15:16:28.330494+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-06T15:16:28.358575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-06T15:16:28.385902+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-06T15:16:28.415347+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-06T15:16:28.444762+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-06T15:16:28.495640+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-06T15:16:28.525938+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-06T15:16:28.555382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-06T15:16:28.583377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-06T15:16:28.615283+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-06T15:16:28.644651+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-06T15:16:28.674004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:16:28.705296+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:16:28.747518+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:16:28.775879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:16:28.803595+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:16:28.831232+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:16:28.859359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:16:28.886889+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:16:28.916793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:16:28.948094+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:16:29.018629+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:16:29.048053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:16:29.075793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:16:29.103471+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:16:29.130517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:16:29.160656+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:16:29.190870+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:16:29.220550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:16:29.365251+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:16:29.395531+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:16:29.425631+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:16:29.455214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:16:29.491694+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:16:29.520016+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:16:29.549249+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-06T15:16:29.577865+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-06T15:16:29.613410+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-06T15:16:29.648577+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-06T15:16:29.691008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-06T15:16:29.728392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-06T15:16:29.767809+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-06T15:16:29.798100+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-06T15:16:29.835084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-06T15:16:29.871700+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-06T15:16:29.907838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-06T15:16:29.942727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-06T15:16:29.976734+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-06T15:16:30.018378+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-06T15:16:30.058762+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-06T15:16:30.817767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-06T15:16:30.848060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-06T15:16:30.880340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-06T15:16:30.912678+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-06T15:16:30.940511+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-06T15:16:30.968362+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-06T15:16:30.998348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-06T15:16:31.027342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-06T15:16:31.065969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-06T15:16:31.098850+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-06T15:16:31.132247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-06T15:16:31.169344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-06T15:16:31.198434+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-06T15:16:31.229937+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-06T15:16:31.257794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-06T15:16:31.285559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-06T15:16:31.320650+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-06T15:16:31.355108+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-06T15:16:31.388192+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-06T15:16:31.420420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-06T15:16:31.453843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-06T15:16:31.487334+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-06T15:16:31.524025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-06T15:16:31.557119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-06T15:16:31.587069+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-06T15:16:31.616310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-06T15:16:31.644997+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-06T15:16:31.672743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-06T15:16:31.701153+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-06T15:16:31.728883+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-06T15:16:31.754876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-06T15:16:31.780671+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-06T15:16:31.807349+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-06T15:16:31.836225+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-06T15:16:31.864958+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-06T15:16:31.893672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-06T15:16:31.927794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-06T15:16:31.958595+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-06T15:16:32.020931+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-06T15:16:32.055156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-06T15:16:32.082948+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-06T15:16:32.108887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-06T15:16:32.134807+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-06T15:16:32.162942+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-06T15:16:32.201193+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-06T15:16:32.236732+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-06T15:16:32.281003+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-06T15:16:32.313412+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-06T15:16:32.346178+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-06T15:16:32.379860+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-06T15:16:32.416867+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-06T15:16:32.454181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-06T15:16:32.485917+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-06T15:16:32.525551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-06T15:16:32.557753+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-06T15:16:32.596562+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-06T15:16:32.629031+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-06T15:16:32.662832+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-06T15:16:32.696666+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-06T15:16:32.731786+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-06T15:16:32.765643+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-06T15:16:32.799885+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-06T15:16:32.829121+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-06T15:16:32.856449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-06T15:16:32.883812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-06T15:16:32.916989+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-06T15:16:32.950647+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-06T15:16:32.982780+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-06T15:16:33.022266+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-06T15:16:33.050168+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-06T15:16:33.084780+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-06T15:16:33.119853+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-06T15:16:33.163363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-06T15:16:33.192212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-06T15:16:33.223521+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-06T15:16:33.251076+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-06T15:16:33.279637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-06T15:16:33.307832+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-06T15:16:33.336235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-06T15:16:33.366018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-06T15:16:33.396162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-06T15:16:33.427833+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-06T15:16:33.461269+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-06T15:16:33.494858+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-06T15:16:33.525277+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-06T15:16:33.555491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-06T15:16:33.586330+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-06T15:16:33.618259+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-06T15:16:33.657734+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-06T15:16:33.685535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-06T15:16:33.713075+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-06T15:16:33.749695+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-06T15:16:33.777841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-06T15:16:33.809361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-06T15:16:33.841428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-06T15:16:33.871435+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-06T15:16:33.902273+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-06T15:16:33.932774+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-06T15:16:33.963781+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-06T15:16:33.993562+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-06T15:16:34.022158+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-06T15:16:34.048465+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-06T15:16:34.080376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-06T15:16:34.109492+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-06T15:16:34.139172+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-06T15:16:34.168982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-06T15:16:34.197362+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-06T15:16:34.225719+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-06T15:16:34.253788+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-06T15:16:34.285610+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-06T15:16:34.317545+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-06T15:16:34.826579+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-06T15:16:35.322990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-06T15:16:35.773643+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-06T15:16:35.808688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-06T15:16:35.836046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-06T15:16:35.876137+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-06T15:16:35.904621+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-06T15:16:35.930949+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-06T15:16:35.957407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-06T15:16:35.994418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-06T15:16:36.024509+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-06T15:16:36.053675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-06T15:16:36.082613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-06T15:16:36.112255+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-06T15:16:36.141820+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-06T15:16:36.176678+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-06T15:16:36.207904+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-06T15:16:36.238241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-06T15:16:36.264310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-06T15:16:36.302299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-06T15:16:36.335688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-06T15:16:36.365667+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-06T15:16:36.469715+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-06T15:16:36.552818+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-06T15:16:36.933174+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-06T15:16:36.991128+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-06T15:16:37.031077+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-06T15:16:37.076706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-06T15:16:37.103728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-06T15:16:37.129851+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-06T15:16:37.172633+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-06T15:16:37.203949+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-06T15:16:37.234575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-06T15:16:37.266651+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-06T15:16:37.296699+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-06T15:16:37.325365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-06T15:16:37.353861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-06T15:16:37.384734+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-06T15:16:37.411236+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-06T15:16:37.437080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-06T15:16:37.647944+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-06T15:16:37.687421+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-06T15:16:37.717848+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-06T15:16:37.754170+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-06T15:16:37.789374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-06T15:16:37.818017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-06T15:16:37.851775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-06T15:16:37.881771+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-06T15:16:37.912380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-06T15:16:37.943638+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-06T15:17:14.532313+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-06T15:17:14.563376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-06T15:17:14.600224+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-06T15:17:14.632073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-06T15:17:14.660794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-06T15:17:14.689989+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-06T15:17:14.731905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-06T15:17:14.782329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-06T15:17:14.823534+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-06T15:17:14.864171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-06T15:17:14.905008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-06T15:17:14.937603+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-06T15:17:14.967700+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-06T15:17:14.997367+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-06T15:17:15.034072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-06T15:17:15.063634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-06T15:17:15.093101+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-06T15:17:15.124791+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-06T15:17:16.165305+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-06T15:18:30.637151+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-06T15:18:30.705766+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-06T15:18:30.747504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-06T15:18:30.908033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-06T15:18:31.053781+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-06T15:18:31.354960+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-06T15:18:31.525882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-06T15:18:31.791588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-06T15:18:32.062232+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-06T15:18:32.112717+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-06T15:18:32.161232+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-06T15:18:32.220312+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-06T15:18:32.320495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-06T15:18:32.348505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-06T15:18:32.374774+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-06T15:18:32.698585+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-06T15:18:32.726667+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-06T15:18:32.752519+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-06T15:18:32.810456+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-06T15:18:32.870820+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-06T15:18:32.914407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-06T15:18:32.959639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-06T15:18:33.004883+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-06T15:18:33.050009+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-06T15:18:33.097013+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-06T15:18:33.141870+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-06T15:18:33.170035+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-06T15:18:33.301644+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-06T15:18:33.484912+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-06T15:18:33.559586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-06T15:18:33.641123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-06T15:18:33.715415+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-06T15:18:33.788927+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-06T15:18:33.904200+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-06T15:18:34.507806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-06T15:18:35.174857+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression
self = <test_golden_snapshot.TestRowCounts object at 0x10de61dc0>
artifact_entries = {'backlog_estimates': {'cols': 8, 'columns': ['advancement_days_12m_avg', 'backlog_months_to_clear_est', 'bulletin_mon...ame:str|ingested_at:datetime64[us, UTC]|iso2:str|iso3:str|region:str|source_file:str', 'numeric_bounds': {}, ...}, ...}

    def test_no_row_count_regression(self, artifact_entries):
        """

### [2026-03-06T15:18:35.771484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-06T15:18:36.422997+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped
self = <test_golden_snapshot.TestSchemaStability object at 0x10de62450>
artifact_entries = {'backlog_estimates': {'cols': 8, 'columns': ['advancement_days_12m_avg', 'backlog_months_to_clear_est', 'bulletin_mon...ame:str|ingested_at:datetime64[us, UTC]|iso2:str|iso3:str|region:str|source_file:str', 'numeric_bounds': {}, ...}, ...}

    def test_no_columns_dropped(self, artifact_entries):
        ""

### [2026-03-06T15:18:37.018745+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-06T15:18:37.631889+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-06T15:18:38.111584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-06T15:18:38.181561+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-06T15:18:38.286898+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-06T15:18:38.317474+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-06T15:18:38.347464+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-06T15:18:38.378025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-06T15:18:38.409325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-06T15:18:38.440157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-06T15:18:38.471405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-06T15:18:38.502740+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-06T15:18:38.543669+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-06T15:18:38.617436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-06T15:18:38.647792+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-06T15:18:38.679535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-06T15:18:38.709073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-06T15:18:38.740874+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-06T15:18:38.772446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-06T15:18:38.801207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-06T15:18:38.829470+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-06T15:18:38.859842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-06T15:18:38.889180+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-06T15:18:38.919709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-06T15:18:38.948651+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-06T15:18:38.976764+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-06T15:18:39.006510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-06T15:18:39.036534+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-06T15:18:39.065057+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-06T15:18:39.093362+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-06T15:18:39.121457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-06T15:18:39.150407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-06T15:18:39.178221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-06T15:18:39.206175+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-06T15:18:39.235421+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-06T15:18:39.263855+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-06T15:18:39.292096+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-06T15:18:39.321149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-06T15:18:39.350010+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-06T15:18:39.378968+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-06T15:18:39.407405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-06T15:18:39.435513+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-06T15:18:39.464064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-06T15:18:39.492292+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-06T15:18:39.519951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-06T15:18:39.546143+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-06T15:18:39.574172+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-06T15:18:39.601262+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-06T15:18:39.630496+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-06T15:18:39.660468+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-06T15:18:39.688637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-06T15:18:39.716502+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-06T15:18:39.743823+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-06T15:18:39.772040+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-06T15:18:39.797479+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-06T15:18:39.825701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-06T15:18:39.854943+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-06T15:18:39.883994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-06T15:18:39.911946+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-06T15:18:39.939410+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-06T15:18:39.966265+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-06T15:18:39.992902+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-06T15:18:40.021308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-06T15:18:40.050706+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-06T15:18:40.078985+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-06T15:18:40.107456+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-06T15:18:40.136146+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-06T15:18:40.164550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-06T15:18:40.192683+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-06T15:18:40.221513+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-06T15:18:40.250777+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-06T15:18:40.278859+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-06T15:18:40.307230+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-06T15:18:40.335951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-06T15:18:40.364786+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-06T15:18:40.393354+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-06T15:18:40.421472+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-06T15:18:40.450773+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-06T15:18:40.479779+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-06T15:18:40.508583+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-06T15:18:40.536732+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-06T15:18:40.564847+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-06T15:18:40.676396+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-06T15:18:40.750601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-06T15:18:40.778681+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-06T15:18:40.809810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-06T15:18:40.837454+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-06T15:18:40.867403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-06T15:18:40.896826+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-06T15:18:40.927058+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-06T15:18:40.956762+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-06T15:18:40.985506+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-06T15:18:41.014302+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-06T15:18:41.045183+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-06T15:18:41.080284+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-06T15:18:41.112572+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-06T15:18:41.143398+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-06T15:18:41.175571+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-06T15:18:41.208214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-06T15:18:41.241628+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-06T15:18:41.273962+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-06T15:18:41.313730+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-06T15:18:41.341838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-06T15:18:41.374978+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-06T15:18:41.403955+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-06T15:18:41.436141+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-06T15:18:41.466586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-06T15:18:41.495129+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-06T15:18:41.523946+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-06T15:18:41.551772+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-06T15:18:41.579806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-06T15:18:41.611619+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-06T15:18:41.640802+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-06T15:18:41.669115+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-06T15:18:41.696804+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-06T15:18:41.722702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-06T15:18:41.748281+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-06T15:18:41.773805+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-06T15:18:41.800102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-06T15:18:41.828287+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-06T15:18:41.860144+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-06T15:18:41.888001+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-06T15:18:41.915414+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-06T15:18:41.943208+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-06T15:18:41.969161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-06T15:18:41.997149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-06T15:18:42.027213+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-06T15:18:42.055025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-06T15:18:42.082669+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-06T15:18:42.112860+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-06T15:18:42.162885+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-06T15:18:42.196143+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-06T15:18:42.227986+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-06T15:18:42.261605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-06T15:18:42.292051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-06T15:18:42.323365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-06T15:18:42.389768+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-06T15:18:42.417404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-06T15:18:42.444089+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-06T15:18:42.470917+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-06T15:18:42.497264+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-06T15:18:42.530879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-06T15:18:42.558311+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-06T15:18:42.586027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-06T15:18:42.613934+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-06T15:18:42.641364+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-06T15:18:42.669272+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts

### [2026-03-06T15:18:42.697102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-06T15:18:42.739615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-06T15:18:42.781240+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-06T15:18:42.810097+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-06T15:18:42.837673+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-06T15:18:42.867020+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-06T15:18:42.893043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-06T15:18:42.919638+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-06T15:18:42.945644+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-06T15:18:43.053517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-06T15:19:29.914936+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=592 failed=2 exit=2

### [2026-03-06T15:22:28.634743+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T152228Z pid=30658 python=3.12.3

### [2026-03-06T15:22:28.661907+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', '-q', '--collect-only']

### [2026-03-06T15:22:29.637381+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=592 failed=0 exit=0

### [2026-03-06T15:22:38.980374+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T152238Z pid=31409 python=3.12.3

### [2026-03-06T15:22:39.008219+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', '-q']

### [2026-03-06T15:22:39.997149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-06T15:22:40.032125+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-06T15:22:40.075518+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-06T15:22:40.104617+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-06T15:22:40.140025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-06T15:22:40.187840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-06T15:22:40.226812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-06T15:22:40.483517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-06T15:22:40.516837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-06T15:22:40.547745+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-06T15:22:40.575921+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-06T15:22:40.604641+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-06T15:22:40.638262+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-06T15:22:40.666799+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-06T15:22:40.695406+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-06T15:22:40.731830+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-06T15:22:40.766517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-06T15:22:40.814665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-06T15:22:40.844964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-06T15:22:40.875329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-06T15:22:40.907120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-06T15:22:40.968489+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-06T15:22:41.006904+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-06T15:22:41.038120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-06T15:22:41.068322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-06T15:22:41.095867+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-06T15:22:41.123661+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-06T15:22:41.154090+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-06T15:22:41.185778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-06T15:22:41.215475+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-06T15:22:41.243100+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-06T15:22:41.269164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-06T15:22:41.380645+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-06T15:22:41.464295+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-06T15:22:41.655610+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-06T15:22:41.789434+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-06T15:22:41.866355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-06T15:22:41.894878+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-06T15:22:41.923608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-06T15:22:41.953169+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-06T15:22:41.982698+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-06T15:22:42.012213+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-06T15:22:42.042387+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-06T15:22:42.072520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-06T15:22:42.102339+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-06T15:22:42.182189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-06T15:22:42.262596+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-06T15:22:42.297573+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-06T15:22:42.378590+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-06T15:22:42.739018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-06T15:22:43.247890+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-06T15:22:43.294355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-06T15:22:43.353161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-06T15:22:43.396780+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-06T15:22:43.441959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-06T15:22:43.495808+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-06T15:22:43.525342+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-06T15:22:43.554519+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-06T15:22:43.620586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-06T15:22:43.650007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-06T15:22:43.679743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-06T15:22:43.708067+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-06T15:22:43.742268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-06T15:22:43.770728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-06T15:22:43.803705+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-06T15:22:43.833104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-06T15:22:43.863239+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-06T15:22:43.894296+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-06T15:22:43.925363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-06T15:22:43.955370+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-06T15:22:43.986409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-06T15:22:44.019487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-06T15:22:44.048286+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-06T15:22:44.076517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-06T15:22:44.107525+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-06T15:22:44.135760+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-06T15:22:44.167599+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-06T15:22:44.196215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-06T15:22:44.227403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-06T15:22:44.259605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-06T15:22:44.292650+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-06T15:22:44.325241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-06T15:22:44.355374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-06T15:22:44.386165+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-06T15:22:44.422925+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-06T15:22:44.482329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-06T15:22:44.545639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-06T15:22:44.580526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-06T15:22:44.628304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-06T15:22:44.669380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-06T15:22:44.699353+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-06T15:22:44.728837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-06T15:22:44.757293+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-06T15:22:44.784984+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-06T15:22:44.812465+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-06T15:22:44.840785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-06T15:22:44.871910+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-06T15:22:44.902809+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-06T15:22:44.932890+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-06T15:22:44.964328+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-06T15:22:44.994892+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-06T15:22:45.024101+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-06T15:22:45.052901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-06T15:22:45.080205+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-06T15:22:45.111411+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-06T15:22:45.139947+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-06T15:22:45.168840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-06T15:22:45.198250+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-06T15:22:45.235792+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-06T15:22:45.264244+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-06T15:22:45.294215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-06T15:22:45.326318+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-06T15:22:45.357628+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-06T15:22:45.388217+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-06T15:22:45.419172+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-06T15:22:45.448999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-06T15:22:45.719966+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-06T15:22:45.766863+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-06T15:22:45.814644+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-06T15:22:45.844072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-06T15:22:45.871643+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-06T15:22:45.901388+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-06T15:22:45.936693+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-06T15:22:45.992842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-06T15:22:46.022839+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-06T15:22:46.051265+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-06T15:22:46.079310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-06T15:22:46.105523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-06T15:22:46.131995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-06T15:22:46.166481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-06T15:22:46.257639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-06T15:22:46.301106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-06T15:22:46.331144+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-06T15:22:46.362143+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-06T15:22:46.397098+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-06T15:22:46.426449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-06T15:22:46.453900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-06T15:22:46.480579+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-06T15:22:46.509182+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-06T15:22:46.536622+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-06T15:22:46.571315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-06T15:22:46.600750+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-06T15:22:46.630064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-06T15:22:46.658980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-06T15:22:46.688758+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-06T15:22:46.718802+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-06T15:22:46.747446+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-06T15:22:46.785899+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-06T15:22:46.815605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-06T15:22:46.843781+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-06T15:22:46.872324+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-06T15:22:46.898317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-06T15:22:46.931868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-06T15:22:46.972791+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-06T15:22:47.002862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-06T15:22:47.032744+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-06T15:22:47.060672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-06T15:22:47.087783+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-06T15:22:47.122579+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-06T15:22:47.150736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-06T15:22:47.178841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-06T15:22:47.208669+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-06T15:22:47.249454+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-06T15:22:47.277220+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-06T15:22:47.307599+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-06T15:22:47.337541+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-06T15:22:47.376944+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-06T15:22:47.407430+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-06T15:22:47.437697+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-06T15:22:47.465207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-06T15:22:47.496675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-06T15:22:47.544415+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-06T15:22:47.573375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-06T15:22:47.602519+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-06T15:22:47.630072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-06T15:22:47.657449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-06T15:22:47.687412+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-06T15:22:47.716897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:22:47.746700+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:22:47.782120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:22:47.812273+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:22:47.842082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:22:47.871737+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:22:47.899903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:22:47.927675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:22:47.954985+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:22:47.984923+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:22:48.038830+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:22:48.067692+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:22:48.096225+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:22:48.125912+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:22:48.155704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:22:48.184956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:22:48.212989+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:22:48.241273+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:22:48.280517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:22:48.311605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:22:48.343036+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:22:48.375272+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:22:48.405845+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:22:48.436179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:22:48.467505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-06T15:22:48.497510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-06T15:22:48.529394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-06T15:22:48.561326+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-06T15:22:48.597846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-06T15:22:48.634004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-06T15:22:48.669907+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-06T15:22:48.699907+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-06T15:22:48.734940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-06T15:22:48.769734+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-06T15:22:48.805814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-06T15:22:48.840695+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-06T15:22:48.877712+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-06T15:22:48.918971+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-06T15:22:48.959875+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-06T15:22:49.696907+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-06T15:22:49.741240+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-06T15:22:49.779253+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-06T15:22:49.812120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-06T15:22:49.841088+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-06T15:22:49.870140+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-06T15:22:49.898876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-06T15:22:49.928574+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-06T15:22:49.963964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-06T15:22:49.997921+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-06T15:22:50.031839+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-06T15:22:50.066449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-06T15:22:50.096959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-06T15:22:50.126964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-06T15:22:50.154839+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-06T15:22:50.183306+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-06T15:22:50.213733+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-06T15:22:50.244481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-06T15:22:50.274767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-06T15:22:50.307222+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-06T15:22:50.339093+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-06T15:22:50.371024+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-06T15:22:50.412701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-06T15:22:50.447712+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-06T15:22:50.474901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-06T15:22:50.501327+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-06T15:22:50.528386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-06T15:22:50.555378+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-06T15:22:50.582053+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-06T15:22:50.610948+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-06T15:22:50.641212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-06T15:22:50.669591+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-06T15:22:50.700004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-06T15:22:50.730489+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-06T15:22:50.764810+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-06T15:22:50.793782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-06T15:22:50.827319+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-06T15:22:50.860241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-06T15:22:50.917322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-06T15:22:50.951401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-06T15:22:50.978657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-06T15:22:51.005002+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-06T15:22:51.033548+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-06T15:22:51.062620+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-06T15:22:51.095272+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-06T15:22:51.128532+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-06T15:22:51.165359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-06T15:22:51.199640+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-06T15:22:51.234922+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-06T15:22:51.268742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-06T15:22:51.305073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-06T15:22:51.339457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-06T15:22:51.372301+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-06T15:22:51.408013+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-06T15:22:51.439718+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-06T15:22:51.473612+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-06T15:22:51.508503+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-06T15:22:51.541340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-06T15:22:51.574443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-06T15:22:51.608126+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-06T15:22:51.640361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-06T15:22:51.672430+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-06T15:22:51.701827+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-06T15:22:51.729910+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-06T15:22:51.760219+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-06T15:22:51.799812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-06T15:22:51.836375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-06T15:22:51.873929+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-06T15:22:51.914793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-06T15:22:51.943727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-06T15:22:51.974857+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-06T15:22:52.008158+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-06T15:22:52.047383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-06T15:22:52.077425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-06T15:22:52.106213+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-06T15:22:52.132518+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-06T15:22:52.161320+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-06T15:22:52.190377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-06T15:22:52.218841+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-06T15:22:52.249607+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-06T15:22:52.280521+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-06T15:22:52.309675+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-06T15:22:52.339495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-06T15:22:52.369235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-06T15:22:52.396112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-06T15:22:52.424292+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-06T15:22:52.453661+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-06T15:22:52.481431+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-06T15:22:52.514581+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-06T15:22:52.541316+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-06T15:22:52.567226+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-06T15:22:52.598783+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-06T15:22:52.627957+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-06T15:22:52.660106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-06T15:22:52.692173+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-06T15:22:52.724704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-06T15:22:52.760370+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-06T15:22:52.793239+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-06T15:22:52.825302+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-06T15:22:52.858467+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-06T15:22:52.889863+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-06T15:22:52.917681+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-06T15:22:52.949538+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-06T15:22:52.980634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-06T15:22:53.009689+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-06T15:22:53.037149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-06T15:22:53.064059+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-06T15:22:53.090777+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-06T15:22:53.119310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-06T15:22:53.148536+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-06T15:22:53.175019+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-06T15:22:53.673207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-06T15:22:54.130877+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-06T15:22:54.583310+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-06T15:22:54.615062+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-06T15:22:54.642510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-06T15:22:54.683752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-06T15:22:54.710683+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-06T15:22:54.737007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-06T15:22:54.766222+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-06T15:22:54.802870+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-06T15:22:54.832463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-06T15:22:54.859607+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-06T15:22:54.889137+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-06T15:22:54.920276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-06T15:22:54.949540+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-06T15:22:54.992795+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-06T15:22:55.024041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-06T15:22:55.053123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-06T15:22:55.079535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-06T15:22:55.115033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-06T15:22:55.145180+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-06T15:22:55.174181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-06T15:22:55.264054+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-06T15:22:55.344268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-06T15:22:55.727613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-06T15:22:55.776656+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-06T15:22:55.813405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-06T15:22:55.845048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-06T15:22:55.871406+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-06T15:22:55.897051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-06T15:22:55.937868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-06T15:22:55.967871+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-06T15:22:55.996062+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-06T15:22:56.025349+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-06T15:22:56.053256+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-06T15:22:56.080083+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-06T15:22:56.108740+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-06T15:22:56.140345+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-06T15:22:56.169041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-06T15:22:56.196247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-06T15:22:56.386588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-06T15:22:56.425954+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-06T15:22:56.455363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-06T15:22:56.490379+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-06T15:22:56.525106+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-06T15:22:56.554366+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-06T15:22:56.587051+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-06T15:22:56.616384+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-06T15:22:56.648520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-06T15:22:56.679672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-06T15:23:33.184122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-06T15:23:33.215815+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-06T15:23:33.253334+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-06T15:23:33.284380+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-06T15:23:33.313130+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-06T15:23:33.339413+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-06T15:23:33.381853+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-06T15:23:33.431886+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-06T15:23:33.472419+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-06T15:23:33.513744+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-06T15:23:33.554466+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-06T15:23:33.586534+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-06T15:23:33.616626+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-06T15:23:33.646789+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-06T15:23:33.681047+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-06T15:23:33.710704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-06T15:23:33.740285+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-06T15:23:33.773508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-06T15:23:34.378368+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-06T15:24:47.717756+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-06T15:24:47.780425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-06T15:24:47.822474+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-06T15:24:47.968052+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-06T15:24:48.115876+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-06T15:24:48.403752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-06T15:24:48.583981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-06T15:24:48.876224+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-06T15:24:49.128339+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-06T15:24:49.177257+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-06T15:24:49.225929+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-06T15:24:49.282299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-06T15:24:49.384624+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-06T15:24:49.411963+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-06T15:24:49.438284+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-06T15:24:49.734092+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-06T15:24:49.764257+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-06T15:24:49.791696+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-06T15:24:49.845463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-06T15:24:49.903169+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-06T15:24:49.945429+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-06T15:24:49.991932+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-06T15:24:50.038314+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-06T15:24:50.081887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-06T15:24:50.127547+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-06T15:24:50.170248+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-06T15:24:50.198782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-06T15:24:50.274111+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-06T15:24:50.465045+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-06T15:24:50.534717+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-06T15:24:50.612259+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-06T15:24:50.683457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-06T15:24:50.753039+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-06T15:24:50.847487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-06T15:24:51.467136+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-06T15:24:52.074774+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression
self = <test_golden_snapshot.TestRowCounts object at 0x110bcd670>
artifact_entries = {'backlog_estimates': {'cols': 8, 'columns': ['advancement_days_12m_avg', 'backlog_months_to_clear_est', 'bulletin_mon...ame:str|ingested_at:datetime64[us, UTC]|iso2:str|iso3:str|region:str|source_file:str', 'numeric_bounds': {}, ...}, ...}

    def test_no_row_count_regression(self, artifact_entries):
        """

### [2026-03-06T15:24:52.726176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-06T15:24:53.320437+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped
self = <test_golden_snapshot.TestSchemaStability object at 0x110bf1c40>
artifact_entries = {'backlog_estimates': {'cols': 8, 'columns': ['advancement_days_12m_avg', 'backlog_months_to_clear_est', 'bulletin_mon...ame:str|ingested_at:datetime64[us, UTC]|iso2:str|iso3:str|region:str|source_file:str', 'numeric_bounds': {}, ...}, ...}

    def test_no_columns_dropped(self, artifact_entries):
        ""

### [2026-03-06T15:24:53.939922+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-06T15:24:54.558376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-06T15:24:55.049012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-06T15:24:55.101913+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-06T15:24:55.198907+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-06T15:24:55.227989+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-06T15:24:55.258457+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-06T15:24:55.286399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-06T15:24:55.315281+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-06T15:24:55.344326+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-06T15:24:55.373525+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-06T15:24:55.403755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-06T15:24:55.443945+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-06T15:24:55.519588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-06T15:24:55.551536+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-06T15:24:55.584204+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-06T15:24:55.615726+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-06T15:24:55.650968+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-06T15:24:55.684348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-06T15:24:55.714121+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-06T15:24:55.743571+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-06T15:24:55.771821+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-06T15:24:55.799822+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-06T15:24:55.830726+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-06T15:24:55.861157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-06T15:24:55.890924+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-06T15:24:55.920403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-06T15:24:55.949578+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-06T15:24:55.977090+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-06T15:24:56.006018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-06T15:24:56.034922+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-06T15:24:56.062882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-06T15:24:56.088892+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-06T15:24:56.116733+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-06T15:24:56.144726+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-06T15:24:56.173183+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-06T15:24:56.201932+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-06T15:24:56.233899+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-06T15:24:56.264671+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-06T15:24:56.293473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-06T15:24:56.323656+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-06T15:24:56.352286+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-06T15:24:56.381885+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-06T15:24:56.409951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-06T15:24:56.438164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-06T15:24:56.465935+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-06T15:24:56.494308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-06T15:24:56.522541+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-06T15:24:56.550868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-06T15:24:56.580524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-06T15:24:56.608880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-06T15:24:56.638415+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-06T15:24:56.668112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-06T15:24:56.702122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-06T15:24:56.730902+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-06T15:24:56.759461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-06T15:24:56.788779+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-06T15:24:56.818108+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-06T15:24:56.846968+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-06T15:24:56.875524+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-06T15:24:56.903360+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-06T15:24:56.930958+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-06T15:24:56.956900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-06T15:24:56.982538+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-06T15:24:57.010060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-06T15:24:57.038728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-06T15:24:57.066949+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-06T15:24:57.095425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-06T15:24:57.123793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-06T15:24:57.151628+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-06T15:24:57.179752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-06T15:24:57.207660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-06T15:24:57.237246+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-06T15:24:57.265927+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-06T15:24:57.296799+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-06T15:24:57.325535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-06T15:24:57.354026+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-06T15:24:57.383164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-06T15:24:57.411855+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-06T15:24:57.440361+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-06T15:24:57.469933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-06T15:24:57.499220+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-06T15:24:57.667714+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-06T15:24:57.737140+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-06T15:24:57.765279+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-06T15:24:57.795589+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-06T15:24:57.824164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-06T15:24:57.853966+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-06T15:24:57.883688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-06T15:24:57.915195+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-06T15:24:57.946047+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-06T15:24:57.976791+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-06T15:24:58.007200+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-06T15:24:58.036973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-06T15:24:58.067123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-06T15:24:58.096123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-06T15:24:58.124207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-06T15:24:58.153008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-06T15:24:58.184180+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-06T15:24:58.213744+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-06T15:24:58.243988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-06T15:24:58.279211+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-06T15:24:58.310728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-06T15:24:58.344554+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-06T15:24:58.373008+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-06T15:24:58.403979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-06T15:24:58.433214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-06T15:24:58.461884+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-06T15:24:58.490850+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-06T15:24:58.519678+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-06T15:24:58.548667+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-06T15:24:58.583736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-06T15:24:58.612214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-06T15:24:58.641102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-06T15:24:58.669126+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-06T15:24:58.697422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-06T15:24:58.726202+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-06T15:24:58.754660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-06T15:24:58.783684+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-06T15:24:58.812338+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-06T15:24:58.845107+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-06T15:24:58.873433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-06T15:24:58.901728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-06T15:24:58.930073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-06T15:24:58.959619+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-06T15:24:58.988776+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-06T15:24:59.017575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-06T15:24:59.046738+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-06T15:24:59.076030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-06T15:24:59.104636+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-06T15:24:59.143288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-06T15:24:59.175770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-06T15:24:59.207211+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-06T15:24:59.240930+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-06T15:24:59.271322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-06T15:24:59.299647+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-06T15:24:59.375982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-06T15:24:59.405440+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-06T15:24:59.434354+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-06T15:24:59.462797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-06T15:24:59.489852+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-06T15:24:59.520474+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-06T15:24:59.548530+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-06T15:24:59.575729+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-06T15:24:59.605541+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-06T15:24:59.636377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-06T15:24:59.665540+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts

### [2026-03-06T15:24:59.695139+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-06T15:24:59.743671+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-06T15:24:59.782939+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-06T15:24:59.810566+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-06T15:24:59.836962+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-06T15:24:59.869499+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-06T15:24:59.898027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-06T15:24:59.927057+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-06T15:24:59.957588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-06T15:25:00.053022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-06T15:27:45.279734+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=592 failed=2 exit=2

### [2026-03-06T15:34:25.806722+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T153425Z pid=68650 python=3.12.3

### [2026-03-06T15:34:25.833956+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_smoke.py', '-q']

### [2026-03-06T15:34:25.925061+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-06T15:34:25.956889+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-06T15:34:28.337124+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-06T15:35:00.401624+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=4 failed=0 exit=2

### [2026-03-06T15:35:19.867352+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T153519Z pid=71942 python=3.12.3

### [2026-03-06T15:35:19.895905+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_dim_area.py', 'tests/test_dim_country.py', 'tests/test_dim_soc.py', 'tests/test_dim_employer.py', 'tests/test_dim_visa_class.py', 'tests/test_data_sanity.py', 'tests/test_paths_check.py', 'tests/test_fact_perm.py', 'tests/test_fact_cutoffs.py', 'tests/test_fact_oews.py', 'tests/test_coverage_expectations.py', 'tests/test_normalization_mappings.py', 'tests/test_employer_name_normalization.py', '-q', '--tb=short']

### [2026-03-06T15:35:57.091429+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-06T15:35:57.120973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-06T15:35:57.157363+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-06T15:35:57.188395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-06T15:35:57.218728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-06T15:35:57.254905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-06T15:35:57.284223+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-06T15:35:57.313649+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-06T15:35:57.341283+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-06T15:35:57.394984+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-06T15:35:57.445134+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-06T15:35:57.485608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-06T15:35:57.528484+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-06T15:35:57.569495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-06T15:35:57.607525+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-06T15:35:57.636282+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-06T15:35:57.665198+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-06T15:35:57.693136+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-06T15:35:57.725244+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-06T15:35:57.754041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-06T15:35:57.788624+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-06T15:35:57.820290+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-06T15:35:57.850274+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-06T15:35:57.880367+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-06T15:35:57.918838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-06T15:35:57.949688+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-06T15:35:57.978095+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-06T15:35:58.007080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-06T15:35:58.036312+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-06T15:35:58.064570+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-06T15:35:58.109329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-06T15:35:58.140843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-06T15:35:58.174448+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-06T15:35:58.203023+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-06T15:35:58.245384+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-06T15:35:58.276048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-06T15:35:58.309033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-06T15:35:58.411882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-06T15:35:58.488353+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-06T15:35:58.856027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-06T15:35:58.904517+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-06T15:35:58.945377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-06T15:35:58.980436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-06T15:35:59.008932+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-06T15:35:59.035108+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-06T15:35:59.074092+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-06T15:35:59.104467+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-06T15:35:59.136808+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-06T15:35:59.165382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-06T15:35:59.195265+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-06T15:35:59.225602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-06T15:35:59.254914+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-06T15:35:59.291111+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-06T15:35:59.321111+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-06T15:35:59.350197+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-06T15:35:59.546199+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-06T15:35:59.589156+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-06T15:35:59.619559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-06T15:35:59.655430+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-06T15:35:59.690203+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-06T15:35:59.720849+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-06T15:35:59.755478+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-06T15:35:59.786305+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-06T15:35:59.818449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-06T15:35:59.850187+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-06T15:35:59.941724+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-06T15:36:00.038385+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-06T15:36:00.067615+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-06T15:36:00.202554+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-06T15:36:00.384410+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-06T15:36:00.451602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-06T15:36:00.528322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-06T15:36:00.596142+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-06T15:36:00.667612+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-06T15:36:00.762069+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-06T15:36:01.092443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-06T15:36:01.121365+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-06T15:36:01.150188+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-06T15:36:01.196377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-06T15:36:01.269454+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-06T15:36:01.314897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-06T15:36:01.360396+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-06T15:36:01.405433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-06T15:36:01.448424+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-06T15:36:01.493723+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-06T15:36:01.538303+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-06T15:36:02.009411+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-06T15:36:02.489516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-06T15:36:02.955185+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-06T15:36:02.984289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-06T15:36:03.010793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-06T15:36:03.036827+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-06T15:36:03.064176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-06T15:36:03.091303+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-06T15:36:03.117212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-06T15:36:03.143046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-06T15:36:03.169973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-06T15:36:03.197392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-06T15:36:03.226072+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-06T15:36:03.253268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-06T15:36:03.281057+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-06T15:36:03.310964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-06T15:36:03.340307+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-06T15:36:03.369173+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-06T15:36:03.398171+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-06T15:36:03.427401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-06T15:36:03.456770+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-06T15:36:03.485622+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-06T15:36:03.514564+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-06T15:36:03.542080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-06T15:36:03.568452+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-06T15:36:03.594520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-06T15:36:03.620258+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-06T15:36:03.649538+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-06T15:36:03.676897+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-06T15:36:03.708205+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-06T15:36:03.735241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-06T15:36:03.761632+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-06T15:36:03.787403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-06T15:36:03.814904+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-06T15:36:03.844967+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-06T15:36:03.874491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-06T15:36:03.904523+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-06T15:36:03.932576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-06T15:36:03.962600+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-06T15:36:03.991084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-06T15:36:04.020717+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-06T15:36:04.051663+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-06T15:36:04.081315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-06T15:36:04.110014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-06T15:36:04.138329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-06T15:36:04.166014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-06T15:36:04.194938+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-06T15:36:04.226443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-06T15:36:04.254986+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-06T15:36:04.284135+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-06T15:36:04.312164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-06T15:36:04.339956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-06T15:36:04.367627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-06T15:36:04.394382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-06T15:36:04.422126+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-06T15:36:04.449891+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-06T15:36:04.478657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-06T15:36:04.507262+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-06T15:36:04.536514+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-06T15:36:04.566506+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-06T15:36:04.595399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-06T15:36:04.656879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-06T15:36:04.698691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-06T15:36:04.862604+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-06T15:36:05.006315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-06T15:36:05.310102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-06T15:36:05.487590+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-06T15:36:05.746724+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-06T15:36:05.993550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-06T15:36:06.046616+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-06T15:36:06.094648+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-06T15:36:06.153268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-06T15:36:06.249994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-06T15:36:06.280584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-06T15:36:06.307482+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-06T15:36:06.335553+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=161 failed=0 exit=0

### [2026-03-06T15:36:12.674398+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T153612Z pid=74906 python=3.12.3

### [2026-03-06T15:36:12.704203+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/p3_metrics/', 'tests/test_rag_artifacts.py', 'tests/test_rag_quality.py', 'tests/test_queue_depth_estimates.py', 'tests/test_approval_denial_trends.py', 'tests/test_new_p1_tables.py', 'tests/datasets/', '-q', '--tb=short']

### [2026-03-06T15:36:13.166940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-06T15:36:13.200763+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-06T15:36:13.235674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-06T15:36:13.264398+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-06T15:36:13.293574+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-06T15:36:13.328321+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-06T15:36:13.362194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-06T15:36:13.398428+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-06T15:36:13.434940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-06T15:36:13.470154+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-06T15:36:13.504526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-06T15:36:13.542759+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-06T15:36:13.578075+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-06T15:36:13.614438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-06T15:36:13.652783+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-06T15:36:13.688477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-06T15:36:13.769230+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-06T15:36:13.804775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-06T15:36:13.838954+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-06T15:36:13.874295+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-06T15:36:13.908179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-06T15:36:13.940980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-06T15:36:13.973260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-06T15:36:14.001520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-06T15:36:14.029547+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-06T15:36:14.057837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-06T15:36:14.090765+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-06T15:36:14.123548+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-06T15:36:14.157464+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-06T15:36:14.195387+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-06T15:36:14.225892+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-06T15:36:14.258491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-06T15:36:14.290241+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-06T15:36:14.332073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-06T15:36:14.361935+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-06T15:36:14.394060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_required_columns_present

### [2026-03-06T15:36:14.421062+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_non_empty

### [2026-03-06T15:36:14.448060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_no_negative_market_median

### [2026-03-06T15:36:14.474900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_n_employers_non_negative

### [2026-03-06T15:36:14.503139+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_total_filings_positive

### [2026-03-06T15:36:14.533120+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_percentile_monotonicity_zero_violations

### [2026-03-06T15:36:14.562251+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_market_median_corrected_range

### [2026-03-06T15:36:14.590211+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_15_1252_h1b_fy2025_not_biased

### [2026-03-06T15:36:14.619622+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_h1b_high_volume_socs_reasonable_medians

### [2026-03-06T15:36:14.650648+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_salary_market.py::test_actual_parquet_zero_monotonicity_violations

### [2026-03-06T15:36:14.679681+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-06T15:36:14.708749+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-06T15:36:14.737924+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-06T15:36:14.767539+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-06T15:36:14.800827+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-06T15:36:14.829953+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-06T15:36:14.858781+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-06T15:36:14.891043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-06T15:36:14.919900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-06T15:36:14.948419+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-06T15:36:14.975377+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-06T15:36:15.001561+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-06T15:36:15.029578+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-06T15:36:15.059308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-06T15:36:15.088607+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-06T15:36:15.121470+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-06T15:36:15.149338+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-06T15:36:15.180743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-06T15:36:15.211557+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-06T15:36:15.241956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-06T15:36:15.269313+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-06T15:36:15.298495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-06T15:36:15.327496+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-06T15:36:15.355596+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-06T15:36:15.387617+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-06T15:36:15.416661+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-06T15:36:15.444600+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-06T15:36:15.475261+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-06T15:36:15.503080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-06T15:36:15.530702+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-06T15:36:15.559350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-06T15:36:15.587908+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-06T15:36:15.614962+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-06T15:36:15.646464+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-06T15:36:15.688875+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-06T15:36:15.721795+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-06T15:36:15.753884+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-06T15:36:15.787359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-06T15:36:15.818981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-06T15:36:15.895762+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-06T15:36:16.014754+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-06T15:36:16.046937+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-06T15:36:16.078433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-06T15:36:16.107405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-06T15:36:16.135961+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-06T15:36:16.166125+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-06T15:36:16.195463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-06T15:36:16.241692+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-06T15:36:16.291475+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-06T15:36:16.336288+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-06T15:36:16.368834+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts

### [2026-03-06T15:36:16.397657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-06T15:36:16.440768+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-06T15:36:16.482122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-06T15:36:16.510814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-06T15:36:16.538580+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-06T15:36:16.569060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-06T15:36:16.597315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-06T15:36:16.626246+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-06T15:36:16.657134+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-06T15:36:16.686544+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-06T15:36:16.716343+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-06T15:36:16.747049+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-06T15:36:16.777546+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-06T15:36:16.807304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-06T15:36:16.836977+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-06T15:36:16.866165+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-06T15:36:16.895535+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-06T15:36:16.926468+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-06T15:36:16.956556+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-06T15:36:16.987437+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-06T15:36:17.018007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-06T15:36:17.049788+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-06T15:36:17.081516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-06T15:36:17.112789+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-06T15:36:17.147612+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-06T15:36:17.177637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-06T15:36:17.207930+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-06T15:36:17.237248+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_exists

### [2026-03-06T15:36:17.269050+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_artifact_loads

### [2026-03-06T15:36:17.298085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_required_columns

### [2026-03-06T15:36:17.326950+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_data_sources

### [2026-03-06T15:36:17.356160+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_fiscal_years_coverage

### [2026-03-06T15:36:17.385373+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_approval_rates_reasonable

### [2026-03-06T15:36:17.414521+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_counts_non_negative

### [2026-03-06T15:36:17.445704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_totals_sum_correctly

### [2026-03-06T15:36:17.477450+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialTrends::test_perm_approval_rate_reasonable

### [2026-03-06T15:36:17.506741+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_exists

### [2026-03-06T15:36:17.538611+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_artifact_loads

### [2026-03-06T15:36:17.568117+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_required_columns

### [2026-03-06T15:36:17.598338+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestApprovalDenialDetailed::test_granular_breakdown_exists

### [2026-03-06T15:36:17.627614+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_approval_denial_trends_json_exists

### [2026-03-06T15:36:17.656166+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_exists

### [2026-03-06T15:36:17.684750+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_by_category_json_exists

### [2026-03-06T15:36:17.712491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_perm_trends_detailed_json_exists

### [2026-03-06T15:36:17.742512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_summary_json_content

### [2026-03-06T15:36:17.772214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_approval_denial_trends.py::TestP3Exports::test_trends_json_content

### [2026-03-06T15:36:17.829123+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-06T15:36:17.929071+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-06T15:36:17.957905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-06T15:36:17.989321+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-06T15:36:18.017393+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-06T15:36:18.046187+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-06T15:36:18.075992+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-06T15:36:18.106294+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-06T15:36:18.135339+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-06T15:36:18.176001+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-06T15:36:18.243563+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-06T15:36:18.273511+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-06T15:36:18.305224+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-06T15:36:18.334746+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-06T15:36:18.366905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-06T15:36:18.395162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-06T15:36:18.421911+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-06T15:36:18.449676+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-06T15:36:18.477798+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-06T15:36:18.506149+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-06T15:36:18.533822+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-06T15:36:18.567574+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-06T15:36:18.600048+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-06T15:36:18.644046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-06T15:36:18.676704+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-06T15:36:18.711050+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-06T15:36:18.758146+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-06T15:36:18.794618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-06T15:36:19.058778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-06T15:36:19.090458+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-06T15:36:19.120087+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-06T15:36:19.151112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-06T15:36:19.180713+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-06T15:36:19.215755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-06T15:36:19.246605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-06T15:36:19.280583+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-06T15:36:19.316035+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-06T15:36:19.350601+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-06T15:36:19.401298+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-06T15:36:19.433557+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-06T15:36:19.464028+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-06T15:36:19.495039+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-06T15:36:19.552709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-06T15:36:19.587762+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-06T15:36:19.618678+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-06T15:36:19.647933+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-06T15:36:19.676281+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-06T15:36:19.706389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-06T15:36:19.735634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-06T15:36:19.765469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-06T15:36:19.795567+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-06T15:36:19.824313+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-06T15:36:19.854187+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-06T15:36:19.954687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-06T15:36:20.042021+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-06T15:36:20.250548+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-06T15:36:20.387470+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-06T15:36:20.472726+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-06T15:36:20.506159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-06T15:36:20.538411+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-06T15:36:20.569299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-06T15:36:20.601382+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-06T15:36:20.631441+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-06T15:36:20.661980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-06T15:36:20.693598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-06T15:36:20.723588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-06T15:36:20.809068+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-06T15:36:20.892260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-06T15:36:20.924665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-06T15:36:21.002142+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-06T15:36:21.367022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-06T15:36:21.817501+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-06T15:36:21.859946+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-06T15:36:21.914985+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-06T15:36:21.957727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-06T15:36:22.001050+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-06T15:36:22.030270+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-06T15:36:22.062948+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-06T15:36:22.092942+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-06T15:36:22.157320+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-06T15:36:22.186677+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-06T15:36:22.217909+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-06T15:36:22.249604+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-06T15:36:22.329840+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-06T15:36:22.363189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-06T15:36:22.395735+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-06T15:36:22.425134+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-06T15:36:22.453657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-06T15:36:22.481618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-06T15:36:22.509815+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-06T15:36:22.537232+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-06T15:36:22.565918+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-06T15:36:22.594000+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-06T15:36:22.622791+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-06T15:36:22.652802+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-06T15:36:22.682988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-06T15:36:22.713982+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-06T15:36:22.747133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-06T15:36:22.778205+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-06T15:36:22.808976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-06T15:36:22.841154+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-06T15:36:22.870340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-06T15:36:22.902935+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-06T15:36:22.933782+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-06T15:36:22.965157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-06T15:36:23.001365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-06T15:36:23.060473+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-06T15:36:23.094838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-06T15:36:23.129510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-06T15:36:23.182530+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-06T15:36:23.227389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-06T15:36:23.256391+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-06T15:36:23.285350+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-06T15:36:23.313495+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-06T15:36:23.344191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-06T15:36:23.373078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-06T15:36:23.402796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-06T15:36:23.430901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-06T15:36:23.461682+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-06T15:36:23.493371+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-06T15:36:23.525131+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-06T15:36:23.556660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-06T15:36:23.587100+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-06T15:36:23.616189+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-06T15:36:23.645181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-06T15:36:23.675302+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-06T15:36:23.704423+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-06T15:36:23.734084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-06T15:36:23.765598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-06T15:36:23.802923+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-06T15:36:23.837407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-06T15:36:23.869074+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-06T15:36:23.899566+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-06T15:36:23.929670+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-06T15:36:23.959585+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-06T15:36:23.988485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-06T15:36:24.018722+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-06T15:36:24.294227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-06T15:36:24.337905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-06T15:36:24.385604+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-06T15:36:24.415727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-06T15:36:24.444846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-06T15:36:24.475555+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-06T15:36:24.511334+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-06T15:36:24.541031+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=286 failed=0 exit=0

### [2026-03-06T15:36:29.090509+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T153629Z pid=75945 python=3.12.3

### [2026-03-06T15:36:29.118854+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/p2_gap_curation/', 'tests/p2_hardening/', 'tests/models/', 'tests/test_golden_snapshot.py', 'tests/test_dry_run.py', '-q', '--tb=short']

### [2026-03-06T15:36:29.580510+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-06T15:36:29.611701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-06T15:36:29.641995+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-06T15:36:29.671763+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-06T15:36:29.698626+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-06T15:36:29.728672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-06T15:36:29.772337+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-06T15:36:29.803935+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-06T15:36:29.834079+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-06T15:36:29.862385+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-06T15:36:29.892162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-06T15:36:29.930874+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-06T15:36:29.960711+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-06T15:36:29.993658+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-06T15:36:30.027974+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-06T15:36:30.065049+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-06T15:36:30.095304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-06T15:36:30.124747+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-06T15:36:30.154556+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-06T15:36:30.192262+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-06T15:36:30.223521+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-06T15:36:30.254609+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-06T15:36:30.286074+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-06T15:36:30.316497+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-06T15:36:30.369248+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-06T15:36:30.399422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-06T15:36:30.429080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-06T15:36:30.459802+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-06T15:36:30.491164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-06T15:36:30.521529+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-06T15:36:30.550824+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:36:30.580429+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:36:30.617715+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:36:30.647238+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:36:30.676915+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:36:30.707714+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:36:30.737861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:36:30.771450+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:36:30.802110+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:36:30.831677+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:36:30.890394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:36:30.922695+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:36:30.951469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:36:30.981268+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:36:31.010814+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:36:31.039443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:36:31.068647+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-06T15:36:31.097085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-06T15:36:31.132117+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-06T15:36:31.162362+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-06T15:36:31.192490+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-06T15:36:31.222070+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-06T15:36:31.251540+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-06T15:36:31.281110+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-06T15:36:31.311819+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-06T15:36:31.344916+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-06T15:36:31.379964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-06T15:36:31.416104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-06T15:36:31.456895+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-06T15:36:31.495420+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-06T15:36:31.532219+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-06T15:36:31.565139+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-06T15:36:31.603012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-06T15:36:31.641445+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-06T15:36:31.677842+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-06T15:36:31.714722+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-06T15:36:31.749448+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-06T15:36:31.791075+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-06T15:36:31.834221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-06T15:36:32.614162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-06T15:36:32.680375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-06T15:36:32.718179+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-06T15:36:32.753720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-06T15:36:32.783620+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-06T15:36:32.813432+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-06T15:36:32.843222+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-06T15:36:32.873981+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-06T15:36:32.909623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-06T15:36:32.947107+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-06T15:36:32.980763+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-06T15:36:33.016021+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-06T15:36:33.047088+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-06T15:36:33.076668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-06T15:36:33.103910+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-06T15:36:33.130579+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-06T15:36:33.160271+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-06T15:36:33.191252+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-06T15:36:33.222104+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-06T15:36:33.251392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-06T15:36:33.280822+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-06T15:36:33.311386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-06T15:36:33.350339+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-06T15:36:33.386736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-06T15:36:33.415629+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-06T15:36:33.445878+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-06T15:36:33.475025+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-06T15:36:33.504408+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-06T15:36:33.533391+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-06T15:36:33.562851+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-06T15:36:33.593159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-06T15:36:33.621919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-06T15:36:33.650027+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-06T15:36:33.678747+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-06T15:36:33.709299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-06T15:36:33.739698+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-06T15:36:33.775646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-06T15:36:33.810386+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-06T15:36:33.877550+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-06T15:36:33.933861+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-06T15:36:33.964012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-06T15:36:33.994994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-06T15:36:34.025646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-06T15:36:34.058173+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-06T15:36:34.087522+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-06T15:36:34.123741+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-06T15:36:34.214920+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-06T15:36:34.257098+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-06T15:36:34.287848+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-06T15:36:34.318789+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-06T15:36:34.354005+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-06T15:36:34.384502+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-06T15:36:34.413573+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-06T15:36:34.442658+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-06T15:36:34.471212+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-06T15:36:34.500326+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-06T15:36:34.537494+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-06T15:36:34.566443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-06T15:36:34.596461+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-06T15:36:34.626512+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-06T15:36:34.655491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-06T15:36:34.683959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-06T15:36:34.716240+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-06T15:36:35.312972+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-06T15:36:35.938590+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression
tests/test_golden_snapshot.py:120: in test_no_row_count_regression
    assert not failures, (
E   AssertionError: Row count regressions detected:
E       • soc_demand_metrics: golden=4,241 → current=3,968 (dropped 6.4%)
E       • worksite_geo_metrics: golden=156,171 → current=134,799 (dropped 13.7%)
E   assert not ['soc_demand_metrics: golden=4,241 → current=3,968 (dropped 6.4%)', 'worksite_geo_me

### [2026-03-06T15:36:36.617315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-06T15:36:37.220580+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped
tests/test_golden_snapshot.py:175: in test_no_columns_dropped
    assert not dropped, (
E   AssertionError: Columns dropped from artifacts:
E       • employer_features: lost columns ['lca_approval_rate_24m', 'lca_filings_24m']
E       • employer_friendliness_scores: lost columns ['lca_approval_rate_24m', 'lca_filings_24m']
E   assert not ["employer_features: lost columns ['lca_approval_rate_24m', 

### [2026-03-06T15:36:37.865736+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-06T15:36:38.462271+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-06T15:36:38.990132+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-06T15:36:39.530817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-06T15:37:54.953404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-06T15:37:54.985639+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=141 failed=2 exit=1

### [2026-03-06T15:38:03.271388+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T153803Z pid=80756 python=3.12.3

### [2026-03-06T15:38:03.300287+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_golden_snapshot.py', '-q', '--tb=long']

### [2026-03-06T15:38:04.552434+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-06T15:38:05.178529+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression
self = <test_golden_snapshot.TestRowCounts object at 0x106d7ac30>
artifact_entries = {'backlog_estimates': {'cols': 8, 'columns': ['advancement_days_12m_avg', 'backlog_months_to_clear_est', 'bulletin_mon...ame:str|ingested_at:datetime64[us, UTC]|iso2:str|iso3:str|region:str|source_file:str', 'numeric_bounds': {}, ...}, ...}

    def test_no_row_count_regression(self, artifact_entries):
        """

### [2026-03-06T15:38:05.824235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-06T15:38:06.429637+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped
self = <test_golden_snapshot.TestSchemaStability object at 0x10c63a270>
artifact_entries = {'backlog_estimates': {'cols': 8, 'columns': ['advancement_days_12m_avg', 'backlog_months_to_clear_est', 'bulletin_mon...ame:str|ingested_at:datetime64[us, UTC]|iso2:str|iso3:str|region:str|source_file:str', 'numeric_bounds': {}, ...}, ...}

    def test_no_columns_dropped(self, artifact_entries):
        ""

### [2026-03-06T15:38:07.088413+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-06T15:38:07.666959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-06T15:38:08.168236+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-06T15:38:08.203519+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=7 failed=2 exit=1

### [2026-03-06T15:38:13.738427+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T153813Z pid=81465 python=3.12.3

### [2026-03-06T15:38:13.768850+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_golden_snapshot.py', '-q', '--tb=long']

### [2026-03-06T15:38:14.839168+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-06T15:38:15.466303+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression
self = <test_golden_snapshot.TestRowCounts object at 0x10850fbc0>
artifact_entries = {'backlog_estimates': {'cols': 8, 'columns': ['advancement_days_12m_avg', 'backlog_months_to_clear_est', 'bulletin_mon...ame:str|ingested_at:datetime64[us, UTC]|iso2:str|iso3:str|region:str|source_file:str', 'numeric_bounds': {}, ...}, ...}

    def test_no_row_count_regression(self, artifact_entries):
        """

### [2026-03-06T15:38:16.217421+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-06T15:38:16.830783+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped
self = <test_golden_snapshot.TestSchemaStability object at 0x10ddea9f0>
artifact_entries = {'backlog_estimates': {'cols': 8, 'columns': ['advancement_days_12m_avg', 'backlog_months_to_clear_est', 'bulletin_mon...ame:str|ingested_at:datetime64[us, UTC]|iso2:str|iso3:str|region:str|source_file:str', 'numeric_bounds': {}, ...}, ...}

    def test_no_columns_dropped(self, artifact_entries):
        ""

### [2026-03-06T15:38:17.497477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-06T15:38:18.085055+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-06T15:38:18.592463+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-06T15:38:18.625219+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=7 failed=2 exit=1

### [2026-03-06T15:38:39.203290+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T153839Z pid=83162 python=3.12.3

### [2026-03-06T15:38:39.234416+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_golden_snapshot.py', '-q', '--tb=short']

### [2026-03-06T15:38:40.285650+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-06T15:38:40.900585+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression

### [2026-03-06T15:38:41.564936+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-06T15:38:42.138490+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped

### [2026-03-06T15:38:42.815838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-06T15:38:43.439621+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-06T15:38:43.924383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-06T15:38:43.955600+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=7 failed=0 exit=0

### [2026-03-06T15:38:48.893565+00:00] *System*

 [bootstrap]  
SESSION_START session=20260306T153848Z pid=83744 python=3.12.3

### [2026-03-06T15:38:48.928091+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_smoke.py', '-q', '--tb=short']

### [2026-03-06T15:38:48.989888+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-06T15:38:49.020681+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-06T15:38:49.469376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-06T15:41:03.221912+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=4 failed=0 exit=2

