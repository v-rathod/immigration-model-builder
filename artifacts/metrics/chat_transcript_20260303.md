# Chat Transcript

### New transcript started 2026-03-02T03:46:40.930095+00:00 (reason=daily)

### [2026-03-02T03:46:40.930770+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T034640Z pid=82893 python=3.12.3

### [2026-03-02T03:46:40.961477+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_normalization_mappings.py', 'tests/test_employer_name_normalization.py', '-v']

### [2026-03-02T03:46:41.402351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-02T03:46:41.431168+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-02T03:46:41.458760+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-02T03:46:41.486266+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-02T03:46:41.511718+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-02T03:46:41.539087+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-02T03:46:41.566455+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-02T03:46:41.593269+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-02T03:46:41.619626+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-02T03:46:41.646125+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-02T03:46:41.673687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-02T03:46:41.700806+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-02T03:46:41.729082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-02T03:46:41.755122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-02T03:46:41.782158+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-02T03:46:41.830052+00:00] > **Agent**

`ERROR` [pytest]  
TEST FAIL: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication
self = <test_normalization_mappings.TestNormalizeEmployerName object at 0x109fb9f10>

    def test_tcs_deduplication(self):
        """All common TCS variants should normalize to the same key."""
        variants = [
            "Tata Consultancy Services Limited",
            "TATA CONSULTANCY SERVICES LIMITED",
            "Tata Consultancy Services Ltd",
            "TATA CONSULTANCY SERVICES L

### [2026-03-02T03:46:41.858125+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-02T03:46:41.884816+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-02T03:46:41.912477+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-02T03:46:41.940930+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-02T03:46:41.968595+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-02T03:46:41.995829+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-02T03:46:42.023227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-02T03:46:42.050769+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-02T03:46:42.077738+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-02T03:46:42.104497+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-02T03:46:42.132956+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-02T03:46:42.160973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-02T03:46:42.190018+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-02T03:46:42.218835+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-02T03:46:42.248160+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-02T03:46:42.277290+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-02T03:46:42.305061+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-02T03:46:42.331866+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-02T03:46:42.358882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-02T03:46:42.386722+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-02T03:46:42.413587+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-02T03:46:42.440796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-02T03:46:42.469776+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-02T03:46:42.496691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-02T03:46:42.524634+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-02T03:46:42.553900+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-02T03:46:42.581663+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-02T03:46:42.608611+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-02T03:46:42.636406+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-02T03:46:42.663191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-02T03:46:42.691354+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-02T03:46:42.721131+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-02T03:46:42.750893+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-02T03:46:42.779359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-02T03:46:42.807709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-02T03:46:42.835860+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-02T03:46:42.865227+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-02T03:46:42.894082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-02T03:46:42.922164+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-02T03:46:42.950843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-02T03:46:42.978159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-02T03:46:43.006175+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-02T03:46:43.101775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-02T03:46:43.143333+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-02T03:46:43.293685+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-02T03:46:43.439399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-02T03:46:43.736623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-02T03:46:43.906368+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-02T03:46:44.157868+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-02T03:46:44.416620+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-02T03:46:44.467099+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-02T03:46:44.517378+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-02T03:46:44.574979+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-02T03:46:44.673593+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-02T03:46:44.702458+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-02T03:46:44.729184+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-02T03:46:44.756521+00:00] > **Agent**

`WARN` [pytest]  
pytest FINISHED: collected=72 failed=1 exit=1

### [2026-03-02T03:47:01.887639+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T034701Z pid=84154 python=3.12.3

### [2026-03-02T03:47:01.914882+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/test_normalization_mappings.py', 'tests/test_employer_name_normalization.py', '-v', '--tb=short']

### [2026-03-02T03:47:02.294181+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-02T03:47:02.322229+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-02T03:47:02.349154+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-02T03:47:02.375864+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-02T03:47:02.404046+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-02T03:47:02.431247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-02T03:47:02.458191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-02T03:47:02.485249+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-02T03:47:02.515846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-02T03:47:02.556167+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-02T03:47:02.589231+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-02T03:47:02.618565+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-02T03:47:02.648885+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-02T03:47:02.677359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-02T03:47:02.710812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-02T03:47:02.741044+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-02T03:47:02.771838+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-02T03:47:02.801365+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-02T03:47:02.832520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-02T03:47:02.861568+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-02T03:47:02.890211+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-02T03:47:02.918315+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-02T03:47:02.944945+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-02T03:47:02.973462+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-02T03:47:03.002755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-02T03:47:03.032879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-02T03:47:03.063556+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-02T03:47:03.093941+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-02T03:47:03.125988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-02T03:47:03.156000+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-02T03:47:03.186306+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-02T03:47:03.216686+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-02T03:47:03.246358+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-02T03:47:03.275485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-02T03:47:03.303742+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-02T03:47:03.333672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-02T03:47:03.362501+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-02T03:47:03.390919+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-02T03:47:03.419043+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-02T03:47:03.447369+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-02T03:47:03.476747+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-02T03:47:03.506240+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-02T03:47:03.535528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-02T03:47:03.566360+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-02T03:47:03.596449+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-02T03:47:03.625304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-02T03:47:03.654374+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-02T03:47:03.686314+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-02T03:47:03.715340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-02T03:47:03.743033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-02T03:47:03.771720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-02T03:47:03.800953+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-02T03:47:03.830693+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-02T03:47:03.859839+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-02T03:47:03.889037+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-02T03:47:03.917599+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-02T03:47:03.946778+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-02T03:47:03.975832+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-02T03:47:04.074405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-02T03:47:04.116009+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-02T03:47:04.266959+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-02T03:47:04.409427+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-02T03:47:04.698418+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-02T03:47:04.887758+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-02T03:47:05.152588+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-02T03:47:05.397099+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-02T03:47:05.444862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-02T03:47:05.492793+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-02T03:47:05.549084+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-02T03:47:05.642632+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-02T03:47:05.671198+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-02T03:47:05.699023+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-02T03:47:05.726492+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=72 failed=0 exit=0

### [2026-03-02T04:03:00.266983+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T040300Z pid=40095 python=3.12.3

### [2026-03-02T04:03:24.158863+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T040324Z pid=41777 python=3.12.3

### [2026-03-02T04:03:24.511403+00:00] > **Agent**

 [features]  
run_features START

### [2026-03-02T04:06:40.460786+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T040640Z pid=52218 python=3.12.3

### [2026-03-02T04:06:40.836555+00:00] > **Agent**

 [features]  
run_features START

### [2026-03-02T04:08:18.865716+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T040818Z pid=59046 python=3.12.3

### [2026-03-02T04:08:20.700140+00:00] > **Agent**

 [features]  
run_features START

### [2026-03-02T04:12:25.676556+00:00] > **Agent**

 [features]  
run_features COMPLETE

### [2026-03-02T04:36:47.386851+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T043647Z pid=47742 python=3.12.3

### [2026-03-02T04:36:47.429048+00:00] *System*

 [pytest]  
pytest session started  args=['/opt/homebrew/lib/python3.12/site-packages/pytest/__main__.py', 'tests/', '-q', '--tb=short']

### [2026-03-02T04:36:48.084297+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_has_rows

### [2026-03-02T04:36:48.120157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_parse_coverage

### [2026-03-02T04:36:48.166851+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaAnnualReportsCoverage::test_fy_range_consistent

### [2026-03-02T04:36:48.199409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_has_rows

### [2026-03-02T04:36:48.233785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_parse_coverage

### [2026-03-02T04:36:48.282774+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestVisaStatisticsCoverage::test_fy_range_consistent

### [2026-03-02T04:36:48.321866+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_has_rows

### [2026-03-02T04:36:48.574299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestNivStatisticsCoverage::test_fy_span

### [2026-03-02T04:36:48.626709+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_has_rows

### [2026-03-02T04:36:48.659598+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestUscisImmigrationCoverage::test_fy_range

### [2026-03-02T04:36:48.688994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_has_rows

### [2026-03-02T04:36:48.721651+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_states_present

### [2026-03-02T04:36:48.755575+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestWarnCoverage::test_parse_coverage

### [2026-03-02T04:36:48.786773+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_trac_stub

### [2026-03-02T04:36:48.819458+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_coverage_files.py::TestStubsCoverage::test_acs_stub

### [2026-03-02T04:36:48.858026+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_issuance-country-0.5]

### [2026-03-02T04:36:48.893809+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_visa_applications-country-0.7]

### [2026-03-02T04:36:48.945716+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_ri[fact_niv_issuance-country-0.7]

### [2026-03-02T04:36:48.977264+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_dhs_admissions-country]

### [2026-03-02T04:36:49.006485+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[dim_visa_ceiling-country]

### [2026-03-02T04:36:49.037545+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCountryRI::test_country_col_exists[fact_waiting_list-country]

### [2026-03-02T04:36:49.107567+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_fact_oews_soc

### [2026-03-02T04:36:49.146337+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestSocRI::test_soc_demand_metrics_soc

### [2026-03-02T04:36:49.177771+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_issuance_category_coverage

### [2026-03-02T04:36:49.207518+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_referential_integrity.py::TestCategoryMapping::test_visa_applications_category_coverage

### [2026-03-02T04:36:49.236572+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_exists

### [2026-03-02T04:36:49.269994+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_required_columns

### [2026-03-02T04:36:49.299571+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_row_count

### [2026-03-02T04:36:49.328222+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_pk_unique

### [2026-03-02T04:36:49.361797+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_efs_ml_range

### [2026-03-02T04:36:49.393251+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestEmployerFriendlinessScoresML::test_no_all_null_rows

### [2026-03-02T04:36:49.422226+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_exists

### [2026-03-02T04:36:49.522401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_required_columns

### [2026-03-02T04:36:49.604802+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_row_count

### [2026-03-02T04:36:49.800836+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_pk_unique

### [2026-03-02T04:36:49.942047+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_case_status_values

### [2026-03-02T04:36:50.025975+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestFactPermUniqueCase::test_employer_id_coverage

### [2026-03-02T04:36:50.059055+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[employer_scores-expected_cols0]

### [2026-03-02T04:36:50.092108+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[oews_wages-expected_cols1]

### [2026-03-02T04:36:50.124017+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[pd_forecasts-expected_cols2]

### [2026-03-02T04:36:50.155469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_exists_with_schema[visa_bulletin-expected_cols3]

### [2026-03-02T04:36:50.183903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[employer_scores]

### [2026-03-02T04:36:50.211817+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[oews_wages]

### [2026-03-02T04:36:50.240566+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[pd_forecasts]

### [2026-03-02T04:36:50.269608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_remaining_artifacts.py::TestLegacyStubs::test_stub_row_count[visa_bulletin]

### [2026-03-02T04:36:50.356391+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_required_columns

### [2026-03-02T04:36:50.435599+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_min_row_count

### [2026-03-02T04:36:50.469720+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_partitioned_exists

### [2026-03-02T04:36:50.547304+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactPerm::test_case_status_not_all_null

### [2026-03-02T04:36:50.870007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_required_columns

### [2026-03-02T04:36:51.314317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactLca::test_min_row_count

### [2026-03-02T04:36:51.354312+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_required_columns

### [2026-03-02T04:36:51.412713+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_pk_unique

### [2026-03-02T04:36:51.455721+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_ref_years_present

### [2026-03-02T04:36:51.501024+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestFactOews::test_tot_emp_positive

### [2026-03-02T04:36:51.532047+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_row_count_exact

### [2026-03-02T04:36:51.561137+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_required_columns

### [2026-03-02T04:36:51.591828+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_vb_presentation_pk_unique

### [2026-03-02T04:36:51.660335+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_partition_count

### [2026-03-02T04:36:51.715837+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestVisaBulletin::test_year_span

### [2026-03-02T04:36:51.755180+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_backlog_estimates_exists

### [2026-03-02T04:36:51.788237+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_fact_cutoff_trends_exists

### [2026-03-02T04:36:51.826186+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_salary_benchmarks_exists

### [2026-03-02T04:36:51.857699+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_category_movement_metrics_exists

### [2026-03-02T04:36:51.894949+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_worksite_geo_metrics_exists

### [2026-03-02T04:36:51.925294+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_soc_demand_metrics_exists

### [2026-03-02T04:36:51.957531+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_core.py::TestCoreMetrics::test_processing_times_trends_schema

### [2026-03-02T04:36:51.986505+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_required_columns

### [2026-03-02T04:36:52.015846+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_pk_unique

### [2026-03-02T04:36:52.044885+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_has_rows

### [2026-03-02T04:36:52.074320+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestDimVisaCeiling::test_ceiling_positive

### [2026-03-02T04:36:52.105623+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_required_columns

### [2026-03-02T04:36:52.135976+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_pk_unique

### [2026-03-02T04:36:52.167218+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_has_rows

### [2026-03-02T04:36:52.197520+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWaitingList::test_count_waiting_non_negative

### [2026-03-02T04:36:52.227383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_required_columns

### [2026-03-02T04:36:52.259752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_pk_unique

### [2026-03-02T04:36:52.290654+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_row_count

### [2026-03-02T04:36:52.321660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_issued_non_negative

### [2026-03-02T04:36:52.354041+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaIssuance::test_fiscal_year_format

### [2026-03-02T04:36:52.384845+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_required_columns

### [2026-03-02T04:36:52.418314+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_pk_unique

### [2026-03-02T04:36:52.447447+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_row_count

### [2026-03-02T04:36:52.477020+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactVisaApplications::test_applications_non_negative

### [2026-03-02T04:36:52.513215+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_required_columns

### [2026-03-02T04:36:52.581638+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_pk_unique

### [2026-03-02T04:36:52.617338+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_row_count

### [2026-03-02T04:36:52.651924+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_issued_non_negative

### [2026-03-02T04:36:52.701228+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_fy_format

### [2026-03-02T04:36:52.744312+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactNivIssuance::test_has_standard_visa_classes

### [2026-03-02T04:36:52.782417+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_required_columns

### [2026-03-02T04:36:52.812182+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_pk_unique

### [2026-03-02T04:36:52.842436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_has_rows

### [2026-03-02T04:36:52.872409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactUscisApprovals::test_approvals_non_negative

### [2026-03-02T04:36:52.901176+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_required_columns

### [2026-03-02T04:36:52.930230+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_pk_unique

### [2026-03-02T04:36:52.960157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_has_rows

### [2026-03-02T04:36:52.991011+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactDhsAdmissions::test_admissions_non_negative

### [2026-03-02T04:36:53.020165+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_required_columns

### [2026-03-02T04:36:53.048934+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_pk_unique

### [2026-03-02T04:36:53.077767+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_rows

### [2026-03-02T04:36:53.107114+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_has_multiple_states

### [2026-03-02T04:36:53.136639+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestFactWarnEvents::test_employees_affected_non_negative

### [2026-03-02T04:36:53.164525+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_trac_adjudications_stub

### [2026-03-02T04:36:53.197113+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_schema_and_pk_new.py::TestStubTables::test_fact_acs_wages_stub

### [2026-03-02T04:36:53.230440+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[dim_visa_ceiling-ceiling]

### [2026-03-02T04:36:53.262376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_issuance-issued]

### [2026-03-02T04:36:53.294928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_visa_applications-applications]

### [2026-03-02T04:36:53.332904+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_niv_issuance-issued]

### [2026-03-02T04:36:53.364021+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_uscis_approvals-approvals]

### [2026-03-02T04:36:53.394580+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_dhs_admissions-admissions]

### [2026-03-02T04:36:53.425438+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_no_negative_counts[fact_warn_events-employees_affected]

### [2026-03-02T04:36:53.456247+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestNonNegative::test_dim_visa_ceiling_positive

### [2026-03-02T04:36:53.486258+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_months_to_clear_range

### [2026-03-02T04:36:53.515436+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_no_nan_only_columns

### [2026-03-02T04:36:53.543898+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestBacklogEstimates::test_category_range

### [2026-03-02T04:36:53.819918+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_niv_fy_span

### [2026-03-02T04:36:53.863114+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_issuance_fy_span

### [2026-03-02T04:36:53.910537+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_visa_applications_fy_span

### [2026-03-02T04:36:53.940343+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_uscis_approvals_fy_span

### [2026-03-02T04:36:53.969576+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_dhs_admissions_fy_span

### [2026-03-02T04:36:54.001150+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestTimeSpans::test_warn_events_date_sane

### [2026-03-02T04:36:54.038300+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/datasets/test_value_ranges_and_continuity.py::TestSalaryBenchmarks::test_percentile_ordering

### [2026-03-02T04:36:54.093359+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestVisaDemandCountryRI::test_country_ri

### [2026-03-02T04:36:54.123715+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_no_nan_only_columns

### [2026-03-02T04:36:54.153403+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_months_quantiles_sane

### [2026-03-02T04:36:54.182839+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestBacklogEstimatesSanity::test_category_country_presence

### [2026-03-02T04:36:54.213690+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_verify_log_pass

### [2026-03-02T04:36:54.241090+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_correlation_threshold

### [2026-03-02T04:36:54.279049+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestEfsAcceptance::test_efs_parquet_valid

### [2026-03-02T04:36:54.370629+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_perm_all_unchanged

### [2026-03-02T04:36:54.412082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_oews_unchanged

### [2026-03-02T04:36:54.442813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_cutoffs_unchanged

### [2026-03-02T04:36:54.475433+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_fact_lca_present

### [2026-03-02T04:36:54.515191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_integration_e2e_sanity.py::TestCoreTablesNoRegression::test_employer_features_intact

### [2026-03-02T04:36:54.547037+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_task_present

### [2026-03-02T04:36:54.577133+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_references_dim_visa_ceiling_or_waiting_list

### [2026-03-02T04:36:54.606661+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestBacklogUsage::test_backlog_row_count_positive

### [2026-03-02T04:36:54.635207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_task_present

### [2026-03-02T04:36:54.664954+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_used_at_least_two_sources

### [2026-03-02T04:36:54.699906+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_output_parquet_exists

### [2026-03-02T04:36:54.727892+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestVisaDemandMetrics::test_references_niv_or_visa_issuance

### [2026-03-02T04:36:54.757645+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_task_present

### [2026-03-02T04:36:54.785214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_output_exists

### [2026-03-02T04:36:54.814311+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestEmployerRiskFeatures::test_join_rate_positive

### [2026-03-02T04:36:54.843701+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_trac_stubbed

### [2026-03-02T04:36:54.874271+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/models/test_model_usage_matrix.py::TestStubsInRegistry::test_acs_stubbed

### [2026-03-02T04:36:54.922581+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_fy_coverage

### [2026-03-02T04:36:54.953794+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_admissions_span

### [2026-03-02T04:36:54.984096+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_uscis_approvals_fy_coverage

### [2026-03-02T04:36:55.014322+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_warn_has_multiple_states

### [2026-03-02T04:36:55.043754+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_all_p2_parquets_exist

### [2026-03-02T04:36:55.086385+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_optional_pdf_parquets_if_exist

### [2026-03-02T04:36:55.129551+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_niv_has_standard_visa_classes

### [2026-03-02T04:36:55.159132+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dim_visa_ceiling_has_employment_category

### [2026-03-02T04:36:55.187996+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_coverage_gap_tables.py::test_dhs_refugee_class

### [2026-03-02T04:36:55.217464+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[dim_visa_ceiling]

### [2026-03-02T04:36:55.249122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_waiting_list]

### [2026-03-02T04:36:55.291596+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_niv_issuance]

### [2026-03-02T04:36:55.321083+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_uscis_approvals]

### [2026-03-02T04:36:55.351092+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_dhs_admissions]

### [2026-03-02T04:36:55.382753+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_non_stub_has_rows[fact_warn_events]

### [2026-03-02T04:36:55.426195+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_row_count

### [2026-03-02T04:36:55.455714+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_row_count

### [2026-03-02T04:36:55.484903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_row_count

### [2026-03-02T04:36:55.516328+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_dim_visa_ceiling_plausible

### [2026-03-02T04:36:55.552721+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_niv_issuance_non_negative

### [2026-03-02T04:36:55.584973+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_uscis_approvals_non_negative

### [2026-03-02T04:36:55.622312+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_non_negative

### [2026-03-02T04:36:55.653280+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_warn_events_non_negative

### [2026-03-02T04:36:55.684500+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[dim_visa_ceiling-fiscal_year]

### [2026-03-02T04:36:55.740209+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_niv_issuance-fiscal_year]

### [2026-03-02T04:36:55.769863+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_uscis_approvals-fiscal_year]

### [2026-03-02T04:36:55.799134+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fiscal_year_format[fact_dhs_admissions-fiscal_year]

### [2026-03-02T04:36:55.828515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_fact_dhs_admissions_fy_range

### [2026-03-02T04:36:55.858990+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_issuance-issued]

### [2026-03-02T04:36:55.889940+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_ranges_gap_tables.py::test_optional_non_negative[fact_visa_applications-applications]

### [2026-03-02T04:36:55.919750+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-02T04:36:55.949221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-02T04:36:55.985516+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-02T04:36:56.015556+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-02T04:36:56.045005+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-02T04:36:56.074064+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-02T04:36:56.103071+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-02T04:36:56.134951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_schema_columns[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-02T04:36:56.167299+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-02T04:36:56.197401+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-02T04:36:56.258502+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-02T04:36:56.287960+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-02T04:36:56.317078+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-02T04:36:56.346214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-02T04:36:56.375541+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-02T04:36:56.407443+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_pk_uniqueness[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-02T04:36:56.439011+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[dim_visa_ceiling-required_cols0-pk_cols0]

### [2026-03-02T04:36:56.470050+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_waiting_list-required_cols1-pk_cols1]

### [2026-03-02T04:36:56.512773+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_niv_issuance-required_cols2-pk_cols2]

### [2026-03-02T04:36:56.541250+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_uscis_approvals-required_cols3-pk_cols3]

### [2026-03-02T04:36:56.570236+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_dhs_admissions-required_cols4-pk_cols4]

### [2026-03-02T04:36:56.600487+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_warn_events-required_cols5-pk_cols5]

### [2026-03-02T04:36:56.632830+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_trac_adjudications-required_cols6-pk_cols6]

### [2026-03-02T04:36:56.662718+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_no_all_null_rows[fact_acs_wages-required_cols7-pk_cols7]

### [2026-03-02T04:36:56.699750+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-02T04:36:56.731568+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_schema_columns[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-02T04:36:56.764101+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_issuance-required_cols0-pk_cols0]

### [2026-03-02T04:36:56.797085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_gap_curation/test_schema_pk_gap_tables.py::test_optional_pk_uniqueness[fact_visa_applications-required_cols1-pk_cols1]

### [2026-03-02T04:36:56.837469+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_approval_rate_in_01

### [2026-03-02T04:36:56.874999+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_monthly_denial_rate_in_01

### [2026-03-02T04:36:56.911348+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_employer_features_approval_rate_in_01

### [2026-03-02T04:36:56.941579+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestRateBounds::test_soc_demand_approval_rate_in_01

### [2026-03-02T04:36:56.976951+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_no_approvals_exceed_filings

### [2026-03-02T04:36:57.014060+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_approvals_nonnegative

### [2026-03-02T04:36:57.050432+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestApprovalsFilings::test_employer_monthly_filings_positive

### [2026-03-02T04:36:57.085529+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_in_0_to_100

### [2026-03-02T04:36:57.120214+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestEFSBounds::test_efs_null_requires_insufficient_flag

### [2026-03-02T04:36:57.162870+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_salary_benchmarks_soc_coverage

### [2026-03-02T04:36:57.204224+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_worksite_geo_soc_coverage

### [2026-03-02T04:36:57.975162+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestReferentialIntegrity::test_employer_features_employer_id_coverage

### [2026-03-02T04:36:58.006387+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_advancement_days_median_in_band

### [2026-03-02T04:36:58.040424+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_competitiveness_ratio_mostly_positive

### [2026-03-02T04:36:58.072836+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_salary_benchmarks_median_reasonable

### [2026-03-02T04:36:58.102016+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_backlog_estimates_nonnegative

### [2026-03-02T04:36:58.133581+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_ranges_and_integrity.py::TestStatisticalSmoke::test_velocity_3m_present_and_numeric

### [2026-03-02T04:36:58.164672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoffs_all_schema

### [2026-03-02T04:36:58.195044+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_fact_cutoff_trends_schema

### [2026-03-02T04:36:58.232111+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_monthly_metrics_schema

### [2026-03-02T04:36:58.265536+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_salary_benchmarks_schema

### [2026-03-02T04:36:58.298376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_worksite_geo_metrics_schema

### [2026-03-02T04:36:58.333465+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_employer_friendliness_scores_schema

### [2026-03-02T04:36:58.363816+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_soc_demand_metrics_schema

### [2026-03-02T04:36:58.392762+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_backlog_estimates_schema

### [2026-03-02T04:36:58.422147+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_processing_times_trends_exists

### [2026-03-02T04:36:58.451024+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestSchema::test_dim_tables_present

### [2026-03-02T04:36:58.486646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoffs_all_pk_unique

### [2026-03-02T04:36:58.519858+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_fact_cutoff_trends_pk_unique

### [2026-03-02T04:36:58.553479+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_category_movement_metrics_pk_unique

### [2026-03-02T04:36:58.585687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_soc_pk_unique

### [2026-03-02T04:36:58.619718+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_area_pk_unique

### [2026-03-02T04:36:58.651540+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPKUniqueness::test_dim_country_pk_unique

### [2026-03-02T04:36:58.697031+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_monotonic_zero_violations

### [2026-03-02T04:36:58.731102+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestMonotonicPercentiles::test_salary_benchmarks_percentile_count

### [2026-03-02T04:36:58.758775+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoffs_all_rows

### [2026-03-02T04:36:58.786672+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_fact_cutoff_trends_rows

### [2026-03-02T04:36:58.816096+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_category_movement_metrics_rows

### [2026-03-02T04:36:58.844207+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_backlog_estimates_rows

### [2026-03-02T04:36:58.872862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_monthly_metrics_rows

### [2026-03-02T04:36:58.901970+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_employer_rows

### [2026-03-02T04:36:58.932166+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_soc_rows

### [2026-03-02T04:36:58.961695+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_area_rows

### [2026-03-02T04:36:58.991014+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_dim_country_rows

### [2026-03-02T04:36:59.019997+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_salary_benchmarks_rows

### [2026-03-02T04:36:59.050618+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_features_rows

### [2026-03-02T04:36:59.079577+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestGoldenRowCounts::test_employer_friendliness_scores_rows

### [2026-03-02T04:36:59.109980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_perm_has_partitions

### [2026-03-02T04:36:59.139738+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_lca_has_partitions

### [2026-03-02T04:36:59.193625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p2_hardening/test_schema_and_pk.py::TestPartitions::test_fact_cutoffs_leaf_count

### [2026-03-02T04:36:59.227095+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_retrogression_detected

### [2026-03-02T04:36:59.257409+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_backward_prediction_on_negative_velocity

### [2026-03-02T04:36:59.285647+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_forward_prediction_on_positive_velocity_no_retro

### [2026-03-02T04:36:59.312660+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_flat_prediction_on_zero_velocity

### [2026-03-02T04:36:59.339319+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_unknown_when_velocity_null

### [2026-03-02T04:36:59.374674+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_12m_rolling_volatility_non_negative

### [2026-03-02T04:36:59.408882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_category_movement_metrics.py::test_output_min_rows

### [2026-03-02T04:36:59.447172+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approval_rate_bounded

### [2026-03-02T04:36:59.482497+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_denial_rate_bounded

### [2026-03-02T04:36:59.517931+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_each_row_is_employer_month

### [2026-03-02T04:36:59.551819+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_audit_rate_t12_bounded

### [2026-03-02T04:36:59.590532+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_5year_filter_removes_old

### [2026-03-02T04:36:59.626260+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_zero_months_approvals_exceed_filings

### [2026-03-02T04:36:59.659384+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_approved_includes_approved_status

### [2026-03-02T04:36:59.698122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_36m_warn_is_warn_not_fail

### [2026-03-02T04:36:59.732281+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_filings_equals_approvals_plus_denials_plus_other

### [2026-03-02T04:36:59.772340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_employer_monthly_metrics.py::test_actual_parquet_no_approvals_exceed_filings

### [2026-03-02T04:36:59.807554+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_queue_position_days_only_for_date_final

### [2026-03-02T04:36:59.845239+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_monthly_advancement_days_diff

### [2026-03-02T04:36:59.881602+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_flag_negative_advancement

### [2026-03-02T04:36:59.913831+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_velocity_3m_requires_min_periods_3

### [2026-03-02T04:36:59.946764+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_fact_cutoff_trends.py::test_retrogression_count_cum_accumulates

### [2026-03-02T04:36:59.980644+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_zero_violations_after_enforce

### [2026-03-02T04:37:00.013174+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_corrects_scrambled_row

### [2026-03-02T04:37:00.043029+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_leaves_correct_row_unchanged

### [2026-03-02T04:37:00.073697+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_enforce_skips_null_rows

### [2026-03-02T04:37:00.120533+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_national_rows_have_null_area_code

### [2026-03-02T04:37:00.154964+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_hourly_annualized_when_annual_missing

### [2026-03-02T04:37:00.189415+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_no_duplicate_soc_area_pairs

### [2026-03-02T04:37:00.228204+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_salary_benchmarks.py::test_actual_parquet_zero_violations

### [2026-03-02T04:37:00.258235+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_12m_window_excludes_older_rows

### [2026-03-02T04:37:00.290691+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_approval_rate_bounded

### [2026-03-02T04:37:00.322030+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_competitiveness_percentile_bounded

### [2026-03-02T04:37:00.363426+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_all_three_windows_differ

### [2026-03-02T04:37:00.394605+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_soc_demand_metrics.py::test_top_employers_json_format

### [2026-03-02T04:37:00.422755+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_hourly_wage_annualized

### [2026-03-02T04:37:00.451234+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_biweekly_annualized

### [2026-03-02T04:37:00.479826+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_monthly_annualized

### [2026-03-02T04:37:00.509325+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_year_passthrough

### [2026-03-02T04:37:00.544616+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_state_grain_counts

### [2026-03-02T04:37:00.574317+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_calculation

### [2026-03-02T04:37:00.604172+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_competitiveness_ratio_null_on_zero_oews

### [2026-03-02T04:37:00.637887+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/p3_metrics/test_worksite_geo_metrics.py::test_soc_area_grain_grouping

### [2026-03-02T04:37:02.760687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_thresholds

### [2026-03-02T04:37:03.196993+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_coverage_report_structure

### [2026-03-02T04:37:03.676394+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_coverage_expectations.py::test_no_stale_files

### [2026-03-02T04:37:03.707584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_all_eb_categories_have_forecasts

### [2026-03-02T04:37:03.735398+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_india_and_china_have_forecasts

### [2026-03-02T04:37:03.764905+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_eb2_india_is_most_backlogged

### [2026-03-02T04:37:03.792699+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecasts_have_56_series

### [2026-03-02T04:37:03.821508+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_forecast_months_are_24

### [2026-03-02T04:37:03.850656+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPDForecastSanity::test_projected_dates_are_in_future

### [2026-03-02T04:37:03.888581+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_rules_covers_many_employers

### [2026-03-02T04:37:03.920528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_ml_covers_high_volume

### [2026-03-02T04:37:03.949221+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_score_range

### [2026-03-02T04:37:03.979927+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_efs_has_tier_distribution

### [2026-03-02T04:37:04.009737+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_no_approval_rate_above_100pct

### [2026-03-02T04:37:04.039415+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestEFSSanity::test_mean_efs_is_moderate

### [2026-03-02T04:37:04.076785+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_median_salary_in_reasonable_range

### [2026-03-02T04:37:04.112439+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_percentile_ordering

### [2026-03-02T04:37:04.141411+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_no_negative_salaries

### [2026-03-02T04:37:04.170659+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSalarySanity::test_salary_coverage

### [2026-03-02T04:37:04.217142+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_california_is_top_filing_state

### [2026-03-02T04:37:04.252586+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_top_states_include_tech_hubs

### [2026-03-02T04:37:04.284242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestGeographicSanity::test_geo_has_multiple_states

### [2026-03-02T04:37:04.382412+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_over_1m_records

### [2026-03-02T04:37:04.465277+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_has_certified_cases

### [2026-03-02T04:37:04.856080+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_perm_spans_many_fiscal_years

### [2026-03-02T04:37:04.903752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_no_impossible_approval_dates

### [2026-03-02T04:37:04.941182+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestPERMSanity::test_year_over_year_volume_stable

### [2026-03-02T04:37:04.972980+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_all_eb_categories

### [2026-03-02T04:37:05.001784+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_cover_india_and_china

### [2026-03-02T04:37:05.029187+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestVisaBulletinSanity::test_cutoffs_span_many_years

### [2026-03-02T04:37:05.072898+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_employer_has_many_employers

### [2026-03-02T04:37:05.104194+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_soc_has_standard_codes

### [2026-03-02T04:37:05.135089+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_country_covers_world

### [2026-03-02T04:37:05.166528+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestDimensionSanity::test_dim_visa_class_has_eb_categories

### [2026-03-02T04:37:05.199191+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_processing_times_exist

### [2026-03-02T04:37:05.228329+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_backlog_months_reasonable

### [2026-03-02T04:37:05.256852+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestProcessingTimesSanity::test_approval_rate_reasonable

### [2026-03-02T04:37:05.289349+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_has_software_developer_soc

### [2026-03-02T04:37:05.318355+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_soc_demand_has_reasonable_count

### [2026-03-02T04:37:05.347206+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestSOCDemandSanity::test_no_negative_filings

### [2026-03-02T04:37:05.562627+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_efs_employers_exist_in_dim_employer

### [2026-03-02T04:37:05.605071+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_employer_features_match_efs_count

### [2026-03-02T04:37:05.637478+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_pd_forecasts_categories_match_cutoffs

### [2026-03-02T04:37:05.675928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_salary_soc_codes_in_dim_soc

### [2026-03-02T04:37:05.713808+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_geo_states_are_valid_us_states

### [2026-03-02T04:37:05.745119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_soc_demand_approval_rate_bounded

### [2026-03-02T04:37:05.782157+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestCrossArtifactConsistency::test_perm_employer_volume_matches_features

### [2026-03-02T04:37:05.812281+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_rag_catalog_exists

### [2026-03-02T04:37:05.843068+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_qa_cache_has_enough_pairs

### [2026-03-02T04:37:05.876004+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_data_sanity.py::TestRAGSanity::test_all_chunks_exist

### [2026-03-02T04:37:42.907036+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_builder_creates_file

### [2026-03-02T04:37:42.937687+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_schema

### [2026-03-02T04:37:42.975344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_classifications

### [2026-03-02T04:37:43.007968+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_area.py::test_dim_area_state_mappings

### [2026-03-02T04:37:43.038092+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_country.py::test_dim_country_schema

### [2026-03-02T04:37:43.067293+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_exists

### [2026-03-02T04:37:43.108297+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_min_rows

### [2026-03-02T04:37:43.164406+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_id_unique

### [2026-03-02T04:37:43.206712+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_name_non_null

### [2026-03-02T04:37:43.248735+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_suffix_removal

### [2026-03-02T04:37:43.292792+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_employer.py::test_dim_employer_title_case

### [2026-03-02T04:37:43.328399+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_schema

### [2026-03-02T04:37:43.360393+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_crosswalk_coverage

### [2026-03-02T04:37:43.393183+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_soc.py::test_dim_soc_hierarchy_extraction

### [2026-03-02T04:37:43.435038+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_builder_creates_file

### [2026-03-02T04:37:43.465340+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_schema

### [2026-03-02T04:37:43.497099+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_family_codes

### [2026-03-02T04:37:43.529796+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dim_visa_class.py::test_dim_visa_class_subcategories

### [2026-03-02T04:37:44.592159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_no_writes

### [2026-03-02T04:39:00.516404+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_dry_run.py::test_dry_run_discovers_files

### [2026-03-02T04:39:00.577269+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_google_canonical

### [2026-03-02T04:39:00.619320+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_dim_employer_no_all_caps_names

### [2026-03-02T04:39:00.770351+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_no_raw_variants

### [2026-03-02T04:39:00.916389+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_google_canonical_present

### [2026-03-02T04:39:01.224765+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_no_all_caps_top_employers

### [2026-03-02T04:39:01.400086+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_microsoft_no_raw_variants

### [2026-03-02T04:39:01.640910+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_no_raw_variants

### [2026-03-02T04:39:01.879800+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_google_canonical_present

### [2026-03-02T04:39:01.929395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_no_raw_variants

### [2026-03-02T04:39:01.979073+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_monthly_metrics_google_canonical_present

### [2026-03-02T04:39:02.037571+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_yearly_has_employer_id

### [2026-03-02T04:39:02.136943+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_employer_salary_profiles_has_employer_id

### [2026-03-02T04:39:02.165668+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_importable

### [2026-03-02T04:39:02.193293+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_employer_name_normalization.py::test_normalize_module_google_dedup

### [2026-03-02T04:39:02.508481+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_cutoffs.py::test_fact_cutoffs_has_data

### [2026-03-02T04:39:02.538173+00:00] > **Agent**

 [pytest]  
TEST SKIP: tests/test_fact_cutoffs.py::test_fact_cutoffs_partitioning

### [2026-03-02T04:39:02.566564+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_exists

### [2026-03-02T04:39:02.611371+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_min_rows

### [2026-03-02T04:39:02.669567+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_primary_key_unique

### [2026-03-02T04:39:02.713341+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_pk_non_null

### [2026-03-02T04:39:02.759733+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_fields_present

### [2026-03-02T04:39:02.803929+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_wage_values_reasonable

### [2026-03-02T04:39:02.846811+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_ref_year

### [2026-03-02T04:39:02.893566+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_soc_codes_detailed

### [2026-03-02T04:39:02.937824+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_oews.py::test_fact_oews_employment

### [2026-03-02T04:39:02.966559+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_exists

### [2026-03-02T04:39:03.064786+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_min_rows

### [2026-03-02T04:39:03.261745+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_unique

### [2026-03-02T04:39:03.333549+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_number_non_null

### [2026-03-02T04:39:03.415526+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_foreign_keys_present

### [2026-03-02T04:39:03.491864+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_dates_parsed

### [2026-03-02T04:39:03.565834+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_fy_derivation

### [2026-03-02T04:39:03.663903+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_fact_perm.py::test_fact_perm_case_status_values

### [2026-03-02T04:39:04.277085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNoRegressions::test_no_artifacts_lost

### [2026-03-02T04:39:04.884749+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_no_row_count_regression

### [2026-03-02T04:39:05.464166+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestRowCounts::test_row_counts_increased_only_warns

### [2026-03-02T04:39:06.114392+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_no_columns_dropped

### [2026-03-02T04:39:06.703289+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_dtype_signature_stable

### [2026-03-02T04:39:07.342856+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestSchemaStability::test_new_columns_only_warns

### [2026-03-02T04:39:07.818882+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_golden_snapshot.py::TestNumericRanges::test_no_extreme_range_shift

### [2026-03-02T04:39:07.904562+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_required_columns

### [2026-03-02T04:39:08.028118+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_pk_unique

### [2026-03-02T04:39:08.057818+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_row_count_minimum

### [2026-03-02T04:39:08.089916+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_fiscal_year_range

### [2026-03-02T04:39:08.119901+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_approval_rate_bounded

### [2026-03-02T04:39:08.148645+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_petitions_non_negative

### [2026-03-02T04:39:08.177422+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_petition_columns_non_negative

### [2026-03-02T04:39:08.206815+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_total_equals_sum

### [2026-03-02T04:39:08.236635+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_stale_markers

### [2026-03-02T04:39:08.271587+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_has_standard_states

### [2026-03-02T04:39:08.338504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactH1BEmployerHub::test_employer_count

### [2026-03-02T04:39:08.368790+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_exists

### [2026-03-02T04:39:08.400488+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_stub_schema

### [2026-03-02T04:39:08.431021+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactProcessingTimes::test_row_count_zero_expected

### [2026-03-02T04:39:08.462427+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_required_columns

### [2026-03-02T04:39:08.490870+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_pk_unique

### [2026-03-02T04:39:08.519349+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_has_rows

### [2026-03-02T04:39:08.550771+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_value_positive

### [2026-03-02T04:39:08.582003+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_series_ids_present

### [2026-03-02T04:39:08.611313+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_year_reasonable

### [2026-03-02T04:39:08.639859+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_new_p1_tables.py::TestFactBLSCES::test_period_format

### [2026-03-02T04:39:08.671065+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_lowercase_same_as_uppercase

### [2026-03-02T04:39:08.701593+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_punctuation_trailing_comma

### [2026-03-02T04:39:08.733916+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_mixed_case_with_period

### [2026-03-02T04:39:08.763308+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llc_variant

### [2026-03-02T04:39:08.792751+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corporation_suffix

### [2026-03-02T04:39:08.822625+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_corp_suffix

### [2026-03-02T04:39:08.851564+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_ltd_suffix

### [2026-03-02T04:39:08.882580+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_llp_suffix

### [2026-03-02T04:39:08.911372+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_whitespace_collapse

### [2026-03-02T04:39:08.940962+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_none_returns_empty

### [2026-03-02T04:39:08.972375+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_empty_string_returns_empty

### [2026-03-02T04:39:09.003590+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_result_is_lowercase

### [2026-03-02T04:39:09.034383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_no_trailing_whitespace

### [2026-03-02T04:39:09.066879+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_semicolon_removed

### [2026-03-02T04:39:09.094914+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_parentheses_removed

### [2026-03-02T04:39:09.123997+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_tcs_deduplication

### [2026-03-02T04:39:09.156703+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_google_deduplication

### [2026-03-02T04:39:09.186175+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_microsoft_deduplication

### [2026-03-02T04:39:09.215419+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_distinct_employers_differ

### [2026-03-02T04:39:09.244812+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeEmployerName::test_amazon_web_services_distinct_from_amazon

### [2026-03-02T04:39:09.275105+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_single_word

### [2026-03-02T04:39:09.303661+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_multi_word

### [2026-03-02T04:39:09.331655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_empty_returns_empty

### [2026-03-02T04:39:09.359434+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestTitleCaseEmployerName::test_already_formatted

### [2026-03-02T04:39:09.387012+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_standard_format_unchanged

### [2026-03-02T04:39:09.414183+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_decimal_stripped

### [2026-03-02T04:39:09.442569+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_6_digits

### [2026-03-02T04:39:09.471928+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_no_dash_8_digits_with_decimal_part

### [2026-03-02T04:39:09.503969+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_whitespace_stripped

### [2026-03-02T04:39:09.536167+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_none_returns_none

### [2026-03-02T04:39:09.567385+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_empty_returns_none

### [2026-03-02T04:39:09.604657+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_too_short_returns_none

### [2026-03-02T04:39:09.632829+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_soc_11_with_decimal

### [2026-03-02T04:39:09.663617+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeSocCode::test_different_soc_groups

### [2026-03-02T04:39:09.694515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_exact

### [2026-03-02T04:39:09.725276+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_uppercase

### [2026-03-02T04:39:09.754643+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_india_mixed_case

### [2026-03-02T04:39:09.784728+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_mainland_variant

### [2026-03-02T04:39:09.815229+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_china_plain

### [2026-03-02T04:39:09.844407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_mexico

### [2026-03-02T04:39:09.873393+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_philippines

### [2026-03-02T04:39:09.903848+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_chargeability

### [2026-03-02T04:39:09.935557+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_row_direct

### [2026-03-02T04:39:09.966173+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso3_passthrough

### [2026-03-02T04:39:09.995093+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_india

### [2026-03-02T04:39:10.023690+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_iso2_china

### [2026-03-02T04:39:10.053536+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_unknown_returns_none

### [2026-03-02T04:39:10.083727+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_none_returns_none

### [2026-03-02T04:39:10.112119+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeCountryCode::test_empty_returns_none

### [2026-03-02T04:39:10.140285+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_hyphen

### [2026-03-02T04:39:10.170471+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_no_hyphen

### [2026-03-02T04:39:10.203237+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb3_uppercase

### [2026-03-02T04:39:10.233112+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb2_niw

### [2026-03-02T04:39:10.262213+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_h1b_variants

### [2026-03-02T04:39:10.292425+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_e3_variant

### [2026-03-02T04:39:10.322813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_eb1

### [2026-03-02T04:39:10.353407+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_unknown_returns_none

### [2026-03-02T04:39:10.383571+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_normalization_mappings.py::TestNormalizeVisaCategory::test_none_returns_none

### [2026-03-02T04:39:10.479880+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_runs_successfully

### [2026-03-02T04:39:10.549491+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_paths_check.py::test_check_paths_validates_data_root

### [2026-03-02T04:39:10.580439+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_file_exists

### [2026-03-02T04:39:10.613515+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_required_columns

### [2026-03-02T04:39:10.643584+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestSchema::test_row_count_minimum

### [2026-03-02T04:39:10.674254+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_categories

### [2026-03-02T04:39:10.704504+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestDimensions::test_countries

### [2026-03-02T04:39:10.735938+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestPrimaryKey::test_pk_unique

### [2026-03-02T04:39:10.766197+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_perm_filings_non_negative

### [2026-03-02T04:39:10.798878+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_eb_category_ratio_bounds

### [2026-03-02T04:39:10.832159+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_category_filings_non_negative

### [2026-03-02T04:39:10.865651+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_applicants_non_negative

### [2026-03-02T04:39:10.900344+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_annual_allocation_positive

### [2026-03-02T04:39:10.933644+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_cumulative_ahead_non_negative

### [2026-03-02T04:39:10.966813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_est_wait_years_non_negative

### [2026-03-02T04:39:10.999953+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestValueRanges::test_confidence_values

### [2026-03-02T04:39:11.036095+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_has_queue

### [2026-03-02T04:39:11.069405+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_wait_substantial

### [2026-03-02T04:39:11.103126+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_eb2_india_cutoff_date_present

### [2026-03-02T04:39:11.141242+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_cumulative_monotonic_for_ahead_rows

### [2026-03-02T04:39:11.172613+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_est_filings_le_raw_filings

### [2026-03-02T04:39:11.206862+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_queue_depth_estimates.py::TestBusinessLogic::test_per_country_allocation_for_oversubscribed

### [2026-03-02T04:39:11.237032+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_exists

### [2026-03-02T04:39:11.267813+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_program_info

### [2026-03-02T04:39:11.300142+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topics

### [2026-03-02T04:39:11.330571+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_topic_descriptions

### [2026-03-02T04:39:11.359312+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_has_artifacts

### [2026-03-02T04:39:11.389020+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestCatalog::test_catalog_artifact_format

### [2026-03-02T04:39:11.418370+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_exist

### [2026-03-02T04:39:11.450267+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunks_not_empty

### [2026-03-02T04:39:11.480393+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_schema

### [2026-03-02T04:39:11.509383+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_ids_unique

### [2026-03-02T04:39:11.538061+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_chunk_text_not_empty

### [2026-03-02T04:39:11.565988+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_coverage

### [2026-03-02T04:39:11.592953+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_pd_forecast_chunks_present

### [2026-03-02T04:39:11.621936+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_employer_chunks_present

### [2026-03-02T04:39:11.653327+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestChunks::test_topic_files_exist

### [2026-03-02T04:39:11.686229+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_cache_exists

### [2026-03-02T04:39:11.720007+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_pairs_not_empty

### [2026-03-02T04:39:11.747395+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_schema

### [2026-03-02T04:39:11.776519+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_questions_unique

### [2026-03-02T04:39:11.806987+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_answers_not_empty

### [2026-03-02T04:39:11.837188+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_topic_coverage

### [2026-03-02T04:39:11.867200+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_methodology_questions

### [2026-03-02T04:39:11.897608+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_has_employer_lookups

### [2026-03-02T04:39:11.927059+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestQACache::test_qa_sources_are_lists

### [2026-03-02T04:39:11.956272+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_exists

### [2026-03-02T04:39:11.986743+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_artifacts.py::TestBuildSummary::test_build_summary_format

### [2026-03-02T04:39:12.031752+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_employer_count_in_efs_answer

### [2026-03-02T04:39:12.065655+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_salary_count_in_salary_answer

### [2026-03-02T04:39:12.101957+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_forecast_count_in_chunk

### [2026-03-02T04:39:12.138741+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_geo_record_count_in_chunk

### [2026-03-02T04:39:12.170989+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_cutoffs_count_in_visa_bulletin_chunk

### [2026-03-02T04:39:12.204057+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_processing_count_in_chunk

### [2026-03-02T04:39:12.283082+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestAnswerFidelity::test_top_employers_are_real

### [2026-03-02T04:39:12.311373+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_chunks

### [2026-03-02T04:39:12.340665+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_every_topic_has_qa_pairs

### [2026-03-02T04:39:12.369947+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_no_topic_exceeds_70_pct_of_chunks

### [2026-03-02T04:39:12.397780+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTopicBalance::test_thin_topics_enriched

### [2026-03-02T04:39:12.428243+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_source_artifacts_exist

### [2026-03-02T04:39:12.456692+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestSourceTraceability::test_chunk_labels_are_descriptive

### [2026-03-02T04:39:12.485843+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_chunk_exceeds_max

### [2026-03-02T04:39:12.516033+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_no_trivially_small_chunks

### [2026-03-02T04:39:12.545166+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestTokenBudget::test_qa_answers_reasonable_length

### [2026-03-02T04:39:12.573961+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_qa_cache_not_older_than_key_artifacts

### [2026-03-02T04:39:12.602732+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestFreshness::test_chunks_not_older_than_key_artifacts

### [2026-03-02T04:39:12.646646+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_keyword_retrieval_hits_correct_topic

### [2026-03-02T04:39:12.688134+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestRetrievalSimulation::test_every_topic_retrievable

### [2026-03-02T04:39:12.716637+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_qa_topic_has_chunks

### [2026-03-02T04:39:12.743458+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_every_chunk_topic_has_qas

### [2026-03-02T04:39:12.773161+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_qa_sources_reference_valid_artifacts

### [2026-03-02T04:39:12.801122+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_rag_quality.py::TestQAChunkAlignment::test_topic_counts_summary

### [2026-03-02T04:39:12.829376+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_paths_yaml_exists

### [2026-03-02T04:39:12.860790+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_data_root_exists

### [2026-03-02T04:39:12.964022+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_import

### [2026-03-02T04:44:36.971085+00:00] > **Agent**

 [pytest]  
TEST PASS: tests/test_smoke.py::test_entrypoints_run_noop

### [2026-03-02T04:44:37.010776+00:00] > **Agent**

 [pytest]  
pytest FINISHED: collected=563 failed=0 exit=0

### [2026-03-02T14:44:56.082753+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T144456Z pid=8269 python=3.12.3

### [2026-03-02T14:44:56.082790+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T144456Z pid=8268 python=3.12.3

### [2026-03-02T14:44:56.117787+00:00] *System*

 [pytest]  
pytest session started  args=['/Users/vrathod1/.vscode/extensions/ms-python.vscode-pylance-2026.1.1/dist/bundled/files/get_pytest_options.py']

### [2026-03-02T14:44:56.117788+00:00] *System*

 [pytest]  
pytest session started  args=['/Users/vrathod1/.vscode/extensions/ms-python.vscode-pylance-2026.1.1/dist/bundled/files/get_pytest_options.py']

### [2026-03-02T14:44:56.336936+00:00] *System*

 [bootstrap]  
SESSION_START session=20260302T144456Z pid=8273 python=3.12.3

### [2026-03-02T14:44:56.381762+00:00] *System*

 [pytest]  
pytest session started  args=['/Users/vrathod1/.vscode/extensions/ms-python.vscode-pylance-2026.1.1/dist/bundled/files/get_pytest_options.py']

