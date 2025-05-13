-- Import all PSA macros
-- This file ensures all macros are available when including the psa namespace

-- Config macros
{% import 'psa/config/get_psa_config.sql' as config %}

-- System macros
{% import 'psa/system/get_system_variables.sql' as system %}

-- Column handling macros
{% import 'psa/columns/get_relation_columns.sql' as relation_columns %}
{% import 'psa/columns/get_staging_columns.sql' as staging_columns %}

-- CTE generation macros
{% import 'psa/ctes/generate_stg_model_cte.sql' as stg_model_cte %}
{% import 'psa/ctes/generate_existing_data_cte.sql' as existing_data_cte %}
{% import 'psa/ctes/generate_new_records_cte.sql' as new_records_cte %}
{% import 'psa/ctes/generate_updated_records_cte.sql' as updated_records_cte %}
{% import 'psa/ctes/generate_mark_previous_records_cte.sql' as mark_previous_records_cte %}
{% import 'psa/ctes/generate_unchanged_records_cte.sql' as unchanged_records_cte %}
{% import 'psa/ctes/generate_final_cte.sql' as final_cte %}

-- Main template
{% import 'psa/change_tracking_versioning_template.sql' as main_template %}