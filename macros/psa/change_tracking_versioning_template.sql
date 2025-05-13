{% macro change_tracking_versioning_template(stg_model_name,primary_key) %}
{# Apply configuration #}
{{ get_psa_config() }}

{# Get system variables #}
{% set system_vars = get_system_variables() %}

{# Get column information #}
{% set column_info = get_relation_columns(this) %}
{% set this_columns = column_info.this_columns %}
{% set system_columns = column_info.system_columns %}
{% set non_system_columns = column_info.non_system_columns %}
{% set all_columns = column_info.all_columns %}
{% set all_columns_names = column_info.all_columns_names %}

{# Get staging columns #}
{% set stg_column_info = get_staging_columns(stg_model_name, this_columns, system_columns) %}
{% set stg_columns = stg_column_info.stg_columns %}
{% set filtered_stg_columns = stg_column_info.filtered_stg_columns %}
{% set new_columns = stg_column_info.new_columns %}

{# Generate CTE queries #}
{{ generate_stg_model_cte(stg_model_name, stg_columns, primary_key) }}
{{ generate_existing_data_cte(is_incremental(), all_columns_names) }}
{{ generate_new_records_cte(is_incremental(), non_system_columns, filtered_stg_columns, new_columns, system_vars) }}
{{ generate_updated_records_cte(is_incremental(), non_system_columns, new_columns, system_vars) }}
{{ generate_mark_previous_records_cte(is_incremental(), non_system_columns, new_columns) }}
{{ generate_unchanged_records_cte(is_incremental(), all_columns_names, new_columns) }}
{{ generate_final_cte(is_incremental()) }}

{# Update system variables #}
{%- do update_system_variables(system_vars.sequence_id, system_vars.job_id, system_vars.task_id) -%}

{% endmacro %}