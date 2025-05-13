{% macro get_and_increment_system_variables() %}
    {# Retrieve the current database and schema from dbt's target context #}
    {% set database = target.database %}
    {% set schema = 'stg' %}
    {% set table = 'stg_parameters_state_management' %}
    
    {# Construct the fully qualified table name #}
    {% set table_ref = database ~ '.' ~ schema ~ '.' ~ table %}

    select
        coalesce(max(case when variable_name = 'wsl_sequence' then value end), 0) + 1 as wsl_sequence,
        coalesce(max(case when variable_name = 'wsl_job_key' then value end), 0) + 1 as wsl_job_key,
        coalesce(max(case when variable_name = 'wsl_task_key' then value end), 0) + 1 as wsl_task_key
    from {{ table_ref }}
{% endmacro %}
