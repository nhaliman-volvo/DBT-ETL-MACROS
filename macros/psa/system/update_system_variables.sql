
{% macro update_system_variables(wsl_sequence_value, wsl_job_key_value, wsl_task_key_value) %}
    {# Retrieve the current database and schema from dbt's target context #}
    {% set database = target.database %}
    {% set schema = 'stg' %}
    {% set table = 'stg_parameters_state_management' %}
    
    {# Construct the fully qualified table name #}
    {% set table_ref = database ~ '.' ~ schema ~ '.' ~ table %}

    {# Construct the SQL statement to perform the MERGE operation #}
    {% set sql %}
    merge into {{ table_ref }} as target
    using (
        select 'wsl_sequence' as variable_name, {{ wsl_sequence_value }} as value union all
        select 'wsl_job_key' as variable_name, {{ wsl_job_key_value }} as value union all
        select 'wsl_task_key' as variable_name, {{ wsl_task_key_value }} as value
    ) as source
    on target.variable_name = source.variable_name
    when matched then
        update set value = source.value
    when not matched then
        insert (variable_name, value) values (source.variable_name, source.value);
    {% endset %}

    {# Execute the SQL statement #}
    {{ run_query(sql) }}

{% endmacro %}