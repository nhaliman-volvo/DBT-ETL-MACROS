{% macro get_staging_columns(stg_model_name, this_columns, system_columns) %}
{% if execute %}
    {% set stg_columns_raw = adapter.get_columns_in_relation(ref(stg_model_name)) %}
{% else %}
    {% set stg_columns_raw = [{'name': 'placeholder_column', 'value': '1'}] %}
{% endif %}

{% set stg_columns = stg_columns_raw | map(attribute='name') | map('lower') | list %}
{% set excluded_stg_columns = ['_execution_id', '_job_id', '_task_id','_inserted_time'] %}
{% set filtered_stg_columns = stg_columns | reject('in', excluded_stg_columns) | list %}
{% set new_columns = filtered_stg_columns | reject('in', this_columns + system_columns) | list %}

{% set stg_column_info = {
  'stg_columns': stg_columns,
  'filtered_stg_columns': filtered_stg_columns,
  'new_columns': new_columns
} %}

{{ return(stg_column_info) }}
{% endmacro %}