{% macro get_relation_columns(relation) %}
{% set database_name = relation.database %}
{% set database_name_lower = database_name.lower() %}
{% set this_relation = adapter.get_relation
  (
    database=database_name_lower,
    schema=relation.schema,
    identifier=relation.name
  ) 
%}

{% if execute %}
  {% if this_relation %}
    {% set this_columns_raw = adapter.get_columns_in_relation(this_relation) %}
  {% else %}
    {% set this_columns_raw = [] %}
  {% endif %}
{% else %}
    {% set this_columns_raw = [{'name': 'placeholder_column', 'value': '1'}] %}
{% endif %}

{% set this_columns = this_columns_raw | map(attribute='name') | map('lower') | list %}
{% set system_columns = ['_is_current', '_is_deleted', '_version', '_valid_from', '_valid_to', '_last_seen_time', '_inserted_time', '_inserted_by_job_id', '_inserted_by_task_id', '_inserted_by_execution_id', '_updated_time', '_updated_by_job_id', '_updated_by_task_id', '_updated_by_execution_id'] %}
{% set non_system_columns = this_columns | reject('in', system_columns) | list %}
{% set all_columns = non_system_columns + system_columns %}

{% set column_info = {
  'this_columns': this_columns,
  'system_columns': system_columns,
  'non_system_columns': non_system_columns,
  'all_columns': all_columns,
  'all_columns_names': all_columns | join(', ')
} %}

{{ return(column_info) }}
{% endmacro %}