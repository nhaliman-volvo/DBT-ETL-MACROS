{% macro get_system_variables() %}
{%- call statement('get_system_variable_query', fetch_result=True) -%}
      {{ get_and_increment_system_variables() }}
{%- endcall -%}

{%- set result = load_result('get_system_variable_query') -%}
{%- set sequence_id_value = result['data'][0][0] if result['data'] else 0 -%}
{%- set job_id_value = result['data'][0][1] if result['data'] else 0 -%}
{%- set task_id_value = result['data'][0][2] if result['data'] else 0 -%}

{% set system_vars = {
  'sequence_id': sequence_id_value,
  'job_id': job_id_value,
  'task_id': task_id_value
} %}

{{ return(system_vars) }}
{% endmacro %}