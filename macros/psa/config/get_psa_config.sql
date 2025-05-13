{% macro get_psa_config() %}
{{
  config(
    materialized='incremental',
    unique_key=['_source_hash_key', '_source_hash_diff'],
    on_schema_change='append_new_columns'
  )
}}
{% endmacro %}