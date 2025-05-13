{% macro generate_existing_data_cte(is_incremental, all_columns_names) %}
{% if is_incremental %}
existing_data as 
(
  select 
    {{ all_columns_names }}

  from {{ this }}
  where _is_current = 'Y'
),
{% endif %}
{% endmacro %}