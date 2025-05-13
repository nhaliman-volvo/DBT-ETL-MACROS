{% macro generate_unchanged_records_cte(is_incremental, all_columns_names, new_columns) %}
{% if is_incremental %}
unchanged_records as 
(
    select
      {{ all_columns_names }}
      {% if new_columns %}
      {% for column in new_columns %}
        , NULL as {{ column }}
      {% endfor %}
    {% endif %}
    from {{ this }}
    where _is_current = 'N'
    union all
    select
      {{ all_columns_names }}
    
      {% if new_columns %}
      {% for column in new_columns %}
        , NULL as {{ column }}
      {% endfor %}
    {% endif %}
    from {{ this }}
    where _is_current = 'Y'
      and not exists (select 1 from updated_records updated where updated._source_hash_key = {{ this }}._source_hash_key)
),
{% endif %}
{% endmacro %}