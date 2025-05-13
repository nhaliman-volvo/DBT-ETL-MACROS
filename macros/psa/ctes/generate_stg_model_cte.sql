{% macro generate_stg_model_cte(stg_model_name, stg_columns, id_column) %}
with stg_model as (
    select 
        -- Select all original columns
        {{ stg_columns | join(', ') }},
        
        -- Add _source_hash_key based on the ID column
        {{ generate_source_hash_key(id_column) }},
        
        -- Add _source_hash_diff based on the rest of the columns
        {{ generate_hash_key(stg_columns, id_column) }}
        
    from {{ ref(stg_model_name) }}
),
{% endmacro %}
