{% macro apply_all_error_checks(model_ref, primary_key) %}
    with source as (
        select * ,
               row_number() over (partition by {{ primary_key }} order by {{ primary_key }}) as row_num
        from {{ ref(model_ref) }}
    )
    select * from (
        {{ classify_null_errors(model_ref, primary_key) }}
        union all
        {{ classify_datatype_errors(model_ref, primary_key) }}
        union all
        {{ classify_duplicate_errors(model_ref, primary_key) }}
        union all
        {{ classify_accepted_value_errors(model_ref, primary_key) }}
    )
{% endmacro %}
