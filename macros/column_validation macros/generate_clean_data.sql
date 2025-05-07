{% macro generate_clean_data(model_ref, error_model_ref, primary_key) %}
    with source as (
        select *,
               row_number() over (partition by {{ primary_key }} order by {{ primary_key }}) as row_num
        from {{ ref(model_ref) }}
    ),
    errors as (
        select {{ primary_key }}, row_num
        from {{ ref(error_model_ref) }}
        where error_type in (
            'NULL_ERROR',
            'DATATYPE_ERROR',
            'DUPLICATE_ERROR',
            'ACCEPTED_VALUE_ERROR'
        )
    ),
    cleaned as (
        select s.*
        from source s
        left join errors e
          on s.{{ primary_key }} = e.{{ primary_key }}
         and s.row_num = e.row_num
        where e.{{ primary_key }} is null
    )
    select * 
    from cleaned
{% endmacro %}
