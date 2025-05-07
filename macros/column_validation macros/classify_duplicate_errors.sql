{% macro classify_duplicate_errors(model_ref, primary_key) %}
    select *,
           'DUPLICATE_ERROR' as error_type
    from source
    where row_num > 1
{% endmacro %}
