{% macro classify_null_errors(model_ref, primary_key) %}
    {% set required_cols = get_required_columns(model_ref) %}
    select *,
           'NULL_ERROR' as error_type
    from source
    where
    {%- for col in required_cols %}
        {{ col }} is null{% if not loop.last %} or {% endif %}
    {%- endfor %}
{% endmacro %}
