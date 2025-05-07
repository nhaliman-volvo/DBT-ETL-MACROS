{% macro classify_accepted_value_errors(model_ref, primary_key) %}
    {% set model_name = model_ref if model_ref is string else model_ref.name %}
    {% set accepted_values_map = get_accepted_values_map(model_name) %}
    select *,
           'ACCEPTED_VALUE_ERROR' as error_type
    from source
    where
    {%- for col, values in accepted_values_map.items() %}
        ({{ col }} is not null and {{ col }} not in (
            {%- for val in values %}
                '{{ val }}'{% if not loop.last %}, {% endif %}
            {%- endfor %}
        )){% if not loop.last %} or {% endif %}
    {%- endfor %}
{% endmacro %}
