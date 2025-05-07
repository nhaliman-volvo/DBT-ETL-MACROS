{% macro get_accepted_values_map(model_name) %}
    {% set column_metadata_map = get_column_test_metadata_map(model_name) %}
    {% set accepted_values_map = {} %}

    {% for column_name, meta in column_metadata_map.items() %}
        {% set accepted_values = meta.get('accepted_values', []) %}

        {% if accepted_values %}
            {% do accepted_values_map.update({ column_name: accepted_values }) %}
        {% endif %}
    {% endfor %}

    {{ return(accepted_values_map) }}
{% endmacro %}
