{% macro get_required_columns(model_name) %}
    {% set column_metadata_map = get_column_test_metadata_map(model_name) %}
    {% set required_columns = [] %}

    {% for column_name, meta in column_metadata_map.items() %}
        {% if meta.get('is_not_null', 'false') | lower == 'true' %}
            {% do required_columns.append(column_name) %}
        {% endif %}
    {% endfor %}

    {{ return(required_columns) }}
{% endmacro %}
