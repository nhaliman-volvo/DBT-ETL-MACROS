{% macro get_columns_from_current_model(model) %}
    {% set columns = [] %}

    {% for col_name in model.columns %}
        {% do columns.append(col_name) %}
    {% endfor %}

    {{ return(columns) }}
{% endmacro %}
