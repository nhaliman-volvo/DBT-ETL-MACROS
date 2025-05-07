{% macro generate_column_casts(model_name) %}
    {% set column_metadata_map = get_column_test_metadata_map(model_name) %}
    {% set sql_exprs = [] %}

    {% for col_name, meta in column_metadata_map.items() %}
        {% set expected_dtype = meta.get('expected_dtype') %}
        {% if expected_dtype %}
            {% set expr = "$" ~ loop.index ~ "::" ~ (expected_dtype | upper) ~ " AS " ~ col_name %}
            {% do sql_exprs.append(expr) %}
        {% endif %}
    {% endfor %}

    {{ return(sql_exprs | join(',\n    ')) }}
{% endmacro %}
