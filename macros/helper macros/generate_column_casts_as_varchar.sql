{% macro generate_column_casts_as_varchar(model_name) %}
    {% set column_metadata_map = get_column_test_metadata_map(model_name) %}
    {% set sql_exprs = [] %}

    {% for col_name in column_metadata_map.keys() %}
        {% set expr = "$" ~ loop.index ~ " AS " ~ col_name %}
        {% do sql_exprs.append(expr) %}
    {% endfor %}

    {{ return(sql_exprs | join(',\n    ')) }}
{% endmacro %}
