{% macro get_all_columns(model_name) %}
    {% set query %}
        SELECT column_name
        FROM expected_dtypes
        WHERE model_name = '{{ model_name }}'
    {% endset %}

    {% set results = run_query(query) %}

    {% if results %}
        {% set column_names = results.rows | map(attribute='COLUMN_NAME') | list %}
        {{ return(column_names) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}
