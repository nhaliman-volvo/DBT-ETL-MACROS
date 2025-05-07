{% macro get_column_test_metadata_map(model_name) %}
    {% set query %}
        SELECT column_name, expected_dtype, is_not_null, is_unique, accepted_values
        FROM expected_dtypes
        WHERE model_name = '{{ model_name }}'
    {% endset %}

    {% if execute %}
        {% set results = run_query(query) %}

        {% if results and results.rows | length > 0 %}
            {% set metadata_map = {} %}

            {% for row in results.rows %}
                {% set col_name = row[0] %}
                {% set expected_dtype = row[1] %}
                {% set is_not_null = row[2] %}
                {% set is_unique = row[3] %}
                {% set accepted_values_raw = row[4] %}

                {% set accepted_values = accepted_values_raw.split(', ') if accepted_values_raw else [] %}

                {% do metadata_map.update({
                    col_name: {
                        'expected_dtype': expected_dtype,
                        'is_not_null': is_not_null,
                        'is_unique': is_unique,
                        'accepted_values': accepted_values
                    }
                }) %}
            {% endfor %}

            {{ return(metadata_map) }}
        {% else %}
            {{ exceptions.raise_compiler_error("❌ No metadata found for model '" ~ model_name ~ "' in expected_dtypes.") }}
        {% endif %}
    {% else %}
        {{ return({}) }}
    {% endif %}
{% endmacro %}
