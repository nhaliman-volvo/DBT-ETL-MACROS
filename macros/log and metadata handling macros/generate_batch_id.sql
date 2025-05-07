{% macro generate_batch_id(source_model_name) %}
    {% set query %}
        SELECT 
            SHA1(LISTAGG(DISTINCT _File_Name, '') WITHIN GROUP (ORDER BY _File_Name))
        FROM {{ ref(source_model_name) }}
    {% endset %}

    {% set result = run_query(query) %}
    {% if result is not none %}
        {% set batch_id = result.columns[0][0] %}
        {{ return(batch_id) }}
    {% else %}
        {{ exceptions.raise_compiler_error("Failed to generate batch ID for model: " ~ source_model_name) }}
    {% endif %}
{% endmacro %}
