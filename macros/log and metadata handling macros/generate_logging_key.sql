{% macro generate_logging_key() %}
    {% set table_ref = target.database ~ '.' ~ target.schema ~ '.KEY_ID_GENERATOR' %}

    {% set query %}
        select "VALUE" 
        from {{ table_ref }}
        where "VARIABLE" = 'KEY'
    {% endset %}

    {% set result = run_query(query) %}
    
    {% if result.rows|length == 0 %}
        {% do exceptions.raise("No key found in the KEY_ID_GENERATOR table") %}
    {% endif %}

    {% set key_value = result.rows[0][0] %}
    
    {{ return(key_value) }}
{% endmacro %}
