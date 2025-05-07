{% macro update_logging_key() %}
    {% set table_ref = target.database ~ '.' ~ target.schema ~ '.KEY_ID_GENERATOR' %}

    {% set query %}
        update {{ table_ref }}
        set "VALUE" = "VALUE" + 1
        where "VARIABLE" = 'KEY'
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
