{% macro update_logging_status() %}
{% if execute %}
    {% set db = target.database %}
    {% set log_table = db ~ '.' ~ schema ~ '.DBT_LOGGING_TABLE' %}
    {% set key = generate_logging_key() %}

    {% set model_name = model.name %}
    {% set update_log_table_query %}
        UPDATE {{ log_table }} AS log
        SET inserted_record_count = (
            SELECT COUNT(*) FROM {{ this }}
        ),
        status = CASE 
            WHEN EXISTS (SELECT 1 FROM {{ this }}) THEN 'SUCCESS'
            ELSE 'FAILURE'
        END,
        timestamp = CURRENT_TIMESTAMP()
        WHERE log.key = '{{ key }}' AND log.{{ adapter.quote('table') }} = '{{ model_name }}'
    {% endset %}
    {% do run_query(update_log_table_query) %}
    {% do update_logging_key() %}
{% endif %}
{% endmacro %}
