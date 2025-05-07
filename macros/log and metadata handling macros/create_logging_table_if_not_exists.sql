{% macro create_logging_table_if_not_exists(log_table) %}
{% set create_log_table_query %}
    CREATE TABLE IF NOT EXISTS {{ log_table }} (
        key STRING,
        source_key STRING,
        snowflake_stg STRING,
        {{ adapter.quote('table') }} STRING,
        role STRING,
        timestamp TIMESTAMP,
        status STRING,
        db STRING,
        schema STRING,
        layer STRING,
        batch_id STRING,
        inserted_record_count INT
    );
{% endset %}
{% do run_query(create_log_table_query) %}
{% endmacro %}
