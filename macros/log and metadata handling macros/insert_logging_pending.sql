{% macro log_model_run_pending() %}
    {% if execute %}
        {% set db = target.database %}
        {% set schema = var('pr_number', target.schema) %}
        {% set role = target.role %}
        {% set log_table = db ~ '.' ~ schema ~ '.DBT_LOGGING_TABLE' %}

        {% set model_name = model.name %}
        {% set layer = model_name.split('_')[0] | upper %}
        {% set source_model = 'load_health_data' %}  {# You can make this dynamic if needed #}
        {% set source_stage = env_var('STG') %}
        {% set key =generate_logging_key() %}  {# This should be a unique key for the log entry #}

    
        {% set source_key = generate_source_key(source_stage) %}
        {% set batch_id = generate_batch_id(source_model) %}

        -- {% do log('Logging key: ' ~ key) %}
        -- {% do log('Source key: ' ~ source_key) %}
        -- {% do log('Batch ID: ' ~ batch_id) %}

        -- {# Create the log table if it doesn’t exist #}
        -- {% do create_logging_table_if_not_exists(log_table) %}

        -- {# Ensure batch_id column exists and populated in source model #}
        -- {% do add_batch_id_column(source_model) %}

        {% set insert_query %}
            INSERT INTO {{ log_table }} (
                key, source_key, snowflake_stg, {{ adapter.quote('table') }},
                role, timestamp, status, db, schema, layer, batch_id, inserted_record_count
            )
            VALUES (
                '{{ key }}',
                '{{ source_key }}',
                '{{ source_stage }}',
                '{{ model_name }}',
                '{{ role }}',
                CURRENT_TIMESTAMP(),
                'PENDING',
                '{{ db }}',
                '{{ schema }}',
                '{{ layer }}',
                '{{ batch_id }}',
                0
            );
        {% endset %}

        {% do run_query(insert_query) %}
    {% endif %}
{% endmacro %}
