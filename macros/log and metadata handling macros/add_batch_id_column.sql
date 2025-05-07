{% macro add_batch_id_column(source_model) %}
    {% set add_batch_id_query %}
        ALTER TABLE {{ ref(source_model) }}
        ADD COLUMN IF NOT EXISTS batch_id STRING;

        UPDATE {{ ref(source_model) }}
        SET batch_id = (
            SELECT 
                SHA1(LISTAGG(DISTINCT _File_Name, '') WITHIN GROUP (ORDER BY _File_Name))
            FROM {{ ref(source_model) }}
        );
    {% endset %}
    {% do run_query(add_batch_id_query) %}
{% endmacro %}
