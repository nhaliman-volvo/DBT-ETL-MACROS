{% macro add_metadata_columns() %}
    METADATA$FILENAME AS _File_Name,
    CURRENT_TIMESTAMP() AS _ONBOARDED_TIME,
    CURRENT_TIMESTAMP() AS _INSERTED_TIME
{% endmacro %}
