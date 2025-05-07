{% macro validate_datatype(column, expected_type) %}
    {% set expected_type_lower = expected_type | lower %}
    {% if expected_type_lower in ['int', 'integer', 'number'] -%}
        try_cast(trim({{ column }}) as integer) is null and trim({{ column }}) is not null
    {%- elif expected_type_lower in ['float', 'double', 'decimal'] -%}
        try_cast(trim({{ column }}) as float) is null and trim({{ column }}) is not null
    {%- elif expected_type_lower in ['timestamp', 'datetime'] -%}
        try_cast(trim({{ column }}) as timestamp) is null and trim({{ column }}) is not null
    {%- elif expected_type_lower in ['date'] -%}
        to_date(trim({{ column }}), 'DD-MM-YYYY') is null and trim({{ column }}) is not null
    {%- elif expected_type_lower in ['string', 'text', 'varchar'] -%}
        try_cast(trim({{ column }}) as varchar) is null and trim({{ column }}) is not null
    {%- else -%}
        false
    {%- endif %}
{% endmacro %}
