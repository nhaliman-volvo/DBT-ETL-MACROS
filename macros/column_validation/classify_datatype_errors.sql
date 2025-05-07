{% macro classify_datatype_errors(model_ref, primary_key) -%}
    {%- set col_metadata_map = get_column_test_metadata_map(model_ref) -%}
    select *,
           'DATATYPE_ERROR' as error_type
    from source
    where
    {%- set conditions = [] -%}
    {%- for col_name, meta in col_metadata_map.items() -%}
        {%- if meta.expected_dtype is not none -%}
            {%- set condition = validate_datatype(col_name, meta.expected_dtype) -%}
            {%- do conditions.append(condition) -%}
        {%- endif -%}
    {%- endfor -%}
    {{- conditions | join(' or ') }}
{%- endmacro %}
