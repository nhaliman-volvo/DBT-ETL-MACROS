{% macro generate_source_key(source_stage) %}
    {{ return(local_md5(source_stage)) }}
{% endmacro %}
