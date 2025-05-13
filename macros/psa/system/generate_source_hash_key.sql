
{% macro generate_source_hash_key(id_column) %}
    sha1(concat(upper(trim(coalesce(cast({{ id_column }} as varchar), ''))))) as _source_hash_key
{% endmacro %}
