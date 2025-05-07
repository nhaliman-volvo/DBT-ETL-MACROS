{% macro combine_error_results(null_errors, type_errors, duplicate_errors, accepted_value_errors) %}
    select * from {{ null_errors }}
    union all
    select * from {{ type_errors }}
    union all
    select * from {{ duplicate_errors }}
    union all
    select * from {{ accepted_value_errors }}
{% endmacro %}
