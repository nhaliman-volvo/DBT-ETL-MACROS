{% macro generate_hash_key(columns, id_column) %}
    {% set non_id_columns = [] %}
    {% for col in columns %}
        {% if col != id_column %}
            {% do non_id_columns.append(col) %}
        {% endif %}
    {% endfor %}

    sha1(
        concat(
            {% for col in non_id_columns %}
                trim(coalesce(cast({{ col }} as varchar), ''))
                {% if not loop.last %} || '|*' || {% endif %}
            {% endfor %}
        )
    ) as _source_hash_diff
{% endmacro %}
