{% macro export_column_metadata() %}
    {% set column_metadata = [] %}
    {% set test_metadata = [] %}

    {% set create_table_sql %}
        CREATE TABLE IF NOT EXISTS expected_dtypes (
            model_name VARCHAR,
            column_name VARCHAR,
            expected_dtype VARCHAR,
            is_not_null BOOLEAN,
            is_unique BOOLEAN,
            accepted_values TEXT
        );
    {% endset %}
    {% do run_query(create_table_sql) %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'model' %}
            {% for col_name, col in node.columns.items() %}
                {% if col.meta.get('expected_type') %}
                    {% do column_metadata.append({
                        "model_name": node.name,
                        "column_name": col_name,
                        "expected_dtype": col.meta['expected_type'],
                        "is_not_null": false,
                        "is_unique": false,
                        "accepted_values": ''
                    }) %}
                {% endif %}
            {% endfor %}
        {% endif %}
    {% endfor %}

    {% for node in graph.nodes.values() %}
        {% if node.resource_type == 'test' %}
            {% set depends_on_nodes = node.get('depends_on', {}).get('nodes', []) %}
            {% set model_name = depends_on_nodes[0].split('.')[-1] if depends_on_nodes | length > 0 else 'UNKNOWN' %}
            {% set test_metadata_node = node.get('test_metadata') %}

            {% if test_metadata_node is not none %}
                {% set test_type = test_metadata_node.name %}
                {% set args = test_metadata_node.kwargs %}
                {% set column_name = args.get('column_name', 'N/A') %}

                {% if column_name != 'N/A' %}
                    {% do test_metadata.append({
                        "model_name": model_name,
                        "column_name": column_name,
                        "test_type": test_type,
                        "args": args
                    }) %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}

    {% for col in column_metadata %}
        {% set matching_tests = test_metadata
            | selectattr("model_name", "equalto", col.model_name)
            | selectattr("column_name", "equalto", col.column_name)
            | list %}

        {% for test in matching_tests %}
            {% if test.test_type == 'not_null' %}
                {% set _ = col.update({'is_not_null': true}) %}
            {% elif test.test_type == 'unique' %}
                {% set _ = col.update({'is_unique': true}) %}
            {% elif test.test_type == 'accepted_values' %}
                {% set values = test.args.get('values', []) %}
                {% set _ = col.update({'accepted_values': values | join(', ')}) %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {% if execute %}
        {% if column_metadata | length > 0 %}
            {% set delete_sql = "DELETE FROM expected_dtypes;" %}
            {% do run_query(delete_sql) %}

            {% set insert_sql %}
                INSERT INTO expected_dtypes (model_name, column_name, expected_dtype, is_not_null, is_unique, accepted_values)
                VALUES
                {% for row in column_metadata %}
                    (
                        '{{ row.model_name }}',
                        '{{ row.column_name }}',
                        '{{ row.expected_dtype }}',
                        {{ 'true' if row.is_not_null else 'false' }},
                        {{ 'true' if row.is_unique else 'false' }},
                        '{{ row.accepted_values }}'
                    ){% if not loop.last %}, {% endif %}
                {% endfor %}
            {% endset %}
            
            {% do run_query(insert_sql) %}
        {% else %}
            {% do log("⚠️ No rows to insert.") %}
        {% endif %}
    {% endif %}
{% endmacro %}
