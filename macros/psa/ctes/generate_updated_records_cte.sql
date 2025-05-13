{% macro generate_updated_records_cte(is_incremental, non_system_columns, new_columns, system_vars) %}
{% if is_incremental %}
updated_records as (
  select
    source.{{ non_system_columns | join(', source.') }}
    ,'Y' as _is_current
    ,'N' as _is_deleted
    ,existing._version + 1 as _version
    ,cast(current_timestamp as timestamp) as _valid_from
    ,cast('3000-01-01 00:00:00' as timestamp) as _valid_to
    ,cast(current_timestamp as timestamp) as _last_seen_time
    ,existing._inserted_time
    ,cast(existing._inserted_by_job_id  as varchar(50)) as _inserted_by_job_id
    ,cast(existing._inserted_by_task_id  as varchar(50)) as _inserted_by_task_id
    ,existing._inserted_by_execution_id
    ,cast(current_timestamp as timestamp) as _updated_time
    ,cast({{ system_vars.job_id }}  as varchar(50)) as _updated_by_job_id
    ,cast({{ system_vars.task_id }}  as varchar(50)) as _updated_by_task_id
    ,{{ system_vars.sequence_id }} as _updated_by_execution_id
  
    {% if new_columns %}
      {% for column in new_columns %}
        , source.{{ column }}
      {% endfor %}
    {% endif %}

    {% if new_columns %}
      {% for column in new_columns %}
        , source.{{ column }}
      {% endfor %}
    {% endif %}
  from stg_model source
  inner join existing_data existing
    on source._source_hash_key = existing._source_hash_key
  where source._source_hash_diff <> existing._source_hash_diff
),
{% endif %}
{% endmacro %}