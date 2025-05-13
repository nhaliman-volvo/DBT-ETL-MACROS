{% macro generate_new_records_cte(is_incremental, non_system_columns, filtered_stg_columns, new_columns, system_vars) %}
new_records as 
(
  select
    {% if is_incremental %}
    source.{{ non_system_columns | join(', source.') }}
    {% else %}
    source.{{ filtered_stg_columns | join(', source.') }}
    ,source._source_hash_key
    ,source._source_hash_diff
    {% endif %}
    ,'Y' as _is_current
    ,'N' as _is_deleted
    ,1 as _version
    ,cast(current_timestamp as timestamp) as _valid_from
    ,cast('3000-01-01 00:00:00' as timestamp) as _valid_to
    ,cast(current_timestamp as timestamp) as _last_seen_time
    ,cast(current_timestamp as timestamp) as _inserted_time
    ,cast({{ system_vars.job_id }}  as varchar(50)) as _inserted_by_job_id
    ,cast({{ system_vars.task_id }}  as varchar(50)) as _inserted_by_task_id
    ,{{ system_vars.sequence_id }} as _inserted_by_execution_id
    ,cast(current_timestamp as timestamp) as _updated_time
    ,cast({{ system_vars.job_id }}  as varchar(50)) as _updated_by_job_id
    ,cast({{ system_vars.task_id }}  as varchar(50)) as _updated_by_task_id
    ,{{ system_vars.sequence_id }} as _updated_by_execution_id
   
    {% if is_incremental %}
    {% if new_columns %}
      {% for column in new_columns %}
        , source.{{ column }}
      {% endfor %}
    {% endif %}
    {% endif %}
  from stg_model source
  {% if is_incremental %}
  left join existing_data existing
    on source._source_hash_key = existing._source_hash_key
  where existing._source_hash_key is null
  {% endif %}
),
{% endmacro %}