{% macro generate_mark_previous_records_cte(is_incremental, non_system_columns, new_columns) %}
{% if is_incremental %}
mark_previous_records_as_old as 
(
  select
    existing.{{ non_system_columns | join(', existing.') }}
    ,'N' as _is_current
    ,existing._is_deleted
    ,existing._version
    ,existing._valid_from
    ,dateadd('millisecond', -1, current_timestamp) as _valid_to
    ,existing._last_seen_time
    ,existing._inserted_time
    ,cast(existing._inserted_by_job_id  as varchar(50)) as _inserted_by_job_id
    ,cast(existing._inserted_by_task_id  as varchar(50)) as _inserted_by_task_id
    ,existing._inserted_by_execution_id
    ,existing._updated_time
    ,cast(existing._updated_by_job_id  as varchar(50)) as _updated_by_job_id
    ,cast(existing._updated_by_task_id  as varchar(50)) as _updated_by_task_id
    ,existing._updated_by_execution_id
 
    
  

    {% if new_columns %}
      {% for column in new_columns %}
        , NULL as {{ column }}
      {% endfor %}
    {% endif %}
  from existing_data existing
  where 
    existing._is_current = 'Y'
    and exists (select 1 from updated_records updated where updated._source_hash_key = existing._source_hash_key)
),
{% endif %}
{% endmacro %}