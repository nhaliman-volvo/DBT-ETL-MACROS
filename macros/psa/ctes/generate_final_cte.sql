{% macro generate_final_cte(is_incremental) %}
final as 
(
  select * 
  from new_records
  {% if is_incremental %}
  union all
  
  select * 
  from updated_records
  
  union all
  
  select * 
  from mark_previous_records_as_old
  
  union all
  
  select * 
  from unchanged_records
  {% endif %}
)

select *
from final
{% endmacro %}