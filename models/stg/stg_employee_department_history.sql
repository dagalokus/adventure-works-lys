with
    source_data as (
        select
            business_entity_id,
            department_id,
            shift_id,
            start_date as start_date_at,
            end_date as end_date_at,
            modified_date as modified_date_at
        from {{ source("HR", "employee_department_history") }}
    )
select *
from source_data
