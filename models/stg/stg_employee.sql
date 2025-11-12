with
    source_data as (
        select
            business_entity_id,
            national_id_number,
            organization_node,
            organization_level,
            gender,
            vacation_hours,
            sick_leave_hours,
            job_title,
            hire_date,
            salaried_flag,
            current_flag,
            modified_date as modified_date_at
        from {{ source("HR", "employee") }}
    )
select *
from source_data
