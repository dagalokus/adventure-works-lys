with
    source_data as (
        select department_id, name, group_name, modified_date as modified_date_at
        from {{ source("HR", "department") }}
    )

select *
from source_data
