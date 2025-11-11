with
    source_data as (
        select business_entity_id, person_type, first_name, last_name
        from {{ source("Person", "person") }}
    )
select *
from source_data
