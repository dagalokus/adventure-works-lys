with
    source_data as (
        select business_entity_id, phone_number
        from {{ source("Person", "person_phone") }}
    )
select *
from source_data
