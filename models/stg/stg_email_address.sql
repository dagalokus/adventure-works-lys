with
    source_data as (
        select business_entity_id, email_address
        from {{ source("Person", "email_address") }}
    )
select *
from source_data
