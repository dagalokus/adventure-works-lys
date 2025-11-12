with
    source_data as (
        select
            business_entity_id,
            person_type,
            first_name,
            last_name,
            email_promotion,
            additional_contact_info,
            demographics
        from {{ source("Person", "person") }}
    )
select *
from source_data
