with
    person_info as (
        select
            p.business_entity_id,
            p.person_type,
            p.first_name,
            p.last_name,
            p.email_promotion,
            p.additional_contact_info,
            p.demographics,
            pp.phone_number,
            ea.email_address
        from {{ ref("stg_person") }} as p
        left join
            {{ ref("stg_person_phone") }} as pp
            on p.business_entity_id = pp.business_entity_id
        left join
            {{ ref("stg_email_address") }} as ea
            on pp.business_entity_id = ea.business_entity_id
    )

select *
from person_info
