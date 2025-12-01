with
    int_customer as (
        select *
        from {{ ref("stg_person") }}
        left join {{ ref("stg_person_phone") }} using (business_entity_id)
        left join {{ ref("stg_email_address") }} using (business_entity_id)
    ),

    final as (
        select
            business_entity_id,
            person_type,
            concat(first_name, ' ', last_name) as full_name,
            email_promotion,
            additional_contact_info,
            demographics,
            phone_number,
            email_address
        from int_customer
    )

select *
from final
