with source_data as (
    select
        business_entity_id,
        rate_change_date as rate_change_date_at,
        rate,
        pay_frequency,
        modified_date as modified_date_at
    from {{ source('HR', 'employee_pay_history') }}
)
select * from source_data