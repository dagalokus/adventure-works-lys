with
    employee as (select * from {{ ref("int_employee") }}),

    person as (select * from {{ ref("int_person") }} where person_type in ('EM', 'SP')),

    final as (
        select
            business_entity_id,
            organization_level,
            job_title,
            gender,
            hire_date,
            vacation_hours,
            sick_leave_hours,
            hourly_rate,
            pay_frequency,
            department_id,
            start_date_at,
            name,
            group_name,
            person_type
        from employee
        left join person using (business_entity_id)
    )
select *
from final
