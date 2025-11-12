with
    department as (select * from {{ ref("stg_department") }}),

    employee as (select * from {{ ref("stg_employee") }}),

    employee_current_department as (
        select *
        from {{ ref("stg_employee_department_history") }}
        where end_date_at is null
    ),

    employee_current_pay as (
        select *
        from {{ ref("stg_employee_pay_history") }}
        qualify
            rank() over (
                partition by business_entity_id order by rate_change_date_at desc
            )
            = 1
    ),

    final as (
        select
            e.business_entity_id,
            e.organization_level,
            e.job_title,
            e.gender,
            e.hire_date,
            e.vacation_hours,
            e.sick_leave_hours,
            ep.rate as hourly_rate,
            ep.pay_frequency,
            ed.department_id,
            ed.start_date_at,
            d.name,
            d.group_name
        from employee as e
        left join
            employee_current_pay as ep on e.business_entity_id = ep.business_entity_id
        left join
            employee_current_department as ed
            on e.business_entity_id = ed.business_entity_id
        left join department as d on ed.department_id = d.department_id
    )

select *
from final
