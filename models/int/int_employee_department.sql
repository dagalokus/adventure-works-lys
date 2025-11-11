with
    employee_department as (
        select
            p.business_entity_id,
            p.first_name,
            p.last_name,
            p.person_type,
            job_title,
            hire_date,
            name as department,
            group_name,
            rate as hourly_rate
        from {{ ref("stg_person") }} as p
        left join
            {{ ref("stg_employee") }} as e
            on p.business_entity_id = e.business_entity_id
        left join
            {{ ref("stg_employee_pay_history") }} as ep
            on e.business_entity_id = ep.business_entity_id
        left join
            {{ ref("stg_employee_department_history") }} as ed
            on ep.business_entity_id = ed.business_entity_id
        left join {{ ref("stg_department") }} as d on ed.department_id = d.department_id
    )
select *
from employee_department
where person_type = "EM" or  person_type = "SP"
