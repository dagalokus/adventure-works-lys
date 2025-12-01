with
    dim_customer as (
        select * from {{ ref("int_customer") }} where person_type in ("IN", "SC", "GC")
    )

select *
from dim_customer
