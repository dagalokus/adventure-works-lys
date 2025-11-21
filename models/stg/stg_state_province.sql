with state_province as (select * from {{ source("Person", "state_province") }})

select *
from state_province
