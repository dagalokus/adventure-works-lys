with sales_reason as (select * from {{ source("Sales", "sales_reason") }})

select *
from sales_reason
