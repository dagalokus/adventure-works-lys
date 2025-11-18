with sales_order as (select * from {{ source("Sales", "sales_order_detail") }})

select *
from sales_order
