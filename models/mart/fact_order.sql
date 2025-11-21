with order as (select * from {{ ref("int_sales_order_header") }})

select *
from order
