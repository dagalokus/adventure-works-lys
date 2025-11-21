with fact_sales as (select * from {{ ref("int_sales_order_detail") }})

select *
from fact_sales