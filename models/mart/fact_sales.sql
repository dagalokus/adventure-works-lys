with fact_sales as (select * from {{ ref("int_sales_order_header") }})

select *
from fact_sales
