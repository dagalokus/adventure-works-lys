with dim_order_product as (select * from {{ ref("int_order_product") }})

select *
from dim_order_product
