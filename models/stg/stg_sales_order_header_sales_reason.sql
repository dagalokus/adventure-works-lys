<<<<<<< HEAD
with
    sales_header_reason as (
        select * from {{ source("Sales", "sales_order_header_sales_reason") }}
    )

select *
from sales_header_reason
=======
with
    sales_header_reason as (
        select * from {{ source("Sales", "sales_order_header_sales_reason") }}
    )

select *
from sales_header_reason
>>>>>>> a4ff0ad41e2166f670f8e5056cd28529504530bd
