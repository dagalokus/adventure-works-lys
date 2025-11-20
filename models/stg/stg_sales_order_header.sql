<<<<<<< HEAD
with
    sales_order_transaction as (
        select * from {{ source("Sales", "sales_order_header") }}
    )

select *
from sales_order_transaction
=======
with
    sales_order_transaction as (
        select * from {{ source("Sales", "sales_order_header") }}
    )

select *
from sales_order_transaction
>>>>>>> a4ff0ad41e2166f670f8e5056cd28529504530bd
