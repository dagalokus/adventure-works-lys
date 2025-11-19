with
    sales_order_transaction as (
        select * from {{ source("Sales", "sales_order_header") }}
    )
select *
from sales_order_transaction
