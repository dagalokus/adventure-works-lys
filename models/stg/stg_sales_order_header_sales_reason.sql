with
    sales_header_reason as (
        select * from {{ source("Sales", "sales_order_header_sales_reason") }}
    )

select *
from sales_header_reason
