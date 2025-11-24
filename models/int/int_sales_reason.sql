with
    sales_reason as (
        select *
        from {{ ref("stg_sales_reason") }}),

    sales_order_header_sales_reason as (
        select *
        from {{ ref("stg_sales_order_header_sales_reason") }}

    ),

    final as (
        select so.sales_order_id, so.sales_reason_id, s.name, s.reason_type
        from sales_order_header_sales_reason as so
        inner join sales_reason as s
        on so.sales_reason_id = s.sales_reason_id
    )

select *
from final
