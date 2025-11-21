with
    sales_order_header as (select * from {{ ref("stg_sales_order_header") }}),

    final as (
        select
            sales_order_id,
            order_date,
            due_date,
            ship_date,
            status,
            territory_id,
            ship_to_address_id,
            ship_method_id
        from sales_order_header
    )

select *
from final
