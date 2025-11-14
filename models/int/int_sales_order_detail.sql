with
    sales_order_detail as (
        select so.*, sh.order_date
        from {{ ref("stg_sales_order_detail") }} as so
        left join
            {{ ref("stg_sales_order_header") }} as sh
            on so.sales_order_id = sh.sales_order_id
    ),

    final as (
        select
            sales_order_id,
            sales_order_detail_id,
            order_date,
            order_qty,
            product_id,
            unit_price,
            unit_price_discount,
            line_total as total
        from sales_order_detail
    )

select *
from final
