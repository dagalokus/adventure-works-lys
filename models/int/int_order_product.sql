with
    order_product as (
        select *
        from {{ ref("stg_sales_order_detail") }}
        inner join {{ ref("stg_product") }} using (product_id)
    ),
    final as (
        select
            sales_order_id,
            sales_order_detail_id,
            carrier_tracking_number,
            order_qty,
            product_id,
            unit_price,
            unit_price_discount,
            line_total,
            name,
            product_number,
            color,
            safety_stock_level,
            reorder_point,
            standard_cost,
            list_price,
            size,
            weight,
            days_to_manufacture,
            sell_start_date,
            sell_end_date,
            discontinued_date
        from order_product
    )

select *
from final
