with
    sales_order_header as (
        select *
        from {{ ref("stg_sales_order_header") }}
    ),

    final as (
        select
            sales_order_id,
            order_date,
            due_date,
            ship_date,
            status,
            online_order_flag,
            sales_order_number,
            purchase_order_number,
            account_number,
            customer_id,
            sales_person_id,
            territory_id,
            ship_to_address_id,
            total_due
        from sales_order_header
    )

select *
from final
