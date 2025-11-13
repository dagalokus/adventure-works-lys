with
    sales_order_transaction as (
        select *
        from {{ ref("stg_sales_order_transaction") }}
        left join {{ ref("stg_sales_order") }} using (sales_order_id)
        left join {{ ref("stg_product") }} using (product_id)
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
            bill_to_address_id,
            ship_to_address_id,
            credit_card_id,
            currency_rate_id,
            sub_total,
            tax_amt,
            freight,
            total_due,
            comment,
            order_qty,
            unit_price,
            unit_price_discount,
            line_total,
            product_id,
            name,
            product_number,
            standard_cost,
            list_price
        from sales_order_transaction
    )

select *
from final
