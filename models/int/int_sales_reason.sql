with
    sales_reason as (
        select *
        from {{ ref("stg_sales_reason") }}
        inner join
            {{ ref("stg_sales_order_header_sales_reason") }} using (sales_reason_id)

    ),

    final as (
        select sales_order_id, sales_reason_id, name, reason_type from sales_reason
    )
select *
from final
