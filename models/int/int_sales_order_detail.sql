with
  sales_order_detail_source as (
    select *
    from {{ ref("stg_sales_order_detail") }}
  ),

  sales_order_header_source as (
    select
      sales_order_id,
      order_date
    from {{ ref("stg_sales_order_header") }}
  ),

  sales_order_detail as (
    select
      so.sales_order_id,
      so.sales_order_detail_id,
      sh.order_date,
      so.order_qty,
      so.product_id,
      so.unit_price,
      so.unit_price_discount,
      so.line_total as total
    from sales_order_detail_source as so
    left join sales_order_header_source as sh
      on so.sales_order_id = sh.sales_order_id
  ),

  final as (
    select *
    from sales_order_detail
  )

select *
from final
