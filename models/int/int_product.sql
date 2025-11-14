with
    product as (
        select
            p.product_id,
            p.product_number,
            p.color,
            p.safety_stock_level,
            p.reorder_point,
            p.standard_cost,
            p.list_price,
            p.size,
            p.weight,
            p.days_to_manufacture,
            p.sell_start_date,
            p.sell_end_date,
            p.discontinued_date,
            p.name as product_name,
            ps.name as subcategory_name,
            pc.name as category_name,
            ps.product_subcategory_id,
            pc.product_category_id
        from {{ ref("stg_product") }} as p
        inner join
            {{ ref("stg_product_subcategory") }} as ps
            on p.product_subcategory_id = ps.product_subcategory_id
        inner join
            {{ ref("stg_product_category") }} as pc
            on ps.product_category_id = pc.product_category_id
    ),

    final as (
        select
            product_name,
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
            discontinued_date,
            product_category_id,
            category_name,
            subcategory_name
        from product
    )

select *
from final
