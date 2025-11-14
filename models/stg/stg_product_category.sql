with product_category as (select * from {{ source("Production", "product_category") }})

select *
from product_category
