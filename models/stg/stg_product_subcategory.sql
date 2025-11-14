with product_subcategory as (
    select * from {{ source('Production', 'product_subcategory') }}
)
select * 
from product_subcategory
