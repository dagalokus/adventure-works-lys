with product as (select * from {{ source("Production", "product") }})

select * from product
