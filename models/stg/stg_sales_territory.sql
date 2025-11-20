with sales_territory as (select * from {{ source("Sales", "sales_territory") }})

select *
from sales_territory
