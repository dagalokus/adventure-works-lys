with ship_method as (select * from {{ source("Purchasing", "ship_method") }})

select *
from ship_method
