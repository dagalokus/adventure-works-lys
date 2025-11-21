with ship_method as (select * from {{ ref("stg_ship_method") }})

select *
from ship_method
