with ship_method as (select * from {{ ref("int_ship_method") }})

select *
from ship_method
