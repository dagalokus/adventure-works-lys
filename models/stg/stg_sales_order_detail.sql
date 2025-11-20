<<<<<<< HEAD
with sales_order as (select * from {{ source("Sales", "sales_order_detail") }})

select *
from sales_order
=======
with sales_order as (select * from {{ source("Sales", "sales_order_detail") }})

select *
from sales_order
>>>>>>> a4ff0ad41e2166f670f8e5056cd28529504530bd
