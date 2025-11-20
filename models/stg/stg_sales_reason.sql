<<<<<<< HEAD
with sales_reason as (select * from {{ source("Sales", "sales_reason") }})

select *
from sales_reason
=======
with sales_reason as (select * from {{ source("Sales", "sales_reason") }})

select *
from sales_reason
>>>>>>> a4ff0ad41e2166f670f8e5056cd28529504530bd
