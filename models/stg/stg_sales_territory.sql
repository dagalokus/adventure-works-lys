<<<<<<< HEAD
with sales_territory as (select * from {{ source("Sales", "sales_territory") }})

select *
from sales_territory
=======
with sales_territory as (select * from {{ source("Sales", "sales_territory") }})

select *
from sales_territory
>>>>>>> a4ff0ad41e2166f670f8e5056cd28529504530bd
