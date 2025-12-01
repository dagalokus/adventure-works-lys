with address as (select * from {{ source("Person", "address") }}) select * from address
