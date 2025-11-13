with dim_date as (
    select * from {{ ref('stg_date_table') }}
)

select * from dim_date