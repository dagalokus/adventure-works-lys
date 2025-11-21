with
    address as (
        select
            a.address_id,
            a.address_line1,
            a.address_line2,
            a.city,
            a.state_province_id,
            a.postal_code,
            a.spatial_location,
            s.state_province_code,
            s.country_region_code,
            s.name as state_name,
            s.territory_id,
            st.name as territory_name,
            st.group,
            st.sales_ytd,
            st.sales_last_year,
            st.cost_ytd,
            st.cost_last_year

        from {{ ref("stg_address") }} as a
        left join
            {{ ref("stg_state_province") }} as s
            on a.state_province_id = s.state_province_id
        left join
            {{ ref("stg_sales_territory") }} as st on s.territory_id = st.territory_id
    )

select *
from address
