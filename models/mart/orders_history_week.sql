{{
    config(
        enabled=True
    )
}}

with prepared as (
    select 
        extract(year from order_date) as year,
        extract(week from order_date) as week,
        status
    from {{ref('sat_order_details')}}
)
select *, count(*) as cnt
from prepared
group by year, week, status
order by year, week, status