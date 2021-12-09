{{
    config(
        enabled=True
    )
}}

with pit as (
    select *, row_number() over (partition by customer_pk order by sat_effective_from desc) as row_num
    from {{ref('pit_customer')}}
)

select sat1.*

from pit
left join {{ref('sat_customer_details')}} sat1
on  pit.customer_pk        = sat1.customer_pk 
and pit.sat_effective_from = sat1.effective_from

where pit.row_num = 1
and pit.sat_effective_from <= '{{ var("date_for_pit", "2021-12-06") }}'::timestamp
