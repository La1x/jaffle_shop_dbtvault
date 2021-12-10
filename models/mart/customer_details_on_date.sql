{{
    config(
        enabled=True
    )
}}

with pit as (
    select *, row_number() over (partition by customer_pk order by sat1_effective_from desc, sat2_effective_from desc) as row_num
    from {{ref('pit_customer')}}
     where sat1_effective_from <= '{{ var("date_for_pit", "2021-12-06") }}'::timestamp 
        or sat2_effective_from <= '{{ var("date_for_pit", "2021-12-06") }}'::timestamp
)
select 
    pit.customer_pk
    ,pit.customer_key
    
    ,sat1.first_name
    ,sat1.last_name
    ,sat1.email
    ,sat1.effective_from as sat1_effective_from
    ,sat1.record_source  as sat1_record_source
    
    ,sat2.country
    ,sat2.age
    ,sat2.effective_from as sat2_effective_from
    ,sat2.record_source  as sat2_record_source
from pit

left join {{ref('sat_customer_details')}} sat1
on pit.customer_pk = sat1.customer_pk 
and pit.sat1_effective_from = sat1.effective_from
and sat1.effective_from <= '{{ var("date_for_pit", "2021-12-06") }}'::timestamp


left join {{ref('sat_customer_details_crm')}} sat2
on pit.customer_pk = sat2.customer_pk 
and pit.sat2_effective_from = sat2.effective_from
and sat2.effective_from <= '{{ var("date_for_pit", "2021-12-06") }}'::timestamp

where row_num = 1
