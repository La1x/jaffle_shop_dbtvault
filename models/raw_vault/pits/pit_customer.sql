{{
    config(
        enabled=True
    )
}}

with hub as (
    select
        customer_pk
        ,customer_key
    from {{ref('hub_customer')}}
),

sat as (
    select
        customer_pk
        ,effective_from
    from {{ref('sat_customer_details')}}
),

pit as (
    select 
        hub.customer_pk
        ,hub.customer_key
        ,sat.effective_from as sat_effective_from
    from hub
    join sat
    on hub.customer_pk = sat.customer_pk
)

select * from pit
