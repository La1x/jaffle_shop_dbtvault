{{
    config(
        enabled=True,
        materialized='view'
    )
}}

with hub as (
    select
        customer_pk
        ,customer_key
    from {{ref('hub_customer')}}
),

sat1 as (
    select
        customer_pk
        ,effective_from
    from {{ref('sat_customer_details')}}
),

sat2 as (
    select
        customer_pk
        ,effective_from
    from {{ref('sat_customer_details_crm')}}
),

pit as (
    select 
        hub.customer_pk
        ,hub.customer_key
        ,sat1.effective_from as sat1_effective_from
        ,sat2.effective_from as sat2_effective_from
    from hub
    full join sat1
    on hub.customer_pk = sat1.customer_pk
    full join sat2
    on hub.customer_pk = sat2.customer_pk
)

select * from pit
