{{
    config(
        materialized='view'
    )
}}

with opportunities as (
    select
        o.opportunity_name,
        u.name as owner_name,  -- raw source: no dim_crm__user model exists yet
        o.stage_name,
        o.mrr_gross,
        o.close_date,
        o.probability,
        a.account_name,
        o.opportunity_id,
        o.account_id,
        o.type_reporting,
        row_number() over (
            partition by o.account_id
            order by o.close_date asc
        ) as _rank
    from {{ ref('fct_crm__opportunity') }} o
    left join {{ source('raw_salesforce', 'user') }} u on o.owner_id = u.id
    left join {{ ref('dim_crm__account') }} a on o.account_id = a.account_id
    where o.is_closed is False
      and o.stage_name != 'Omitted'
)
select * from opportunities
where _rank = 1