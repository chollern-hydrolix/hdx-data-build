{{
    config(
        materialized="view"
    )
}}


with opportunities as (
    select
        o.opportunity_name,
        u1.name as owner_name,
        o.stage_name,
        o.mrr_gross,
        o.close_date,
        o.probability,
        a.account_name as account_name,
        o.opportunity_id as opportunity_id,
        o.account_id,
        o.type_reporting,
        row_number() over (
            partition by o.account_id
            order by o.close_date asc
        ) as _rank
    from {{ref('fct_opportunity')}} o
    left join salesforce.user u1 on o.owner_id = u1.id
    left join {{ref('dim_account')}} a on o.account_id = a.account_id
    where o.is_closed is False
    and o.stage_name != 'Omitted'
)
select * from opportunities
where _rank = 1
