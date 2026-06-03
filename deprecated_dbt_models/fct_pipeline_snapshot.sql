{{config(materialized="table")}}

with opportunities as (
    select
        date_trunc('month', snapshot_ts)::date as snapshot_month,
        date_trunc('day', snapshot_ts)::date as snapshot_date,
        id as opportunity_id,
        name as opportunity_name,
        account_id as account_id,
        close_date as close_date,
        probability as probability,
        stage_name as stage_name,
        amount as opportunity_amount,
        coalesce(g_m_r_r_gross_2025__c, g_m_r_r__c) as gmrr_gross,
        g_m_r_r_net__c as gmrr_net,
        coalesce(forecast_category, 'N/A') as forecast_category,
        coalesce(hydrolix_product__c, 'N/A') as hydrolix_product,
        coalesce(channel__c, 'N/A') as channel,
        coalesce(region__c, 'N/A') as region,
        coalesce(country__c, 'N/A') as country,
        coalesce(type, 'N/A') as type,
        coalesce(type_calculated__c, 'N/A') as type_calculated,
        term__c as term,
        contract_term__c as contract_term,
        is_closed,
        is_won
    from salesforce.opportunity_snapshot
), snapshot_dates as (
    select
        snapshot_month,
        min(snapshot_date) as min_snapshot_date
    from opportunities
    group by 1
), opportunities_with_min_month as (
    select
        o.*
    from opportunities o
    inner join snapshot_dates s on o.snapshot_date = s.min_snapshot_date
), opportunities_with_account as (
    select
        o.*,
        a.name as account_name
    from opportunities_with_min_month o
    left join salesforce.account a on o.account_id = a.id
)
select * from opportunities_with_account
