{{
    config(
        materialized="table"
    )
}}

with salesforce_opportunities as (
    select
        o.id as opportunity_id,
        o.name as opportunity_name,
        o.close_date as close_date,
        o.stage_name as stage_name,
        o.probability as probability,
        o.forecast_category as forecast_category,
        o.amount as amount,
        o.type as type,
        o.type_calculated__c as type_calculated,
        o.type_reporting__c as type_reporting,
        o.account_id as account_id,
        coalesce(o.channel__c, 'N/A') as channel,
        coalesce(o.hydrolix_service__c, 'N/A') as hydrolix_product,
        coalesce(o.region__c, 'N/A') as region,
        coalesce(o.country__c, 'N/A') as country,
        o.m_r_r__c as mrr_gross,
        o.m_r_r_net__c as mrr_net,
        o.g_m_r_r_gross_2025__c as gmrr_gross,
        o.l_m_r_r_gross_2025__c as lmrr_gross,
        o.t_c_v_net__c as tcv_net,
        o.a_r_r__c as arr,
        o.n_r_r__c as nrr_gross,
        o.event_n_r_r_gross_2025__c as event_nrr_gross_2025,
        o.owner_id as owner_id,
        o.is_closed as is_closed,
        o.is_won as is_won,
        o.p_o_c_initiated__c as poc_initiated,
        o.p_o_c_initiated__c and o.is_closed is False as is_poc,
        coalesce(o.loss_reason__c, 'N/A') as loss_reason,
        coalesce(o.lead_source, 'N/A') as lead_source,
        coalesce(o.lead_source_details__c, '') as lead_source_details,
        coalesce(o.lead_source_note__c, '') as lead_source_details_other,
        o.created_date as created_date,
        o.last_modified_date as last_modified_date,
        left(o.id, 15) as opportunity_short_id
    from salesforce.opportunity o
    where o.is_deleted is False
)
select * from salesforce_opportunities
