{{
    config(
        materialized="table"
    )
}}

with salesforce_accounts as (
    select
        a.name as account_name,
        a.id as account_id,
        a.type,
        coalesce(a.industry, 'N/A') as industry,
        coalesce(a.number_of_employees, -1) as number_of_employees,
        coalesce(a.account_source, 'N/A') as account_source,
        coalesce(a.region__c, 'N/A') as region,
        coalesce(a.vertical__c, 'N/A') as vertical,
        coalesce(a.country__c, 'N/A') as country,
        coalesce(a.revenue_range__c, 'N/A') as revenue_range,
        coalesce(a.primary_industry__c, 'N/A') as primary_industry,
        coalesce(a.primary_sub_industry__c, 'N/A') as primary_sub_industry,
        coalesce(a.akamai_account_i_d__c, 'N/A') as akamai_account_id,
        u1.name as account_owner,
        a.created_date,
        a.last_modified_date
    from salesforce.account a
    left join salesforce.user u1 on a.owner_id = u1.id
    where is_deleted is False
), salesforce_new_business_contracts as (
    select
        account_id,
        min(recognition_date__c) as customer_date
    from salesforce.contract
    where type_calculated__c = 'New Business'
    group by 1
), salesforce_churn_contracts as (
    select
        account_id,
        max(recognition_date__c) as churn_date
    from salesforce.contract
    where type_calculated__c in ('Cancellation', 'Expiration')
    group by 1
), account_with_dates as (
    select
        a.*,
        new_biz.customer_date,
        coalesce(churn.churn_date, make_date(2099, 12, 31)) as churn_date
    from salesforce_accounts a
    left join salesforce_new_business_contracts new_biz on a.account_id = new_biz.account_id
    left join salesforce_churn_contracts churn on a.account_id = churn.account_id
), account_with_months as (
    select
        *,
        date_trunc('month', customer_date)::date as customer_month,
        date_trunc('month', churn_date)::date as churn_month
    from account_with_dates
), account_with_type as (
    select
        *,
        case
            when customer_date is not null then
                case
                    when churn_date > current_date then 'Customer'
                    else 'Churned Customer'
                end
            when type = 'Prospect' then 'Prospect'
            else 'N/A'
        end as account_type
    from account_with_months
)
select * from account_with_type
