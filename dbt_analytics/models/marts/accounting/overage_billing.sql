{{config(materialized="table")}}

with contract_data as (
    select
        a.account_name,
        c.contract_number,
        a.akamai_account_id,
        c.akamai_contract_id,
        c.contract_start_date,
        c.contract_end_date,
        c.commit_amount,
        c.commit_type,
        c.overage_charges,
        ((regexp_match(c.overage_charges, '[0-9]+(?:\.[0-9]+)?'))[1]::numeric) as overage_charges_number,
        case
            when c.commit_type = 'TB per Month' then ((regexp_match(c.overage_charges, '[0-9]+(?:\.[0-9]+)?'))[1]::numeric) / (1024^4)
            else ((regexp_match(c.overage_charges, '[0-9]+(?:\.[0-9]+)?'))[1]::numeric) / 1000000000
        end as overage_commit_normalized,
		-- case
        --     when d.project_name is null then concat(d.clean_cluster_hostname, '-akamai')
        --     else concat(d.clean_cluster_hostname, '-', d.project_name)
        -- end as cluster_project,
        ie_p.cluster_project__c as cluster_project,
        d.salesforce_id as deployment_id,
        m.month_date as reporting_month
    from {{ref('fct_crm__contract')}} c
    left join {{ref('dim_crm__account')}} a on c.account_id = a.account_id
    left join {{ref('fct_crm__deployment')}} d on c.contract_id = d.contract_id
    left join {{ source('raw_salesforce', 'ie_project__c') }} ie_p on d.salesforce_id = ie_p.deployment__c
    inner join {{ref('dim_month')}} m
        on c.contract_start_month <= m.month_date
        and c.contract_end_month >= m.month_date
    where d.deployment_id is not null
    and c.usage_based_billing_confirmed is False
    and c.has_overages is True
    and c.status = 'Activated'
), usage_data as (
    select
        date_trunc('month', date) as reporting_month,
        concat(cluster_hostname, '-', project_name) as cluster_project,
        table_name,
        sum(total_bytes) as total_bytes,
        sum(total_rows) as total_rows
    from {{ source('argus', 'daily_usage_with_table') }}
    where table_name != 'hydro'
    group by 1, 2, 3
), contracts_with_usage as (
    select
        c.*,
        u.table_name,
        u.total_bytes,
        u.total_rows,
        case
            when c.commit_type = 'TB per Month' then u.total_bytes
            else u.total_rows
        end as usage_amount,
        case
            when c.commit_type = 'TB per Month' then c.commit_amount * (1024^4)
            else c.commit_amount * 1000000000
        end as commit_normalized
    from contract_data c
    left join usage_data u
        on c.reporting_month = u.reporting_month
        and c.cluster_project = u.cluster_project
), contract_usage_with_overage as (
    select
        c.*,
        c.commit_normalized - c.usage_amount as overage_balance
    from contracts_with_usage c
    where c.table_name is not null
    and c.total_bytes is not null
)
select * from contract_usage_with_overage
order by 1
