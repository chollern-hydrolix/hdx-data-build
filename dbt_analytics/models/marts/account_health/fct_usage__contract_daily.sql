{{
    config(
        materialized='incremental',
        unique_key=['contract_id', 'deployment_sfid', 'reporting_date'],
        incremental_strategy='delete+insert',
        indexes=[
            {'columns': ['reporting_date']},
            {'columns': ['contract_id']},
            {'columns': ['account_id']},
            {'columns': ['deployment_sfid']}
        ]
    )
}}

/*
===============================================================================
Model:
    fct_usage__contract_daily

Grain:
    One row per (contract_id, deployment_sfid, reporting_date)

Description:
    Daily usage enriched with the contract and account that were commercially
    active for each deployment on each day. Contract alignment is time-bound via
    fct_crm__contract_deployment_history, so usage is always attributed to the
    correct contract even across renewals or mid-term replacements.

Metrics:
    - total_bytes / total_rows  (daily ingest)
    - max_qpm                   (daily peak query performance)
    - total_queries             (daily query count)
    - total_gb/gib/tb/tib       (byte conversions for common reporting units)
    - total_billion_rows        (row conversion for Billion records commit types)
===============================================================================
*/

with usage as (
    select * from {{ ref('fct_usage__deployment_daily') }}
    {% if is_incremental() %}
    where reporting_date > (select coalesce(max(reporting_date), '1900-01-01'::date) from {{ this }})
    {% endif %}
)
select
    a.account_name,
    a.parent_account_name,
    c.contract_number,
    u.reporting_date,
    date_trunc('month', u.reporting_date)::date as reporting_month,
    c.contract_start_date,
    c.contract_end_date,
    c.status,
    c.type_reporting,
    c.hydrolix_product,
    c.region,
    u.contract_id,
    u.salesforce_id as deployment_sfid,
    u.deployment_id as deployment_ulid,
    u.account_id,
    c.commit_amount,
    c.commit_type,
    u.total_bytes,
    u.total_bytes / (1000.0 ^ 3) as total_gb,
    u.total_bytes / (1024.0 ^ 3) as total_gib,
    u.total_bytes / (1000.0 ^ 4) as total_tb,
    u.total_bytes / (1024.0 ^ 4) as total_tib,
    u.total_rows,
    u.total_rows / 1000000000.0 as total_billion_rows,
    u.max_qpm,
    u.total_queries
from usage u
left join {{ ref('fct_crm__contract') }} c  on u.contract_id = c.contract_id
left join {{ ref('dim_crm__account') }} a   on u.account_id = a.account_id
