{{ config(materialized="table") }}

with poc_data as (
    select
        customer_name,
        approval_amount,
        status,
        credit_amount,
        poc_month,
        requested_date,
        start_date,
        end_date,
        hdx_deployment_id,
        invoice_number,
        date_applied,
        hydrolix_notes,
        akamai_account_id,
        taskengine_last_sync_ts,
        row_number
    from google_sheets.hydrolix_poc
)
select * from poc_data
