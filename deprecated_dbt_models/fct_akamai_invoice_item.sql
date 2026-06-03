{{
    config(
        materialized="table",
        indexes=[
            {'columns': ['invoice_month']}
        ]
    )
}}

with linode_instances as (
    select
        invoice_month,
        linode_label,
        billing_interval_start_timestamp,
        billing_interval_end_timestamp,
        ceil(billable_hours) as quantity,
        cluster_region,
        linode_type_list_price as list_price,
        total_amount,
        linode_type_label,
        linode_id,
        concat(linode_type_label, ' - ', linode_label) as invoice_description
    from {{ ref('stg_linode_instance_billing') }}
)
select * from linode_instances
