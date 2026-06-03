{{
    config(
        materialized="table",
        indexes=[
            {'columns': ['linode_id']},
            {'columns': ['reporting_date']}
        ]
    )
}}

with base_data as (
    select * from {{ref('stg_daily_linode_resource_billing')}}
    where cloud_account = 'dev'
)
select * from base_data
