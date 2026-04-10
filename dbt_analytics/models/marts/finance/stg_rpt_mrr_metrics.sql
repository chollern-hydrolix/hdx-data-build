{{ config(materialized="view") }}

select * from {{ ref('mart_mrr_contracts') }}
where status = 'Activated'
