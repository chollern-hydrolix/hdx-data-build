{{ config(materialized="table") }}

/*
Necessary for backwards compatibility-- Keizan uses this table
*/

select * from {{ref('fct_crm__deployment')}}
