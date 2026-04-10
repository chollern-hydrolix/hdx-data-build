{{ config(materialized="table") }}

WITH months AS (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="CAST('2021-01-01' AS DATE)",
        end_date="CAST('2031-01-01' AS DATE)"
    ) }}
)

SELECT 
    date_month AS month_datetime,
    date_month::date as month_date
FROM months
ORDER BY 1
