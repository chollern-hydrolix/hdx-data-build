{{ config(materialized="table") }}

WITH days AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2022-01-01' AS DATE)",
        end_date="CAST('2031-01-01' AS DATE)"
    ) }}
)
SELECT 
    date_day AS day_datetime,
    date_day::date as day_date,
    date_trunc('month', date_day)::date as month_date,
    extract(DOW from date_day) as day_of_week
FROM days
ORDER BY 1
