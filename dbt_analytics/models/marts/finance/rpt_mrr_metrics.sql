{{
    config(
        materialized="table",
        indexes=[
            {'columns': ['metric']},
            {'columns': ['reporting_month']}
        ]
    )
}}

with unpivot_table as (
    {{
        dbt_utils.unpivot(
            relation=ref('stg_rpt_mrr_metrics'),
            remove=[
                'mrr_gross', 'mrr_net', 'gmrr_gross', 'gmrr_net', 'lmrr_gross', 'lmrr_net', 'churned_mrr_gross', 'churned_mrr_net', 'start_month', 'end_month', 'reporting_effective_month',
                'previous_contract_start_month', 'previous_contract_end_month', 'previous_contract_type_calculated', 'contract_overlaps_previous', 'status', 'replacement_start_month', 'replacement_end_month',
                'contract_start_date', 'contract_end_date', 'contract_term_months', 'commit_amount', 'commit_type', 'mrr_band', 'usage_band', 'fx_rate', 'fx_impact_mrr', 'lead_source', 'activated_effective_date',
                'primary_industry', 'primary_sub_industry', 'akamai_sales_rep', 'sold_by', 'contract_active', 'lead_source_details', 'lead_source_details_other', 'opportunity_id', 'reporting_quarter', 'reporting_quarter_label',
                'total_bytes', 'total_rows', 'cumulative_bytes', 'cumulative_rows', 'max_qpm', 'cumulative_max_qpm', 'nrr_gross', 'total_queries'
            ],
            exclude=['account_name', 'type', 'type_calculated', 'type_reporting', 'channel', 'region', 'country', 'hydrolix_product', 'activated_effective_month', 'churn_month', 'reporting_month', 'account_id', 'contract_id', 'contract_number'],
            field_name='metric',
            value_name='value',
            cast_to='float'
        )
    }}
), unpivot_expiration_risk as (
    {{
        dbt_utils.unpivot(
            relation=ref('fct_expiration_risk_contract'),
            remove=['mrr_gross', 'mrr_net', 'gmrr_gross', 'gmrr_net', 'lmrr_gross', 'lmrr_net', 'start_date', 'end_date', 'status', 'start_month', 'end_month', 'expiration_month', 'is_expiration_risk'],
            exclude=['account_name', 'type', 'type_calculated', 'type_reporting', 'channel', 'region', 'country', 'hydrolix_product', 'activated_effective_month', 'churn_month', 'reporting_month', 'account_id', 'contract_id', 'contract_number'],
            field_name='metric',
            value_name='value',
            cast_to='float'
        )
    }}
), unpivot_table_with_expiration_risk as (
    select * from unpivot_table
        union all
    select * from unpivot_expiration_risk
),
-- Add report labels
table_with_labels as (
    select
        u.*,
        m.label_name as label,
        concat(m.sort_order::text, '. ', m.label_name) as label_with_sort
    from unpivot_table_with_expiration_risk u
    left join static_mapping.mrr_metric_label m on u.metric = m.metric_name
    where u.value != 0
    order by m.sort_order
)
select * from table_with_labels
