{{ config(
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
            relation=ref('mart_contract_expiration'),
            remove=[
                'is_up_for_renewal', 'is_renewed', 'is_bridge_renewal', 'reporting_effective_month', 'reporting_end_month', 'churn_date_reporting_month', 'churn_confirmed', 'churn_confirmed_date', 'type_reporting', 'account_owner',
                'sold_by', 'primary_industry', 'primary_sub_industry', 'next_opp_close_date', 'next_opp_mrr_gross', 'next_opp_owner_name', 'next_opp_probability', 'next_opp_stage_name', 'next_opp_type_reporting', 'has_next_opportunity',
                'commit_type', 'commit_amount', 'total_bytes', 'total_rows', 'cumulative_bytes', 'cumulative_rows', 'max_qpm', 'cumulative_max_qpm', 'should_remove', 'total_queries'
            ],
            exclude=[
                'account_name', 'start_month', 'end_month', 'mrr_gross', 'type', 'type_calculated', 'activated_effective_month', 'reporting_month', 'account_id', 'contract_id', 'contract_number',
                'replaced_by_new_contract', 'renewal_contract_id', 'renewal_contract_number', 'renewal_mrr_gross', 'renewal_type', 'replaced_by_draft_contract', 'is_event', 'renewal_category',
                'channel', 'region', 'country', 'hydrolix_product'
            ],
            field_name='metric',
            value_name='value',
            cast_to='float'
        )
    }}
),
-- Add report labels
table_with_labels as (
    select
        u.*,
        m.label_name as label,
        concat(m.sort_order::text, '. ', m.label_name) as label_with_sort
    from unpivot_table u
    left join static_mapping.mrr_metric_label m on u.metric = m.metric_name
    where u.value != 0
    order by m.sort_order
)
select * from table_with_labels
