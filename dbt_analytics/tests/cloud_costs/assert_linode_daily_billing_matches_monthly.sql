with d as (
  select linode_id, invoice_month,
         sum(daily_total_amount) as summed_daily
  from analytics.stg_linode_instance_billing_daily
  group by 1, 2
), m as (
  select linode_id, invoice_month, total_amount
  from analytics.stg_linode_instance_billing
)
select d.linode_id, d.invoice_month,
     d.summed_daily, m.total_amount,
     d.summed_daily - m.total_amount as diff
from d
join m using (linode_id, invoice_month)
where abs(d.summed_daily - m.total_amount) > 0.001

{#with daily as (#}
{#    select#}
{#        linode_id,#}
{#        invoice_month,#}
{#        sum(daily_total_amount) as summed_daily_total,#}
{#        sum(daily_hdx_amount) as summed_daily_hdx#}
{#    from {{ ref('stg_linode_instance_billing_daily') }}#}
{#    group by 1, 2#}
{#), monthly as (#}
{#    select#}
{#        linode_id,#}
{#        invoice_month,#}
{#        total_amount as monthly_total,#}
{#        hdx_amount as monthly_hdx#}
{#    from {{ ref('stg_linode_instance_billing') }}#}
{#)#}
{#select#}
{#    d.linode_id,#}
{#    d.invoice_month,#}
{#    d.summed_daily_total,#}
{#    m.monthly_total,#}
{#    d.summed_daily_total - m.monthly_total as total_diff,#}
{#    d.summed_daily_hdx,#}
{#    m.monthly_hdx,#}
{#    d.summed_daily_hdx - m.monthly_hdx as hdx_diff#}
{#from daily d#}
{#full outer join monthly m#}
{#    on d.linode_id = m.linode_id#}
{#   and d.invoice_month = m.invoice_month#}
{#where abs(coalesce(d.summed_daily_total, 0) - coalesce(m.monthly_total, 0)) > 0.01#}
{#   or abs(coalesce(d.summed_daily_hdx, 0) - coalesce(m.monthly_hdx, 0)) > 0.01#}
{#   or d.linode_id is null#}
{#   or m.linode_id is null#}
