{{ config(materialized="table") }}

-- Query all shutdown times of linode instances using the event table
WITH
  linode_shutdowns AS (
    SELECT
      e.entity_id AS linode_id,
      e.created
    FROM
      linode.event e
    WHERE
      e.action = 'linode_shutdown'
  ), linode_shutdowns_deduped as (
    select
        linode_id,
        created,
        row_number() over (
            partition by linode_id
            order by created desc
        ) as _rank
    from linode_shutdowns
  ),
  base_query AS (
    SELECT
      c.id AS cluster_id,
      c.created AS cluster_created_date,
      c.label AS cluster_label,
      c.region AS cluster_region,
      li.id AS linode_id,
      li.created AS linode_created_date,
      li.label AS linode_label,
      li.status AS linode_final_status,
      lt.label AS linode_type_label,
      lt.list_price AS linode_type_list_price,
      lt.hdx_price AS linode_type_hdx_price,
      lt.monthly_list_price AS linode_type_monthly_list_price,
      ls.created AS linode_shutdown_date,
      li.cloud_account
    FROM
      linode.cluster c
      -- FULL OUTER JOIN on linode instance to capture all linodes even if not assigned to a cluster
      FULL OUTER JOIN linode.linode_instance li ON c.id = li.lke_cluster_id
      -- LEFT JOIN to get linode pricing
      LEFT JOIN linode.linode_type lt ON li.type = lt.id
      -- LEFT JOIN to get all shutdown times of each linode
      LEFT JOIN linode_shutdowns_deduped ls ON li.id = ls.linode_id
    WHERE (ls._rank = 1 OR ls._rank IS NULL)
  )
select * from base_query
