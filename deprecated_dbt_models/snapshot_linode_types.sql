{{
    config(
        materialized='incremental',
        unique_key=None
    )
}}

select
    *,
    current_date as snapshot_date
from linode.linode_type
