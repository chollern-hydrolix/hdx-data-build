{% snapshot snapshot_crm__lead %}

{{
    config(
        unique_key='lead_id',
        strategy='timestamp',
        updated_at='system_modstamp',
        invalidate_hard_deletes=true
    )
}}

select * from {{ref('fct_crm__lead')}}

{% endsnapshot %}
