{% snapshot snapshot_crm__deployment %}

{{
    config(
        unique_key='salesforce_id',
        strategy='timestamp',
        updated_at='system_modstamp',
        invalidate_hard_deletes=true
    )
}}

select * from {{ref('fct_crm__deployment')}}

{% endsnapshot %}
