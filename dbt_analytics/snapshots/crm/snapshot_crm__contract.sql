{% snapshot snapshot_crm__contract %}

{{
    config(
        unique_key='contract_id',
        strategy='timestamp',
        updated_at='system_modstamp',
        invalidate_hard_deletes=true
    )
}}

select * from {{ref('fct_crm__contract')}}

{% endsnapshot %}
