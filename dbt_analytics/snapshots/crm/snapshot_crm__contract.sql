{% snapshot snapshot_crm__contract %}

{{
    config(
        unique_key='contract_id',
        strategy='timestamp',
        updated_at='system_modstamp'
    )
}}

select * from {{ref('fct_crm__contract')}}

{% endsnapshot %}
