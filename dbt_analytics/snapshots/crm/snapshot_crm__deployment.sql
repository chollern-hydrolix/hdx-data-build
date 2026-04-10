{% snapshot snapshot_crm__deployment %}

{{
    config(
        unique_key='salesforce_id',
        strategy='timestamp',
        updated_at='system_modstamp'
    )
}}

select * from {{ref('fct_crm__deployment')}}

{% endsnapshot %}
