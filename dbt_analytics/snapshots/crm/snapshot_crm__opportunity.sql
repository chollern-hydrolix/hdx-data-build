{% snapshot snapshot_crm__opportunity %}

{{
    config(
        unique_key='opportunity_id',
        strategy='timestamp',
        updated_at='system_modstamp',
        invalidate_hard_deletes=true
    )
}}

select * from {{ref('fct_crm__opportunity')}}

{% endsnapshot %}
