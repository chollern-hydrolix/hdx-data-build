{% snapshot snapshot_crm__case %}

{{
    config(
        unique_key='case_id',
        strategy='timestamp',
        updated_at='system_modstamp',
        invalidate_hard_deletes=true
    )
}}

select * from {{ref('fct_crm__case')}}

{% endsnapshot %}
