# snapshot_crm__lead

## Details

| | |
|---|---|
| **Schema** | `analytics` |
| **Materialization** | snapshot |
| **Strategy** | timestamp |
| **Unique Key** | `lead_id` |
| **Updated At** | `system_modstamp` |

## Depends On

- `fct_crm__lead`

## Columns

| Column | Type | Description |
|---|---|---|
| `lead_id` | character varying(18) |  |
| `name` | text |  |
| `title` | text |  |
| `company` | text |  |
| `lead_record_type` | text |  |
| `status` | text |  |
| `lead_owner` | text |  |
| `region` | text |  |
| `channel` | text |  |
| `lead_source` | text |  |
| `lead_source_details` | text |  |
| `lead_source_note` | text |  |
| `industry` | text |  |
| `sub_industry` | text |  |
| `last_activity_date` | date |  |
| `bdr_qualified_by` | text |  |
| `bdr_notes` | text |  |
| `duration_in_prospect` | numeric |  |
| `duration_in_mql` | numeric |  |
| `duration_in_sql` | numeric |  |
| `entered_prospect` | timestamp |  |
| `entered_converted` | timestamp |  |
| `entered_disqualified` | timestamp |  |
| `entered_nurture` | timestamp |  |
| `entered_mql` | timestamp |  |
| `entered_sql` | timestamp |  |
| `created_date` | timestamp |  |
| `created_by` | text |  |
| `last_modified_date` | timestamp |  |
| `lead_short_id` | text |  |
| `system_modstamp` | timestamp |  |
| `dbt_scd_id` | text |  |
| `dbt_updated_at` | timestamp |  |
| `dbt_valid_from` | timestamp |  |
| `dbt_valid_to` | timestamp |  |
