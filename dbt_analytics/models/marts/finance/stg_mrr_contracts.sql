{{ config(materialized="table") }}

WITH "source" AS (
    SELECT
      "salesforce"."contract"."start_date" AS "start_date",
      "salesforce"."contract"."end_date" AS "end_date",
      "salesforce"."contract"."contract_term" AS "contract_term",
      "salesforce"."contract"."m_r_r_net__c" AS "mrr_net",
      "salesforce"."contract"."m_r_r__c" AS "mrr_gross",
      coalesce(contract_override.gmrr_gross, "salesforce"."contract"."g_m_r_r_gross_2025__c") AS "gmrr_gross",
      coalesce(contract_override.gmrr_net, "salesforce"."contract"."g_m_r_r_net_2025__c") AS "gmrr_net",
      "salesforce"."contract"."l_m_r_r_gross_2025__c" AS "lmrr_gross",
      "salesforce"."contract"."l_m_r_r_net_2025__c" AS "lmrr_net",
      "salesforce"."contract"."a_r_r__c" AS "arr",
      "salesforce"."contract"."closed_date__c" AS "closed_date",
      "salesforce"."contract"."contract_number" AS "contract_number",
      "salesforce"."contract"."t_c_v__c" AS "tcv",
      "salesforce"."contract"."churn_date_reporting__c" AS "churn_date_reporting",
      "salesforce"."contract"."churn_date_reporting_month__c" AS "churn_date_reporting_month",
      "salesforce"."contract"."churn_date_reporting_quarter__c" AS "churn_date_reporting_quarter",
      "salesforce"."contract"."finance_reporting_date__c" AS "finance_reporting_date",
      -- Change type_calculated if override is not null
      coalesce(contract_override.type_calculated, "salesforce"."contract"."type_calculated__c") as "type_calculated",
      "salesforce"."contract"."channel__c" AS "channel",
      "salesforce"."contract"."hydrolix_product__c" AS "hydrolix_product",
      "salesforce"."contract"."region__c" AS "region",
      "salesforce"."contract"."country__c" AS "country",
      "salesforce"."contract"."closed_month__c" AS "closed_month",
      "salesforce"."contract"."status" AS "status",
      "salesforce"."contract"."type__c" AS "type",
      "Account"."name" AS "account_name",
      "Account"."id" AS "account_id",
      "salesforce"."contract"."id" as "contract_id",
      "salesforce"."contract"."recognition_date__c" as "activated_effective_date",
      "salesforce"."contract"."recognition_month__c" as "activated_effective_month",
      date_trunc('month', "salesforce"."contract"."start_date") as "start_month",
      date_trunc('month', "salesforce"."contract"."end_date") as "end_month",
      "salesforce"."contract"."churn_m_r_r_gross_2025__c" as "churn_mrr_gross_2025",
      "salesforce"."contract"."churn_m_r_r_net_2025__c" as "churn_mrr_net_2025",
      "salesforce"."contract"."churn_m_r_r_net__c" as "churn_mrr_net",
      date_trunc('month', salesforce.contract.previous_contract_start_date__c)::date as previous_contract_start_month,
      date_trunc('month', salesforce.contract.previous_contract_end_date__c)::date as previous_contract_end_month,
      salesforce.contract.previous_contract__c as previous_contract_id,
      salesforce.contract.replaced_by_new_contract__c as replaced_by_new_contract,
      salesforce.contract.bridge_renewal__c as is_bridge_renewal,
      salesforce.contract.replaced_by_new_draft_contract__c as replaced_by_draft_contract,
      salesforce.contract.commit_amount__c as commit_amount,
      salesforce.contract.commit_type__c as commit_type,
      salesforce.contract.type_reporting__c as type_reporting,
      salesforce.contract.f_x_impact_m_r_r__c as fx_impact_mrr,
      salesforce.contract.f_x_rate__c as fx_rate,
      salesforce.contract.customer_count__c as customer_count_impact,
      salesforce.contract.churn_confirmed__c as churn_confirmed,
      salesforce.contract.churn_confirmed_date__c as churn_confirmed_date,
      salesforce.contract.opportunity__c as opportunity_id,
    u1.name as sold_by,
    salesforce.contract.contract_active__c as contract_active,
    "Account".industry as industry,
    "Account".primary_industry__c as primary_industry,
    "Account".primary_sub_industry__c as primary_sub_industry,
    salesforce.contract.akamai_sales_rep__c as akamai_sales_rep
    FROM
      "salesforce"."contract"
     
LEFT JOIN "salesforce"."account" AS "Account" ON "salesforce"."contract"."account_id" = "Account"."id"
LEFT JOIN static_mapping.contract_override ON "salesforce"."contract"."id" = contract_override.contract_id
LEFT JOIN "salesforce"."user" as u1 ON "salesforce"."contract"."sold_by__c" = u1.id
   
WHERE
    (
        NOT (
          LOWER("salesforce"."contract"."account_name_text__c") LIKE '%123test%'
        )
    OR (
          "salesforce"."contract"."account_name_text__c" IS NULL
        )
      )

   AND (
        NOT (
          LOWER("salesforce"."contract"."account_name_text__c") LIKE '%456test%'
        )
        OR (
          "salesforce"."contract"."account_name_text__c" IS NULL
        )
      )
    --   AND (
    --     ("salesforce"."contract"."status" = 'Activated')
    --     OR ("salesforce"."contract"."status" = 'Draft')
    --     OR (
    --       "salesforce"."contract"."status" = 'In Approval Process'
    --     )
    --   )
      AND (
        ("salesforce"."contract"."type__c" = 'Cancellation')
        OR ("salesforce"."contract"."type__c" = 'Downgrade')
        OR ("salesforce"."contract"."type__c" = 'Event')
        OR ("salesforce"."contract"."type__c" = 'Expiration')
        OR ("salesforce"."contract"."type__c" = 'Extend')
        OR ("salesforce"."contract"."type__c" = 'New Business')
        OR ("salesforce"."contract"."type__c" = 'Renew')
        OR ("salesforce"."contract"."type__c" = 'Upgrade')
      )
    AND "salesforce"."contract"."is_deleted" is False
)

SELECT * FROM "source"
