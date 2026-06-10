{% macro salesforce_name_to_table(column) %}
    /*
    Converts a Salesforce object API name column to its corresponding PostgreSQL
    table name in the raw_salesforce schema.

    Equivalent to the Python logic:
        def to_snake_case(text):
            parts = text.split('__')
            return '__'.join(
                re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', part).lower()
                for part in parts
            )

    The split-on-'__' + per-part approach is equivalent to applying the regex
    to the full string directly, because any uppercase letter following '__' is
    always preceded by '_' (not in [a-z0-9]), so the regex never fires across
    the '__' separators.

    Examples:
        Contract              -> contract
        Opportunity           -> opportunity
        CampaignMember        -> campaign_member
        OpportunityTeamMember -> opportunity_team_member
        IE_Cluster__c         -> ie_cluster__c
        IE_BucketCost__c      -> ie_bucket_cost__c
    */
    lower(regexp_replace({{ column }}, '([a-z0-9])([A-Z])', '\1_\2', 'g'))
{% endmacro %}
