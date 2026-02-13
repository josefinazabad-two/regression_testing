{#- /*
--  Filename: source_freshness.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Tests for source freshness to identify if reporting replica is within SLA
--
--      This test is only for warning level, any rows returned indicates
--      warning

--  Primary Key:
--      db_object

--  Technical Debt
--      this would preferrably be done as a part of source freshness testing
--      within standard dbt framework - however it is difficult to capture the
--      results of that into our testing logging. Note that I've not checked
--      if the dbt artifacts framework shows the results of source freshness.
*/ -#}

{{ config(
    severity="warn",
    store_failures = true,
) }}

select
    db_object
    ,test_result
    ,latest_change
    ,warn_if_before
from {{ ref('source_freshness_s10') }}
where test_result = 'WARN'

{#- /* End of File */ #}
