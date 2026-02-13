{#- /*
--  Filename: source_freshness_s10.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Identify how fresh data is in key tables and include
--      definition for warning/error level notifications

--      implements an extra grace hours before warning/error
--      when given that input (structured this way to support
--      unit testing).

--  Primary Key:
--      Write some information about the primary key for this table

--  Technical Debt
--      none known

--  History
--      2024-03-11: churchj2: Initial Version
*/ -#}


select
    db_object
    ,latest_change
    ,warn_if_before
    ,error_if_before
    ,case
        when latest_change < error_if_before then 'ERROR'
        when latest_change < warn_if_before then 'WARN'
    end as test_result
from {{ ref('source_freshness_s05') }}


{#- /* End of File */ #}
