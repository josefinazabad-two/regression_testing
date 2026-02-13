{#- /*
--  Filename: source_freshness_s10.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Consolidates data from tables to test for source freshness
--      implements warning/error times for each table and
--      grace hours to support extended grace period before alerts
--      on weekends. The calculate for extended grace is implemented
--      in next layer to support unit testing

--      extended grace is because we find lower activity on weekends
--      which leads to additional unnecessary warnings & errors

--  Primary Key:
--      Write some information about the primary key for this table

--  Technical Debt
--      none known

--  History
--      2024-03-11: churchj2: Initial Version
*/ -#}

{% set grace_hours=-12 %}

with base as (
    select
        max(qr_change_timestamp) as latest_change
        ,dateadd(hour ,-2 ,current_timestamp) as warn_if_before
        ,dateadd(hour ,-6 ,current_timestamp) as error_if_before
        ,'{{ source("Oracle_eBS_FPIM","MTL_MATERIAL_TRANSACTIONS") }}' as db_object
    from {{ source('Oracle_eBS_FPIM','MTL_MATERIAL_TRANSACTIONS') }}
    union all
    select
        max(qr_change_timestamp) as latest_change
        ,dateadd(hour ,-2 ,current_timestamp) as warn_if_before
        ,dateadd(hour ,-6 ,current_timestamp) as error_if_before
        ,'{{ source("Oracle_eBS_FPIM","PO_DISTRIBUTIONS_ALL") }}' as db_object
    from {{ source('Oracle_eBS_FPIM','PO_DISTRIBUTIONS_ALL') }}
    union all
    select
        max(qr_change_timestamp) as latest_change
        ,dateadd(hour ,-2 ,current_timestamp) as warn_if_before
        ,dateadd(hour ,-6 ,current_timestamp) as error_if_before
        ,'{{ source("Oracle_eBS_FPIM","AP_INVOICE_DISTRIBUTIONS_ALL") }}' as db_object
    from {{ source('Oracle_eBS_FPIM','AP_INVOICE_DISTRIBUTIONS_ALL') }}
    union all
    select
        max(qr_change_timestamp) as latest_change
        ,dateadd(hour ,-2 ,current_timestamp) as warn_if_before
        ,dateadd(hour ,-6 ,current_timestamp) as error_if_before
        ,'{{ source("Oracle_eBS_FPIM","MTL_ONHAND_QUANTITIES_DETAIL") }}' as db_object
    from {{ source('Oracle_eBS_FPIM','MTL_ONHAND_QUANTITIES_DETAIL') }}
    union all
    select
        max(qr_change_timestamp) as latest_change
        ,dateadd(minute ,-15 ,current_timestamp) as warn_if_before
        ,dateadd(hour ,-3 ,current_timestamp) as error_if_before
        ,'{{ source("Oracle_eBS_FPIM","FND_CONCURRENT_REQUESTS") }}' as db_object
    from {{ source('Oracle_eBS_FPIM','FND_CONCURRENT_REQUESTS') }}
)

,pass2 as (
    select
        convert_timezone('UTC', 'Pacific/Auckland', latest_change) as latest_change
        ,warn_if_before
        ,error_if_before
        ,db_object
        ,case
            when extract(dayofweek ,current_timestamp) in (6 ,0 ,1) then {{ grace_hours }}
            else 0
        end as grace_hours
    from base
)

select
    db_object
    ,latest_change
    ,dateadd(hour ,grace_hours ,warn_if_before) as warn_if_before
    ,dateadd(hour ,grace_hours ,error_if_before) as error_if_before
from pass2


{#- /* End of File */ #}
