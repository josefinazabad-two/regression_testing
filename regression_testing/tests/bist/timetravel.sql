{#- /*
--  Filename: timetravel.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Tests for data retention period on specific objects
--      Tables in ingest/persist schemas should have retention
--      periods that allow us to check old versions of data for
--      changes within some reasonable window.

--      during Dec & Jan (due to year end shutdown) this period is set to 60 days
--      during the rest of the year the period is set to 14 days
--      consider increasing to 60 days all year round to reduce the effort
--      associated wit this.

--  Primary Key:
--      TABLE_CATALOGUE, TABLE_SCHEMA, TABLE_NAME

--  Technical Debt
--      none known

--  History
--      2023-12-14: churchj2: Initial Version
*/ -#}

{{ config(
    severity="warn",
    store_failures = true,
) }}

{# /*
-- Month numbers where we do longer period for retention, note that there
-- must be at least 2 entries in the list, otherwise the sql fails due
-- to the way that jinja expands in the sql
*/ #}
{% set month_numbers_extended='1,12' %}
{% set standard_retention_period=14 %}
{% set extended_retention_period=60 %}

{# /* Get the retention period based on current month */ #}
with retention_period1 as (
    select
        case
            when month(current_date) in ({{ month_numbers_extended }}) then {{ extended_retention_period }}
            else {{ standard_retention_period }}
        end as retention_period
    from dual
)

{# /* command to prompt user with to respond to this error along with comment in the resultant table */ #}
,retention_period as (
    select
        rp.*
        ,'Set to longer retention for Dec/Jan due to Christmas shutdown, otherwise shorter period' as retention_comment
        ,concat('use role accountadmin; ALTER ACCOUNT SET MIN_DATA_RETENTION_TIME_IN_DAYS = ' ,to_char(rp.retention_period) ,'; SHOW PARAMETERS like ''%DATA_RETENTION_TIME_IN_DAYS%'' in account;') as cmd
    from retention_period1 as rp
)

select
    t.table_schema
    ,t.table_name
    ,t.table_type
    ,t.is_transient
    ,t.retention_time
    ,rp.retention_comment
    ,case
        when t.is_transient = 'YES'
            then concat('drop table if exists ' ,t.table_schema ,'.' ,t.table_name ,';')
        else rp.cmd
    end as cmd
    ,hash(t.table_schema ,t.table_name) as skey_{{ this.name }}
from {{ source('target_info_schema_land','tables') }} as t
left join retention_period as rp
where
    (
        t.table_schema like 'DBT_PERSIST%' or t.table_schema like 'DBT_INGEST%'
    ) and (
        t.table_schema not like '%_UAT'
        and t.table_schema not in ('DBT_INGEST_DBT_ARTIFACTS')
    ) and (
        t.is_transient = 'YES'
        or t.retention_time != rp.retention_period
    )



{#- /* End of File */ #}
