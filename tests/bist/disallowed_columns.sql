{#- /*
--  Filename: disallowed_columns.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Identifies column names that are typically not allowed in certain places - for example _include_regression should be in 
--      warehouse layer, but not in presentation layer.

--  Primary Key:
--      primary key made up of database, schema, table and column name

--  Technical Debt
--      this should turn into an error severity in future
--      only being left as warn for introduction. Once the
--      result in ACC environment is clean/near clean it should
--      become error

*/ -#}

{{ config(
    severity="warn",
    store_failures = true,
) }}

select * from (
    select
        table_schema
        ,table_name
        ,column_name
        ,data_type
        ,case
            when column_name like '%PASSWORD%'
                then 'No business value in warehouse containing password information.'
            when column_name in ('_INCLUDE_REGRESSION' ,'SSID')
                then 'For warehouse testing purposes only.'
            when column_name like 'BKEY%'
                then 'For ingest layer purposes only.'
            when column_name like 'SKEY%' and column_name != 'SKEY_' || table_name
                then 'Foreign keys only allowed on fact/bridge tables.'
            when data_type like 'TIMESTAMP%'
                then 'Ensure consistency of timestamp storage'
            when data_type = 'TEXT'
                then 'Text attributes not allowed on FACT/BRIDGE tables - these belong on DIMs'
            else 'reason not given'
        end as reason
        ,'PRESENT' as db_name
        ,hash(table_schema ,table_name ,column_name) as skey_{{ this.name }}
        ,1 as _include_regression
    from {{ source('target_info_schema','columns') }}
    union all
    select
        table_schema
        ,table_name
        ,column_name
        ,data_type
        ,case
            when column_name like '%PASSWORD%'
                then 'No business value in warehouse containing password information.'
            when column_name in ('_INCLUDE_REGRESSION' ,'SSID')
                then 'For warehouse testing purposes only.'
            when column_name like 'BKEY%'
                then 'For ingest layer purposes only.'
            when column_name like 'SKEY%' and column_name != 'SKEY_' || table_name
                then 'Foreign keys only allowed on fact/bridge tables.'
            when data_type like 'TIMESTAMP%'
                then 'Ensure consistency of timestamp storage'
            when data_type = 'TEXT'
                then 'Text attributes not allowed on FACT/BRIDGE tables - these belong on DIMs'
            else 'reason not given'
        end as reason
        ,'CORE' as db_name
        ,hash(table_schema ,table_name ,column_name) as skey_{{ this.name }}
        ,1 as _include_regression
    from {{ source('target_info_schema_core','columns') }}
) as unioned_tables

where
    table_schema not in ('INFORMATION_SCHEMA' ,'INFO_HS' ,'PUBLIC' ,'DBT_TEST_REGRESSION' ,'DBT_TEST_AUDIT' ,'DBT_TEST_BIST', 'ROW_COMPARISON')
    and (
{# /*
-- Data Models are not allow to contain these columns as the columns are intended for
-- internal testing purposes only.
*/ #}
    (
        column_name in ('_INCLUDE_REGRESSION' ,'SSID')
        and db_name = 'PRESENT'
    )
{# /*
-- foreign keys are only allowed on fact/bridge tables - for simplicity
-- we do not support outrigger tables
*/ #}
    or (
        ((column_name like 'SKEY_%' and column_name != 'SKEY_' || table_name)
        or (column_name like '%_KEY' and column_name != table_name || '_KEY'))
        and split_part(table_name, '_', 1) not in ('FACT', 'BRIDGE', 'REP')
    )
    
{# /*
-- BKEY is only allowed on ingest type tables and should be removed elsewhere
-- this column is only used to make it easier to test ingest schemas and ensure
-- that intended testing is in place (note we don't actually do this yet)
*/ #}
    or (
        column_name like '%BKEY%'
    )

{# /*
-- There is no agreed reason to include any password information in the warehouse
-- during translation project will allow this in ingest tables to avoid
-- breakage of qlik extract/load process
*/ #}
    or (
        column_name like '%PASSWORD%'
    )
{# /*
-- No special characters allowed in columns names that would require quoting
-- restricted to uppercase alphanumeric and underscores (i.e. strings where
-- quotes are not necessary)
*/ #}
    or (
        regexp_replace(column_name,'[A-Z0-9_]*','') != ''
    )

{# /*
-- FACTs may not have text attributes on them - these all belong on dim tables

    or (
        table_schema not in ('INFORMATION_SCHEMA' ,'INFO_HS' ,'PUBLIC' ,'DBT_TEST_REGRESSION' ,'DBT_TEST_AUDIT' ,'DBT_TEST_BIST')
        and (table_name like 'FACT_%' or table_name like 'BRIDGE_%')
        and (
            data_type in ('TEXT')
        )
    )
*/ #}
{# /*
-- In future will only allow TIMESTAMP_LTZ once we get to data models/warehouse
-- the purpose is to ensure consistency of timestamp approach
*/ #}
    or (
        data_type in ('TIMESTAMP_TZ','TIMESTAMP_NTZ')
    )
    )

order by
    table_schema
    ,table_name
    ,column_name



{#- /* End of File */ #}
