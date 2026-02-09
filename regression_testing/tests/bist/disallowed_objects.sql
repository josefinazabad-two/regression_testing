{#- /*
--  Filename: disallowed_columns.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Identifies object names that are typically not allowed in warehouse
--      or data model layers - these should only be DIM, FACT or BRIDGE

--  Primary Key:
--      primary key made up of database, schema, table

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
        ,hash(table_schema ,table_name) as skey_{{ this.name }}
        ,1 as _include_regression
    from {{ source('target_info_schema','tables') }}
    union all
    select
        table_schema
        ,table_name
        ,hash(table_schema ,table_name) as skey_{{ this.name }}
        ,1 as _include_regression
    from {{ source('target_info_schema_core','tables') }}
) as unioned_tables

where
    (
        table_schema = 'DBT'
        or (
            table_schema not in ('INFORMATION_SCHEMA' ,'INFO_HS' ,'PUBLIC' ,'DBT_TEST_REGRESSION' ,'DBT_TEST_AUDIT' ,'DBT_TEST_BIST')
        )
        and table_schema not in ('DBT_DM_ADMIN')
        and (
            split_part(table_name ,'_' ,1) not in ('DIM' ,'FACT' ,'BRIDGE' ,'ISLAND')
        )
        and table_name not in ('LAST_LOAD_COMPLETE')
    )
order by
    table_schema
    ,table_name



{#- /* End of File */ #}
