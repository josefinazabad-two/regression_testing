{#- /*
--  Filename: probably_incorrect_type.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      this tets identifies columns that are probably not typed correctly
--      data/time names indicate that the type should be a date or time
--      type. Note that we probably do not need to split date and time
--      columns in snowflake - this is designed as a qlik optimisation and
--      is not certain if it provides any benefit at all in snowflake.

--      flags should be boolean

--      counts should be numbers (should also standardise naming - have seen a few "number_of" columns)

--      This case is designed as a best attempt based on name of column and data type, so
--      there may be false negatives included and false positives excluded.
--      false negatives could be specifically filtered out (or, if appropriate, columns renamed)
--      The general goal of column naming is for consistency to help users with understanding

--  Primary Key:
        table_schema ,table_name ,column_name

--  Technical Debt
--      none known

*/ -#}

{{ config(
    severity="warn",
    store_failures=true,
) }}

with base as (

    select * from (
        select
            table_schema
            ,table_name
            ,column_name
            ,data_type
            ,case
                when column_name like 'FLAG_%' then 'BOOLEAN'
                when column_name like 'COUNT%' then 'NUMBER'
                when column_name like 'NUMBER_%' then 'should rename to COUNT_ and type NUMBER'
                when column_name like '%DATE%' then 'DATE/TIME/TIMESTAMP'
                when column_name like '%LEAD_TIME%' then 'NUMBER'
                else 'tbd'
            end as expected_data_type
        from {{ source('target_info_schema','columns') }}
        union all
        select
            table_schema
            ,table_name
            ,column_name
            ,data_type
            ,case
                when column_name like 'FLAG_%' then 'BOOLEAN'
                when column_name like 'COUNT%' then 'NUMBER'
                when column_name like 'NUMBER_%' then 'should rename to COUNT_ and type NUMBER'
                when column_name like '%DATE%' then 'DATE/TIME/TIMESTAMP'
                when column_name like '%LEAD_TIME%' then 'NUMBER'
                else 'tbd'
            end as expected_data_type
        from {{ source('target_info_schema_core','columns') }}
    ) as unioned_tables

    where (
        column_name like '%DATE%' or column_name like '%TIME%'
        or column_name like 'COUNT%' or column_name like 'NUMBER%'
    )
    and column_name not in ('LAST_UPDATED_BY' ,'LAST_UPDATE_LOGIN')
    and column_name not like 'SKEY_%'
    and column_name not like '%_KEY'
    and data_type not in ('TIMESTAMP_NTZ' ,'TIMESTAMP_LTZ' ,'TIMESTAMP_TZ' ,'DATE')
    and table_schema not in ('INFORMATION_SCHEMA' ,'INFO_HS' ,'PUBLIC' ,'DBT_TEST_REGRESSION' ,'DBT_TEST_AUDIT' ,'DBT_TEST_BIST')
)

select *
from base
where data_type != expected_data_type



{#- /* End of File */ #}
