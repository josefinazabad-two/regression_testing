{#- /*

Columns added/removed, but only for tables that are in both databases

*/ -#}

with base as (
    select
        cid.*
        ,concat_ws('.' ,cid.table_catalog ,cid.table_schema ,cid.table_name) as table_full_name
    from {{ ref('columns_in_database_s10') }} as cid
)

,base2 as (
    select
        a.table_schema
        ,a.table_name
        ,a.column_name
        ,a.data_type
        ,sum(a.test) as test
        ,sum(a.gold) as gold
        ,max(case when a.test = 1 then a.table_full_name end) as test_table
        ,max(case when a.gold = 1 then a.table_full_name end) as gold_table
    from base as a
    where
        a.table_catalog not ilike '%_STAGE'
        and a.table_schema not in ('INFORMATION_SCHEMA' ,'INFO_HS' ,'PUBLIC' ,'DBT_TEST_REGRESSION' ,'DBT_TEST_AUDIT')
    group by
        a.table_schema
        ,a.table_name
        ,a.column_name
        ,a.data_type
)

select b.*
from base2 as b


{#- /* End of File */ #}
