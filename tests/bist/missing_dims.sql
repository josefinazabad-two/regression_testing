{#- /*
--  Filename: missing_dims.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Identifies skeys in fact tables (within a data model) where the related dimension does not exist in that model.

--  Primary Key:
--      tbd

--  Technical Debt
--      none known
*/ -#}

{{ config(
    severity="warn",
    store_failures = true,
) }}

with data as (
    select distinct
        a.table_schema
        ,a.column_name
        ,b.table_name    -- extract dim name from SKEY
        ,replace(a.column_name ,'SKEY_' ,'') as exp_table_name
    from {{ source('target_info_schema','columns') }} as a
    left join {{ source('target_info_schema','tables') }} as b
        on
            b.table_schema = a.table_schema
            and b.table_name = replace(a.column_name ,'SKEY_' ,'')
    where
        a.table_schema like 'DBT_DM_%'      -- only look in data model schemas
        and a.table_name like 'FACT_%'      -- only look at fact tables
        and a.column_name like 'SKEY_DIM_%' -- only consider foreign keys
        and b.table_name is null            -- only return results where no matching dimension name in the schema
    order by
        a.table_schema
        ,exp_table_name
)

select
    *
    ,hash(*) as skey_{{ this.name }}
from data


{#- /* End of File */ #}
