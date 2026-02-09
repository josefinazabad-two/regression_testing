{#- /*
--  Filename: unreferenced_nodes.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Test whether models are referenced or not.
--      if they're not referenced by another object they should
--      be removed from the database

--  Primary Key:
--      node_id should be unique for this test

--  Technical Debt
--      should ingest/persist schemas be included
--      should _REGRESSION database be included?
*/ -#}

{{ config(
    severity="warn",
    store_failures = true,
) }}


with flattened_dependencies as (
    select
        a.node_id
        ,replace(b.value ,'"' ,'') as references
    from {{ ref('defined_models_s01') }} as a
    ,lateral flatten(input => a.depends_on_nodes) as b
)


select
    a.node_id
    ,b.node_id as referenced_by
    ,a.table_schema
    ,a.alias as table_name
    ,a.obj as object_name
    ,hash(a.node_id) as skey_{{ this.name }}
from {{ ref('defined_models_s01') }} as a
left join flattened_dependencies as b on b.references = a.node_id
where
    b.node_id is null
    and a.table_schema not like 'DBT_DM_%'
    {# /* 
    -- maybe should include these in the future - need to consider 
    -- impact as we have users who use these directly as well
     */ #}
    and a.table_schema not like 'DBT_PERSIST_%'
    and a.table_schema not like 'DBT_INGEST_%'
    and a.table_catalog not like '%_REGRESSION'



{#- /* End of File */ #}
