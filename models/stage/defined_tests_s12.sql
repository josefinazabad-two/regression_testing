{#- /*
--  Filename: defined_tests_s01.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      get list of tests defined in dbt to compare against
--      expected test cases to identify missing cases that should be defined.

--  Primary Key:
--      node_id

--  Technical Debt
--      none known
*/ -#}

{{ config(
    materialized="view",
    tags=["must_be_view"],
) }}


with defined_tests as (
    select * from {{ ref('defined_tests_s01') }}
)

,nodes as (
    select *
    from {{ ref('defined_models_s01') }}
)

select
    n.node_id
    ,n.table_catalog
    ,n.table_schema
    ,n.alias as table_name
    ,a.base_node
    ,a.column_name
    ,a.node_foreign
    ,a.foreign_column_name
    ,a.test_case
    ,a.count
    ,n.model_path
    ,nf.model_path as model_path_foreign
    ,nf.table_catalog as table_catalog_foreign
    ,nf.table_schema as table_schema_foreign
    ,nf.alias as table_name_foreign
    ,true as test_exists
    ,concat_ws('.' ,n.table_schema ,n.alias) as obj
    ,concat_ws('.' ,n.table_catalog ,n.table_schema ,n.alias) as object_full_name
    ,concat_ws('.' ,nf.table_catalog ,nf.table_schema ,nf.alias) as object_full_name_foreign
    ,case when ft.table_catalog is not null then true end as object_foreign_exists
from defined_tests as a
left join nodes as n on a.base_node = n.node_id
left join nodes as nf on a.node_foreign = nf.node_id
left join
    {{ source('target_info_schema','tables') }} as ft
    on
        concat_ws('.' ,ft.table_catalog ,ft.table_schema ,ft.table_name)
        = concat_ws('.' ,nf.table_catalog ,nf.table_schema ,nf.alias)



{#- /* End of File */ #}
