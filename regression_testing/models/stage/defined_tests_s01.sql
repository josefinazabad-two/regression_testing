{#- /*
--  Filename: defined_tests_s01.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      gets records of test defined by dbt from the last upload

--  Primary Key:
--      node_id

--  Technical Debt
--      none known
*/ -#}

{{ config(
    tags=["must_be_view"]
) }}

with tab1 as (

    select
        all_results
        ,node_id
        ,replace(all_results:attached_node ,'"' ,'') as base_node
        ,upper(replace(all_results:test_metadata:kwargs:field ,'"' ,'')) as foreign_column_name
        ,upper(replace(all_results:test_metadata:kwargs:column_name ,'"' ,'')) as column_name
        ,all_results:depends_on:nodes as nodes
        ,all_results:refs as refs
        ,replace(all_results:test_metadata:name ,'"' ,'') as test_case
    from {{ ref('dbt_artifacts_tests_s01') }}
    where run_started_at in (select max(run_started_at) from {{ ref('dbt_artifacts_tests_s01') }})
)

,tab2 as (
    select
        a.base_node
        ,a.column_name
        ,a.foreign_column_name
        ,a.nodes
        ,a.test_case
        ,a.node_id
        ,replace(min(b.value) ,'"' ,'') as node_min
        ,replace(max(b.value) ,'"' ,'') as node_max
        ,count(*) as count
    from tab1 as a
    ,lateral flatten(input => a.nodes) as b
    group by
        a.base_node
        ,a.column_name
        ,a.foreign_column_name
        ,a.nodes
        ,a.test_case
        ,a.node_id
)

select
    a.base_node
    ,a.column_name
    ,a.foreign_column_name
    ,a.nodes
    ,a.test_case
    ,a.node_id
    ,a.count
    ,coalesce(decode(a.node_min ,a.node_id ,null ,a.node_min) ,decode(a.node_max ,a.node_id ,null ,a.node_max))
        as node_foreign
from tab2 as a


{#- /* End of File */ #}
