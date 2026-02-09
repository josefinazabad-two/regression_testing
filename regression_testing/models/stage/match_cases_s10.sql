{#- /*
--  Filename: match_cases_s10.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      match actual case against expected cases

--  Primary Key:
--      pkey column

--  Technical Debt
--      none known
*/ -#}

{{ config(
    materialized="view",
    tags=["must_be_view"],
) }}

with data as (
    select
        e.object_full_name
        ,e.column_name
        ,e.test_type
        ,d.object_foreign_exists
        ,e.key_type
        ,d.test_case
        ,d.node_id
        ,d.model_path
        ,e.table_type
        ,d.test_exists
        ,e.table_catalog
        ,e.table_schema
        ,e.table_name
        ,d.object_full_name_foreign
        ,concat_ws('.' ,e.table_schema ,e.table_name) as object_partial_name
        ,concat_ws('.' ,d.table_schema ,d.table_name) as object_partial_name_foreign
        ,hash(e.table_schema ,e.table_name ,e.column_name ,e.test_type) as pkey
    from {{ ref('expected_cases_s20') }} as e
    left join {{ ref('defined_tests_s12') }} as d on
        e.object_full_name = d.object_full_name
        and e.test_type = d.test_case
        and e.column_name = d.column_name
)

select
    object_full_name
    ,column_name
    ,test_type
    ,key_type
    ,test_case
    ,object_foreign_exists
    ,object_full_name_foreign
    ,node_id
    ,model_path
    ,table_type
    ,test_exists
    ,table_catalog
    ,table_schema
    ,table_name
    ,pkey
    ,object_partial_name
    ,object_partial_name_foreign
from data


{#- /* End of File */ #}
