{#- /*
--  Filename: defined_models_s01.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      gets records of models in the databaes defined
--      by dbt from the last upload

--  Primary Key:
--      node_id

--  Technical Debt
--      none known
*/ -#}

{{ config(
    materialized="view",
    tags=["must_be_view"]
) }}

select
    node_id
    ,depends_on_nodes
    ,path as model_path
    ,upper(database) as table_catalog
    ,upper(schema) as table_schema
    ,upper(alias) as alias
    ,concat_ws('.' ,upper(schema) ,upper(alias)) as obj
from {{ ref('dbt_artifacts_models_s01') }}
where run_started_at in (select max(run_started_at) from {{ ref('dbt_artifacts_models_s01') }})



{#- /* End of File */ #}
