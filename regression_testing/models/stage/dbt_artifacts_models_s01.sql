{#- /* -- noqa: TMP
--  Filename: dbt_artifacts_models_s01.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      ingest of dbt_artifacts table - done specifically to handle
--      an sqlfluff error being removed from other objects.
--      sqlfluff doesn't handle 2 argument form of dbt ref function
--      see comment on first line which turns off the related sqlfluff
--      rule

--  Technical Debt
--      this whole model is tech debt - if sqlfluff error can be
--      resolved then this could go away.
*/ -#}

{{ config(
    tags=["must_be_view"]
) }}

select * from {{ ref('dbt_artifacts','models') }}


{#- /* End of File */ #}
