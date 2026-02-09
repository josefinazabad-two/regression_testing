{#- /*
--  Filename: objects_not_recently_updated.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      this test identifies objects in the database that have not been updated today
--      for ETL we generally expect all models to be update each day.
--      this may not always be true and if so this test will need updating to exclude
--      such cases.

--      Persist for EBS_NR is not expected to change
--      Test schemas can be excluded

--  Primary Key:
--      combination of table_catalog, table_schema and table_name

--  Technical Debt
--      none known
*/ -#}

{{ config(
    severity="warn",
    store_failures = true,
) }}

{% set strDropTable %}
    'drop ' ,split_part(table_type ,' ' ,-1) ,' if exists ' ,table_schema ,'.' ,table_name
{% endset %}

select
    table_schema
    ,table_name
    ,table_type
    ,created
    ,last_altered
    ,last_ddl
    ,last_ddl_by
    ,concat({{ strDropTable }} ,';') as drop_command
    ,concat({{ var('openCurly') }} ,' query_list.append(' ,{{ var('singleQuote') }} ,{{ strDropTable }} ,{{ var('singleQuote') }} ,') ' ,{{ var('closeCurly') }}) as drop_command2
from {{ source('target_info_schema','tables') }}
where
    last_altered < current_date
    and not (table_schema in ('INFO_HS' ,'PUBLIC' ,'PERSIST_TYPE2_EBS_NR' ,'DBT_TEST_AUDIT'))




{#- /* End of File */ #}
