{#- /*
--  Filename: expected_cases_s20.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      filtering out excluded test cases

--  Primary Key:
--      Write some information about the primary key for this table

--  Technical Debt
--      none known
*/ -#}


select
    table_catalog
    ,table_schema
    ,table_name
    ,column_name
    ,test_type
    ,object_full_name
    ,table_type
    ,key_type
    ,exclude_case
from {{ ref('expected_cases_s10') }}
where exclude_case = false

{#- /* End of File */ #}
