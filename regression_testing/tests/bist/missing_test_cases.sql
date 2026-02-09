{#- /*
--  Filename: missing_test_cases.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      identifies test cases that should exist but do not

--  Primary Key:
--      table_schema,table_name,column_name,test_case

--  Technical Debt
--      this should turn into an error severity in future
--      only being left as warn for introduction. Once the
--      result in ACC environment is clean/near clean it should
--      become error
*/ -#}

{{ config(
    severity="error",
    store_failures = true,
) }}

select
    table_type
    ,key_type
    ,test_type
    ,test_exists
    ,object_foreign_exists

    ,object_partial_name as object_full_name
    ,column_name
    ,test_case
    ,object_partial_name_foreign as object_full_name_foreign
    ,node_id
    ,model_path
    -- ,table_catalog
    -- ,table_schema
    -- ,table_name
    ,pkey as skey_{{ this.name }}
from {{ ref('expected_test_cases') }}
where
    test_exists is null

    {#- /* if the foreign table doesn't exist then no need to test the relationship */ #}
    and not (key_type = 'foreign_key' and test_type = 'relationships' and object_foreign_exists is null)
order by table_type ,test_type ,key_type ,object_full_name




{#- /* End of File */ #}
