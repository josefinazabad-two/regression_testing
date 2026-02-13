{#- /*
--  Filename: catalog_target_s10.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Defines the two databases involved in a regression test
--      note that swapping to use current_database() as gold
--      allows general build of comparison objects even if
--      some object are not available in the gold database

--  Primary Key:
--      none - there is only 1 row

--  Technical Debt
--      none known
*/ -#}


select
    current_database() || '%' as test_database
    ,upper('{{ env_var("DBT_REGRESSION_FROM_DB" , var("qa_db") ) }}') || '%' as gold_database
    -- ,current_database() as gold_database
from dual




{#- /* End of File */ #}
