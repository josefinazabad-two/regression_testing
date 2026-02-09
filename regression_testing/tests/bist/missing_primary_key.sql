{#- /*
--  Filename: missing_primary_key.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      test to identify cases where the primary key is missing (based on naming convention)

--  Primary Key:
--      table_schema and table_name form composite key

--  Technical Debt
--      I'm pretty sure that this is not working correctly in some cases - need to do some validation
--      consider REP_SOH_PLAN - it doesn't have a primary key column match naming convention but is
--      not showing up in the errors. - may need some test, bugfix or clarification of spec for this
--      test.
*/ -#}


{{ config(
    severity="error",
    store_failures = true,
) }}


select
    *
    ,hash(column_full_name) as skey_{{ this.name }}
from {{ ref('expected_primary_key') }}
where col_exists is null


{#- /* End of File */ #}
