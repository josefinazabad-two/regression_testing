{# /*
--  Filename:   table_hash_compare.sql
--  Author:     Jared Church <jared.church@healthsourcenz.co.nz
--
--  Purpose:
--      This will use pre-hook to create a large number of test objects
--      the resultant view shows the current state of comparison for
--      warehouse layer against gold db.
--
--      gold_db = if target=acceptance then prod, else acceptance
--
*/ #}


{{ config(
    materialized="view",
    pre_hook="{{ test_regression(target_table=this.schema~'.'~this.name~'_b') }}"
) }}

-- depends_on: {{ ref('columns_in_both') }}
-- depends_on: {{ ref('objects_in_both') }}


select *
from {{ this }}_b

{# /* End of File */ #}
