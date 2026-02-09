{# /*
--   Filename: objects_added_removed.sql
--   Author:   Jared Church <jared.church@heathsourcenz.co.nz
--
--   Purpose:
--        identifies objects that are added or removed in
--        test db vs gold
--
*/ #}

select
    *
    ,case
        when gold = 1 and test = 0 then 'removed'
        when gold = 0 and test = 1 then 'added'
    end as change
from {{ ref('objects_in_database_s20') }}
where gold != test
order by table_schema ,table_name
