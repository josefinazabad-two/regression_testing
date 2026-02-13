select *
from {{ ref('columns_in_database_s20') }}
where gold = test
order by
    table_schema
    ,table_name
    ,column_name
    ,data_type
