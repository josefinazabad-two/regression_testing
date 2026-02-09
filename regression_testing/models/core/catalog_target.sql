{# /*
--   Filename: catalog_target.sql
--   Author:   Jared Church <jared.church@heathsourcenz.co.nz
--
--   Purpose:
--        identifies test and gold databases for the regression test
--
*/ #}

select *
from {{ ref('catalog_target_s10') }}
