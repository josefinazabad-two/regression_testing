{#- /*
--  Filename: po_structure_consistency.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      This test is to check consistency of po information
--      across the 4 levels of po documents. Specifically
--      the test asks are there any distributions that
--      exist where headers, lines, and line locations
--      do not exist (this should not be possible in EBS)

--      Only test for FPIM seeing as other source systems
--      are more static and less likely an issue of concern.

--      Decision was made to test against the persist schema
--      seeing as that is the source for most data models
--      and is valid for the day, testing against source or
--      ingest schema would be a point in time and that is
--      something that doesn't tell us specifically what we
--      end up with in our data models.

--  Primary Key:
--      po_distribution_id

--  Technical Debt
--      none known

--  History
--      2024-03-18: churchj2: Initial Version
*/ -#}

{{ config(
    severity = "warn",
) }}


select
    pod.po_distribution_id
    ,poll.line_location_id
    ,pol.po_line_id
    ,poh.po_header_id
from {{ ref('PO_DISTRIBUTIONS_ALL_fpim_persist') }} as pod
left join {{ ref('PO_LINE_LOCATIONS_ALL_fpim_persist') }} as poll on poll.line_location_id = pod.line_location_id
left join {{ ref('PO_LINES_ALL_fpim_persist') }} as pol on pol.po_line_id = pod.po_line_id
left join {{ ref('PO_HEADERS_ALL_fpim_persist') }} as poh on poh.po_header_id = pod.po_header_id
where
    poh.po_header_id is null
    or pol.po_line_id is null
    or poll.line_location_id is null



{#- /* End of File */ #}
