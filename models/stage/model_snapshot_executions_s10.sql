{#- /*
--  Filename: model_snapshot_executions_s10.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Combines Model and Snapshot executions into one table
--      also handles timezones

--  Primary Key:
--      NODE_ID & COMMAND_INVOCATION_ID

--  Technical Debt
--      There seems to be some complexity in snowflake converting from UTC to local
--      probably need to keep an eye on this as it may not handel DST correctly, or a
--      bug may be fixed in the future.
*/ -#}

{{ config(
    tags=["must_be_view"]
) }}

{% set cols %}
    node_id
    ,command_invocation_id
    ,schema
    ,name
    ,alias
    ,materialization
    ,status


    ,total_node_runtime
    ,compile_started_at as compile_started_at_raw
    ,convert_timezone('UTC' ,'Pacific/Auckland' ,compile_started_at) as compile_started_at
    ,convert_timezone('UTC' ,'Pacific/Auckland' ,run_started_at) as run_started_at
    ,convert_timezone('UTC' ,'Pacific/Auckland' ,query_completed_at) as query_completed_at

    ,was_full_refresh
    ,message
    ,adapter_response
    ,rows_affected
    ,thread_id
{% endset %}

select {{ cols }} from {{ ref ('dbt_artifacts','snapshot_executions') }}
union all
select {{ cols }} from {{ ref ('dbt_artifacts','model_executions') }}
