{% macro startup_logging() %}
    {#
    --
    -- logging done at start up
    --
    --  creates log table if it doesn't exist
    --  moves contents of log table into history table (only keep most recent day in the log table itself)
    --  truncate the log table (except for any entries that already exist from today)
    --  insert a start record into the log table.
    --
    #}
    {% set log_table = var('cInfoSchema')~"."~var('cLogTable') %}
    {% set log_function_base = 'dbt ('~dbt_version~') '~flags.WHICH %}

    {% set query_create_log_table %}
        create table if not exists {{ log_table }}
            (timestamp timestamp,
            function varchar(255),
            message text)
    {% endset %}

    {% set query_create_log_table_history %}
        create table if not exists {{ log_table }}_history
            (hist_timestamp timestamp,
            timestamp timestamp,
            function varchar(255),
            message text)
    {% endset %}

    {% set query_copy_history %}
        insert into {{ log_table }}_history
            select current_timestamp,* from {{ log_table }}
            where cast(timestamp as date) <> current_date
    {% endset %}

    {% set query_clear_log_table %}
        delete from {{ log_table }}
            where cast(timestamp as date) <> current_date
    {% endset %}

    {% set query_insert_log %}
        insert into {{ log_table }} 
            select current_timestamp as timestamp
                ,'{{ log_function_base }} - start' as function 
                ,'start' as message
    {% endset %}

    {% do run_query(query_create_log_table) %}
    {% do run_query(query_create_log_table_history) %}
    {% do run_query(query_copy_history) %}
    {% do run_query(query_clear_log_table) %}
    {% do run_query(query_insert_log) %}

{% endmacro %}



{#- /* End of File */ #}
