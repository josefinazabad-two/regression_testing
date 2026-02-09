{#- /*
--  Filename: get_database_family.sql
--  Author: Jared Church

--  Purpose:
--      returns a list of databases in the database family

--  Technical Debt
--      none known

*/ -#}




{% macro get_database_family(family= var('qa_db') ,verbose=false) %}

    {{ log('Database Family: '~family,verbose) }}
    {% set res = run_query("show databases like '"~family~"%'") %}
    {#
        {% do res.print_table() %}
    #}
    {% set col_name = return_column(data=res,col=1)%}
    {{ log(col_name,verbose) }}
    {{ return(col_name) }}

{% endmacro %}


{#- /* End of File */ #}
