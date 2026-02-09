{#- /*
--  Filename: get_regression_db.sql
--  Author: Jared Church

--  Purpose:
--      returns the name of the regression database - this has been
--      consolidated into the core database as we move towards alignment
--      with FDP database structure.

--  Technical Debt
--      none known

*/ -#}


{% macro get_regression_db(verbose=false) %}

    {% set regression_db=target.database~'_core' %}
    {{ log('Regression DB: '~regression_db,verbose) }}
    {% set res = run_query('create database if not exists '~regression_db) %}
    {{ return(regression_db) }}

{% endmacro %}


{#- /* End of File */ #}
