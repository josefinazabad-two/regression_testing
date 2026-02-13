{#- /*
--  Filename: clone_db_v1.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      This is a deprecated function, new version clone_db
--      created that uses a snowflake stored procedure

*/ -#}

{% macro clone_db_v1(from_db= var('qa_db') ,to_db='',verbose=false) %}

    {% set qa_db= var('qa_db') %}
    {% set qa_role='PSCH_QA_SERVICE_ROLE_FR' %}
    {% set prod_db= var('prod_db') %}
    {% set prod_role='PSCH_PRD_SERVICE_ROLE_FR' %}
    {% set dev_role='FPIM_DEVELOPER_FR' %}

    {{ log('args: from_db:   '~from_db  ,verbose) }}
    {{ log('args: to_db:     '~to_db  ,verbose) }}

    {# if not set at the args then use default database from profile.yml #}
    {% if not to_db|length %}
        {% set to_db=database %}
    {% endif %}

    {# /*
    -- if to_db is set to acceptance then we're cloning from prod to acceptance
    -- otherwise we assume cloning from acceptance to a dev environemnt
    -- set up variables for the two methods
    */ #}
    {% if to_db == qa_db %}
        {% set from_db = prod_db %}
        {% set to_role = qa_role %}
    {% else %}
        {# /* from db is acceptance */ #}
        {% set from_role = qa_role %}
        {% set to_role   = dev_role %}
    {% endif %}

    {% if from_db == prod_db %}
        {% set from_role = prod_role %}
    {% endif %}

    {% set from_lr = from_db~'_land_raw' %}
    {% set to_lr   = to_db~'_land_raw' %}

    {% set from_land = from_db~'_land' %}
    {% set to_land   = to_db~'_land' %}

    {% set from_persist = from_db~'_persist' %}
    {% set to_persist   = to_db~'_persist' %}

    {% set from_staging = from_db~'_staging' %}
    {% set to_staging   = to_db~'_staging' %}

    {% set from_core = from_db~'_core' %}
    {% set to_core   = to_db~'_core' %}

    {% set from_present = from_db~'_present' %}
    {% set to_present   = to_db~'_present' %}

    {{ log('from_db:   '~from_db  ,verbose) }}
    {{ log('from_role: '~from_role  ,verbose) }}
    {{ log('to_db:     '~to_db  ,verbose) }}
    {{ log('to_role:   '~to_role  ,verbose) }}

    {# drop database family for to_db #}
    {{ log('Drop Database Family',true) }}
    {% do run_query('drop database if exists '~to_db) %}
    {% do run_query('drop database if exists '~to_db~'_land_raw') %}
    {% do run_query('drop database if exists '~to_db~'_land') %}
    {% do run_query('drop database if exists '~to_db~'_persist') %}
    {% do run_query('drop database if exists '~to_db~'_staging') %}
    {% do run_query('drop database if exists '~to_db~'_core') %}
    {% do run_query('drop database if exists '~to_db~'_present') %}
    {% do run_query('drop database if exists '~to_db~'_regression') %}

    {# clone required databases #}
    {{ clone_db2_v1(from_db=from_lr ,to_db=to_lr ,from_role=from_role ,to_role=to_role) }}
    {{ clone_db2_v1(from_db=from_db ,to_db=to_db ,from_role=from_role ,to_role=to_role) }}
    {{ clone_db2_v1(from_db=from_land ,to_db=to_land ,from_role=from_role ,to_role=to_role) }}
    {{ clone_db2_v1(from_db=from_persist ,to_db=to_persist ,from_role=from_role ,to_role=to_role) }}
    {{ clone_db2_v1(from_db=from_staging ,to_db=to_staging ,from_role=from_role ,to_role=to_role) }}
    {{ clone_db2_v1(from_db=from_core ,to_db=to_core ,from_role=from_role ,to_role=to_role) }}
    {{ clone_db2_v1(from_db=from_present ,to_db=to_present ,from_role=from_role ,to_role=to_role) }}
    {# run upgrade for any changes necessary here #}
    {{ upgrade_version() }}

    {# /*
        update permissions - known issue that qliksense service accounts
        lose access to dev folder when clone is done.
        This is an expensive operation and is not typically an issue
        as Qlik doesn't normally use dev databases. If that process
        changes may need to reintroduce

        {{ log('Trigger full permissions update',true) }}
        {{ permissions() }}
    */ #}

{% endmacro %}

{% macro clone_db2_v1(from_db,to_db,from_role,to_role) %}

    {# /* drop & clone the existing to databases - main and land */ #}
    {{ log('clone db '~to_db~' from '~from_db,'true') }}
    {% do run_query('use role '~to_role) %}
    {% do run_query('drop database if exists '~to_db) %}
    {% do run_query('create database '~to_db~' clone '~from_db) %}

    {# /* grant appropriate permissions on the newly cloned databases */ #}
    {{ log('permissions on db: '~to_db,'true') }}
    {% do run_query('use role '~from_role) %}
    {% do run_query('revoke all privileges on database '~to_db~' from role '~from_role) %}
    {% do run_query('grant ownership on all file formats in database '~to_db~' to role '~to_role~'  copy current grants') %}
    {% do run_query('grant ownership on all schemas in database '~to_db~' to role '~to_role~'  copy current grants') %}
    {% do run_query('grant ownership on all tables  in database '~to_db~' to role '~to_role~'  copy current grants') %}
    {% do run_query('grant ownership on all views   in database '~to_db~' to role '~to_role~'  copy current grants') %}

    {# /* 
        drop regression test schema - this schema still has views that point at objects relative
        to the source database rather than target database. Removing this reduces chance of 
        confusion.
    */ #}
    {% do run_query('use role '~to_role) %}
    {% do run_query('drop schema if exists '~to_db~'.DBT_TEST_REGRESSION') %}

{% endmacro %}



{#- /* End of File */ #}
