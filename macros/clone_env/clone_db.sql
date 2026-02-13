{#- /*
--  Filename: clone_db.sql
--  Author: Jared Church <jared.church@healthsourcenz.co.nz>

--  Purpose:
--      Cloning of database

--      Only accepts either prod or acceptance as the from db.
--      default to db is dev_<username>
--      if to db is acceptance then from db enforced as prod.

--  Future Development
--      Introduce a "regression mode" as compared to a "dev
--      mode" - where dev mode clones everything (and is default)
--      while regression mode only dev's what is necessary.
--      Noting that regression mode may go away - if we change
--      regression strategy to "build only what is neceesary" then
--      regression needs a fully clone.

--  Technical Debt
--      handling of the database family should be modified
--      into a proper list of databases etc rather than
--      the current method including looking at snowflake
--      itself to handle which databases to process.
--      Done this way as a quick solution to address gap
--      introduced when core database was created.

*/ -#}

{% macro clone_db(from_db= var('qa_db') ,to_db='',verbose=false) %}

    {% set qa_db= var('qa_db') %}
    {% set qa_role= var('qa_role') %}
    {% set prod_db= var('prod_db') %}
    {% set prod_role= var('prod_role') %}
    {% set dev_role= var('dev_role') %}

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

    {% set suffix_lr = '_land_raw' %}
    {% set suffix_land = '_land' %}
    {% set suffix_persist = '_persist' %}
    {% set suffix_staging = '_stage' %}
    {% set suffix_core = '_core' %}
    {% set suffix_present = '_present' %}

    {% set from_lr = from_db~suffix_lr %}
    {% set from_land = from_db~suffix_land %}
    {% set from_persist = from_db~suffix_persist %}
    {% set from_staging = from_db~suffix_staging %}
    {% set from_core = from_db~suffix_core %}
    {% set from_present = from_db~suffix_present %}



    {{ log('from_db:   '~from_db  ,verbose) }}
    {{ log('from_role: '~from_role  ,verbose) }}
    {{ log('to_db:     '~to_db  ,verbose) }}
    {{ log('to_role:   '~to_role  ,verbose) }}

    {# /* drop database family for to_db */ #}
    {{ log('Drop Database Family',true) }}
    {% do run_query('drop database if exists '~to_db) %}
    {% do run_query('drop database if exists '~to_db~suffix_lr) %}
    {% do run_query('drop database if exists '~to_db~suffix_land) %}
    {% do run_query('drop database if exists '~to_db~suffix_persist) %}
    {% do run_query('drop database if exists '~to_db~suffix_staging) %}
    {% do run_query('drop database if exists '~to_db~suffix_core) %}
    {% do run_query('drop database if exists '~to_db~suffix_present) %}
    {% do run_query('drop database if exists '~to_db~suffix_regression) %}

    {# /* clone required databases */ #}
    {{ clone_db2(from_db=from_lr ,to_db=to_db ,from_role=from_role ,to_role=to_role, fam_member=suffix_lr) }}
    {{ clone_db2(from_db=from_db ,to_db=to_db ,from_role=from_role ,to_role=to_role, fam_member='') }}
    {{ clone_db2(from_db=from_land ,to_db=to_db ,from_role=from_role ,to_role=to_role, fam_member=suffix_land) }}
    {{ clone_db2(from_db=from_persist ,to_db=to_db ,from_role=from_role ,to_role=to_role, fam_member=suffix_persist) }}
    {{ clone_db2(from_db=from_staging ,to_db=to_db ,from_role=from_role ,to_role=to_role, fam_member=suffix_staging) }}
    {{ clone_db2(from_db=from_core ,to_db=to_db ,from_role=from_role ,to_role=to_role, fam_member=suffix_core) }}
    {{ clone_db2(from_db=from_present ,to_db=to_db ,from_role=from_role ,to_role=to_role, fam_member=suffix_present) }}
    {# /* run upgrade for any changes necessary here  */ #}
    {% do run_query('use database '~to_db) %}
    {{ upgrade_version() }}

{% endmacro %}

{% macro clone_db2(from_db,to_db,from_role,to_role, fam_member) %}

    {# /* drop & clone the existing to databases - main and land */ #}
    {{ log('clone db '~to_db~fam_member~' from '~from_db,'true') }}
    {% do run_query('use role '~to_role) %}
    {% do run_query('drop database if exists '~to_db~fam_member) %}

    {# /* use stored procedure that handles cloning and grants transfer */#}
    {% do run_query('use database DEV_ADMIN') %}
    {% do run_query('use schema PUBLIC') %}
    {% do run_query("call CREATE_CLN_DB('"~to_db~"', '"~from_db~"', '"~to_role~"')") %}

    {# /* 
        drop regression test schema - this schema still has views that point at objects relative
        to the source database rather than target database. Removing this reduces chance of 
        confusion.
    */ #}
    {% do run_query('use role '~to_role) %}
    {% do run_query('drop schema if exists cln_'~to_db~'_'~from_db~'.DBT_TEST_REGRESSION') %}
    {% do run_query('alter database cln_'~to_db~'_'~from_db~' rename to '~to_db~fam_member) %}


{% endmacro %}



{#- /* End of File */ #}
