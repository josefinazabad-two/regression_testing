{% macro upgrade_version() %}
    {#
    --
    -- Version upgrade of database at startup
    -- checks the verison stored in info schema.version table and upgrade
    -- if needed
    -- This allows for changes to the schema to be delivered with release
    -- rather than manually changing

    -- This macro is no longer used as a part of the on-run-start
    -- because it had tendency to cause failure in execution of run/build
    -- operations. See below

    -- changes to the schema should generally be executed on 
    -- their own execution of dbt as change to the schema may cause
    -- change to the compile executed before the macro is run. Consider
    -- the case of an incremental table that is droped - the compile sees
    -- the table as existing and compiles as incremental load to the table
    -- however once it gets there the table doesn't exist (droped by this
    -- macro after the compile). The model will fail.
    --
    -- DB versions older that version 53 are no longer supported.
    -- Generally support for old versions can be dropped once
    -- prod environment is updated.
    --
    #}

    {{ log('upgrade_version - start') }}
    {% if selected_resources|length > 0 %}
        {{ print('############ upgrade_version macro is removed from on-run-start hook. Please run separately') }}
        {{ print('############ > dbt run-operation upgrade_version') }}
    {% endif %}

    {% do run_query('create database if not exists '~target.database) %}
    {% do run_query('create database if not exists '~target.database~'_LAND') %}
    {% do run_query('create database if not exists '~target.database~'_PERSIST') %}
    {% do run_query('create database if not exists '~target.database~'_STAGE') %}
    {% do run_query('create database if not exists '~target.database~'_CORE') %}
    {% do run_query('create database if not exists '~target.database~'_PRESENT') %}

    {% set current_version = get_version() %}
    {{ log('Database version (Start): '~current_version) }}

    
    {% set new_version=55 %}
    {% if current_version < new_version %}
        {% set comment = 'Update database to version '~new_version~' - removed redundant objects from data warehouse' %}
        {% set query_list=[] %}
        {{ query_list.append('CREATE OR REPLACE SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2') }}
        {{ query_list.append('CREATE OR REPLACE SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM') }}
        {{ query_list.append('CREATE OR REPLACE SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR') }}
        {{ query_list.append('CREATE OR REPLACE SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM') }}
        {{ query_list.append('CREATE OR REPLACE SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN') }}

        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2.ONELINK_TYPE2 LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2.ONELINK_TYPE2') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2.ONELINK_TYPE2 SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2.ONELINK_TYPE2') }}

        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.EGO_MTL_SY_ITEMS_EXT_B_NET_CONTENT LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.EGO_MTL_SY_ITEMS_EXT_B_NET_CONTENT') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.EGO_MTL_SY_ITEMS_EXT_B_NET_CONTENT SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.EGO_MTL_SY_ITEMS_EXT_B_NET_CONTENT') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.FND_LOOKUP_VALUES LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.FND_LOOKUP_VALUES') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.FND_LOOKUP_VALUES SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.FND_LOOKUP_VALUES') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.HR_LOCATIONS_ALL LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.HR_LOCATIONS_ALL') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.HR_LOCATIONS_ALL SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.HR_LOCATIONS_ALL') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_ITEM_SUB_INVENTORIES LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_ITEM_SUB_INVENTORIES') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_ITEM_SUB_INVENTORIES SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_ITEM_SUB_INVENTORIES') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_ONHAND_QUANTITIES_DETAIL LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_ONHAND_QUANTITIES_DETAIL') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_ONHAND_QUANTITIES_DETAIL SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_ONHAND_QUANTITIES_DETAIL') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_ONHAND_QUANTITIES_DETAIL_TIMESTAMP LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_ONHAND_QUANTITIES_DETAIL_TIMESTAMP') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_ONHAND_QUANTITIES_DETAIL_TIMESTAMP SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_ONHAND_QUANTITIES_DETAIL_TIMESTAMP') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_SECONDARY_INVENTORIES LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_SECONDARY_INVENTORIES') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_SECONDARY_INVENTORIES SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_SECONDARY_INVENTORIES') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_SYSTEM_ITEMS_B_UOI LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_SYSTEM_ITEMS_B_UOI') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.MTL_SYSTEM_ITEMS_B_UOI SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.MTL_SYSTEM_ITEMS_B_UOI') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.PO_LINE_LOCATIONS_ALL LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.PO_LINE_LOCATIONS_ALL') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.PO_LINE_LOCATIONS_ALL SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.PO_LINE_LOCATIONS_ALL') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.XX_EBS_OBJECTS LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.XX_EBS_OBJECTS') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.XX_EBS_OBJECTS SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_FPIM.XX_EBS_OBJECTS') }}

        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.FND_LOOKUP_VALUES LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.FND_LOOKUP_VALUES') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.FND_LOOKUP_VALUES SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.FND_LOOKUP_VALUES') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.HR_LOCATIONS_ALL LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.HR_LOCATIONS_ALL') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.HR_LOCATIONS_ALL SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.HR_LOCATIONS_ALL') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.MTL_ITEM_SUB_INVENTORIES LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.MTL_ITEM_SUB_INVENTORIES') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.MTL_ITEM_SUB_INVENTORIES SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.MTL_ITEM_SUB_INVENTORIES') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.MTL_ONHAND_QUANTITIES_DETAIL LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.MTL_ONHAND_QUANTITIES_DETAIL') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.MTL_ONHAND_QUANTITIES_DETAIL SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.MTL_ONHAND_QUANTITIES_DETAIL') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.MTL_SECONDARY_INVENTORIES LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.MTL_SECONDARY_INVENTORIES') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.MTL_SECONDARY_INVENTORIES SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_EBS_NR.MTL_SECONDARY_INVENTORIES') }}

        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_CYCLE_COUNT LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_CYCLE_COUNT') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_CYCLE_COUNT SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_CYCLE_COUNT') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_INVENTORY LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_INVENTORY') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_INVENTORY SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_INVENTORY') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_PRICE LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_PRICE') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_PRICE SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_PRICE') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_SUPPLIER LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_SUPPLIER') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_SUPPLIER SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_LEGACY_FPIM.SOH_PLAN_TYPE2_SUPPLIER') }}

        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_CYCLE_COUNT_TYPE2 LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_CYCLE_COUNT_TYPE2') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_CYCLE_COUNT_TYPE2 SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_CYCLE_COUNT_TYPE2') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_INVENTORY_TYPE2 LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_INVENTORY_TYPE2') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_INVENTORY_TYPE2 SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_INVENTORY_TYPE2') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_PRICE_TYPE2 LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_PRICE_TYPE2') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_PRICE_TYPE2 SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_PRICE_TYPE2') }}
        {{ query_list.append('CREATE OR REPLACE TABLE '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_SUPPLIER_TYPE2 LIKE HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_SUPPLIER_TYPE2') }}
        {{ query_list.append('INSERT INTO '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_SUPPLIER_TYPE2 SELECT * FROM HSNZ_BII_PSCH_SNAPSHOTS.PERSIST_TYPE2_SOH_PLAN.SOH_PLAN_SUPPLIER_TYPE2') }}

        -- Grant USAGE on all schemas
        {{ query_list.append('GRANT USAGE ON SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2 TO ROLE FPIM_DEVELOPER_FR') }}
        {{ query_list.append('GRANT USAGE ON SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM TO ROLE FPIM_DEVELOPER_FR') }}
        {{ query_list.append('GRANT USAGE ON SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR TO ROLE FPIM_DEVELOPER_FR') }}
        {{ query_list.append('GRANT USAGE ON SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM TO ROLE FPIM_DEVELOPER_FR') }}
        {{ query_list.append('GRANT USAGE ON SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN TO ROLE FPIM_DEVELOPER_FR') }}
        -- Grant SELECT on all tables in schemas
        {{ query_list.append('GRANT SELECT ON ALL TABLES IN SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2 TO ROLE FPIM_DEVELOPER_FR') }}
        {{ query_list.append('GRANT SELECT ON ALL TABLES IN SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM TO ROLE FPIM_DEVELOPER_FR') }}
        {{ query_list.append('GRANT SELECT ON ALL TABLES IN SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR TO ROLE FPIM_DEVELOPER_FR') }}
        {{ query_list.append('GRANT SELECT ON ALL TABLES IN SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_LEGACY_FPIM TO ROLE FPIM_DEVELOPER_FR') }}
        {{ query_list.append('GRANT SELECT ON ALL TABLES IN SCHEMA '~target.database~'_PERSIST.PERSIST_TYPE2_SOH_PLAN TO ROLE FPIM_DEVELOPER_FR') }}

        {{ execute_query_list(query_list,comment) }}
        {{ set_version(new_version) }}
    {% endif %}

-- 55 was skipped due to psch - fdp migration

    {% set new_version=56 %}
    {% if current_version < new_version %}
        {% set comment = 'Update database to version '~new_version~' - removed redundant objects from data warehouse' %}
        {% set query_list=[] %}
        {% if object_exists(target.database~'_PRESENT', 'DBT_DM_SUPPLYCHAIN', 'DIM_CALENDAR') %}
            {{ query_list.append('drop schema if exists '~target.database~'_PRESENT.DBT_DM_SUPPLYCHAIN') }}  
        {% endif %} 

        {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.FACT_DIFOT') }}
        {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.DIM_DIFOT_TARGETS') }}
        {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.FACT_STOCK_AVAILABILITY') }}
        {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.FACT_NON_CATALOGUE_PO_REQ') }}
        {{ query_list.append('drop view if exists '~target.database~'_CORE.DBT_WAREHOUSE.island_supply_chain_sla_variables') }}

        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_10_DIFOT') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_20_DIFOT') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_30_DIFOT') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_30_STOCK_AVAILABILITY') }} 
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_40_STOCK_AVAILABILITY') }} 
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_10_NON_CATALOGUE_PO_REQ') }} 
        {{ query_list.append('drop table if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_40_NON_CATALOGUE_PO_REQ') }} 
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_50_NON_CATALOGUE_PO_REQ') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_04_FND_LOOKUP_VALUES_SNAPSHOTS') }}        
        {{ query_list.append('drop table if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_05_FND_LOOKUP_VALUES_SNAPSHOTS') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_04_HR_LOCATIONS_ALL_SNAPSHOT') }}        
        {{ query_list.append('drop table if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_05_HR_LOCATIONS_ALL_SNAPSHOTS') }}

        {{ execute_query_list(query_list,comment) }}
        {{ set_version(new_version) }}
    {% endif %}


    {% set new_version=57 %}
    {% if current_version < new_version %}
        {% set comment = 'Update database to version '~new_version~' - removed redundant objects from data warehouse' %}
        {% set query_list=[] %}
        {{ query_list.append('drop table if exists '~target.database~'_LAND_RAW.REFERENCE.GF_DIFOT') }}
        {{ query_list.append('drop view if exists '~target.database~'_LAND.DBT_INGEST_REFERENCE_DATA.GF_DIFOT') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING.EDITIONING_VIEWS_S05') }}
        {{ query_list.append('drop table if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_10_UNIQUE_PO_DIST_LINE_LOCATION') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_50_DIFOT_FILTER') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_TYPE2.FND_LOOKUP_VALUES_fpim_type2_s10') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_TYPE2.HR_LOCATIONS_ALL_fpim_type2_s10') }}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_TYPE2.FND_LOOKUP_VALUES_nr_type2_s10') }} 
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_TYPE2.HR_LOCATIONS_ALL_nr_type2_s10') }} 
        {{ query_list.append('drop table if exists '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.XX_EBS_OJECTS') }} 
        {{ query_list.append('drop table if exists '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.FND_LOOKUP_VALUES') }} 
        {{ query_list.append('drop table if exists '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.FND_LOOKUP_VALUES') }}
        {{ query_list.append('drop table if exists '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_NR.HR_LOCATIONS_ALL') }} 
        {{ query_list.append('drop table if exists '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM.HR_LOCATIONS_ALL') }}  
        {% if is_enabled_fpim_uat() %}
            {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_TYPE2.FND_LOOKUP_VALUES_fpim_uat_type2_s10') }}
            {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_TYPE2.HR_LOCATIONS_ALL_fpim_uat_type2_s10') }}
            {{ query_list.append('drop table if exists '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM_UAT.FND_LOOKUP_VALUES') }}
            {{ query_list.append('drop table if exists '~target.database~'_PERSIST.PERSIST_TYPE2_EBS_FPIM_UAT.HR_LOCATIONS_ALL') }}
        {% endif %}
        {{ query_list.append('drop view if exists '~target.database~'_STAGING.DBT_STAGING_QLIK_TRANSLATION.STAGE_160_PO_LINE_LOCATIONS_ALL_FPIM') }}
        {{ execute_query_list(query_list,comment) }}
        {{ set_version(new_version) }}
    {% endif %}

    {% set new_version=58 %}
    {% if current_version < new_version %}
        {% set comment = 'Update database to version '~new_version~' - removed redundant objects from data warehouse' %}
        {% set query_list=[] %}
        {% if object_exists(target.database~'_CORE', 'DBT_WAREHOUSE', 'DIM_CONTRACT_FINANCIALS') %}
            {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.DIM_CONTRACT_FINANCIALS') }}  
        {% endif %} 
        {% if object_exists(target.database~'_PRESENT', 'DBT_DM_CONTRACTS', 'DIM_CONTRACT_FINANCIALS') %}
            {{ query_list.append('drop view if exists '~target.database~'_PRESENT.DBT_DM_CONTRACTS.DIM_CONTRACT_FINANCIALS') }}  
        {% endif %} 
        {{ execute_query_list(query_list,comment) }}
        {{ set_version(new_version) }}
    {% endif %}

    {% set new_version=59 %}
    {% if current_version < new_version %}
        {% set comment = 'Update database to version '~new_version~' - removed redundant objects from data warehouse' %}
        {% set query_list=[] %}
        {{ query_list.append('drop schema if exists '~target.database~'_PRESENT.DBT_DM_NATIONAL_CONTRACTS_REGISTER') }}  
        {{ execute_query_list(query_list,comment) }}
        {{ set_version(new_version) }}
    {% endif %}

    {% set new_version=60 %}
    {% if current_version < new_version %}
        {% set comment = 'Update database to version '~new_version~' - removed redundant objects from data warehouse' %}
        {% set query_list=[] %}
        {{ query_list.append('drop table if exists '~target.database~'_LAND_RAW.CERAF.NCAMIS_XML') }} 
        {{ query_list.append('drop schema if exists '~target.database~'_LAND_RAW.NATIONAL_CONTRACTS_REGISTER cascade') }}   
        {{ query_list.append('drop view if exists '~target.database~'_LAND.DBT_INGEST_REFERENCE_DATA.NATIONAL_CONTRACTS_REGISTER') }}
        {{ query_list.append('drop view if exists '~target.database~'_CORE.DBT_WAREHOUSE.FACT_CONTRACT') }}
        {{ execute_query_list(query_list,comment) }}
        {{ set_version(new_version) }}
    {% endif %}

    {% set new_version=61 %}
    {% if current_version < new_version %}
        {% set comment = 'Update database to version '~new_version~' - drop jira objects' %}
        {% set query_list=[] %}
        {% if object_exists(target.database~'_PRESENT', 'DBT_DM_JIRA_DATA', 'FACT_JIRA_DATA') %}
            {{ query_list.append('drop schema if exists '~target.database~'_PRESENT.DBT_DM_JIRA_DATA') }} 
            {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.DIM_JIRA_TICKETS') }} 
            {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.DIM_JIRA_USER') }} 
            {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.BRIDGE_JIRA_LABELS') }} 
            {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.BRIDGE_JIRA_SPRINTS') }} 
            {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.DIM_SPRINTS') }} 
            {{ query_list.append('drop table if exists '~target.database~'_CORE.DBT_WAREHOUSE.FACT_JIRA_DATA') }} 
        {% endif %} 
        {% do run_query('alter database if exists '~target.database~'_STAGING rename to '~target.database~'_STAGE') %}
        {{ execute_query_list(query_list,comment) }}
        {{ set_version(new_version) }}
    {% endif %}

    {% set current_version = get_version() %}
    {{ log("Database version: "~current_version,'true') }}

{% endmacro %}





{#- /* End of File */ #}
