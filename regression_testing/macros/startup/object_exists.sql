{% macro object_exists(db_name, schema_name, object_name) %}

    {#- /* If the schema OR the table OR the database doesn't exist, return false */ -#}

    {%- set schema_exists = adapter.get_relation(database=db_name, schema=schema_name, identifier=object_name) -%}

    {% if schema_exists is none %}
        {{ return(false) }}
    {% else %}
        {{ return(true) }}
    {% endif %}

{% endmacro %}
