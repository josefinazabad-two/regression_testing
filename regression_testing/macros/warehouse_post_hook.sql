{% macro warehouse_post_hook(verbose=false) %}
    {# /*
    -- This macro implements 5 tests to be run on all tables in warehouse schema
    --
    --  Test 1: SKEY naming convention - skey column must exist and named as skey_<tablename>
    --  Test 2: SKEY column may not contain null values
    --  Test 3: SKEY column must be unique
    --  Test 4. Dimensions have an unknown row.
    --  Test 5. Foreign keys of FACT table are not null (warning).
    --
    --  Any of these test will generate an error if they fail, preventing build of
    --  the offending model.
    --
    */ #}

    {% if execute %}
        {% set short_this=this.schema~'.'~this.table %}

        {# /* skey naming convention skey_<tablename> */ #}
        {% set skey='SKEY_'~this.table %}



        {# /* all models in warehouse must be either dim/fact or bridge */ #}
        {{ log(short_this~' test file naming convention ',true) }}
        {% if 'DIM_' not in this.table|upper  and 'FACT_' not in this.table|upper and 'BRIDGE_' not in this.table|upper %}
            {{ exceptions.raise_compiler_error(this.table~" error not dim/fact/bridge") }}
        {% endif %}


        {# /* Test 1: cause an error if skey does not exist */ #}
        {{ log(short_this~' test primary key exists ('~skey~') ',true) }}
        select {{ skey }} from {{ this }} where 1=0;



        {# /* Tets 2/5: cause an warning if any skey has null results */ #}
        {{ log(short_this~' test primary key not null ('~skey~') ',true) }}
        {%- set columns = adapter.get_columns_in_relation(this) -%}
        {% for column in columns %}
            {% if 'SKEY_' in column.column|upper %}
                {% set result = run_query("select "~column.column~",count(*) from "~this~" where "~column.column~" is null group by "~column.column) %}
                {% if result|length > 0 %}
                    {% set message_text="Model: "~this.table~" Key: "~column.column~" has nulls - please implement coalesce(...,{{ var('cUnknownKeyValue') }})" %}
                    {% if column.column == skey %}
                        {{ exceptions.raise_compiler_error(message_text) }}
                    {% else %}
                        {{ exceptions.warn(message_text) }}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endfor %}



        {# /* Test 3: cause an error if primary key is not unique */ #}
        {{ log(short_this~' test primary key unique ('~skey~') ',true) }}
        {% set result = run_query("select count(distinct "~skey~"),count(*) from "~this~" having count(distinct "~skey~") <> count(*)") %}
        {% if result|length > 0 %}
            {{ exceptions.raise_compiler_error(skey~" is not unique") }}
        {% endif %}



        {# /* Test 4: dim must have unknown row with skey = var('cUnknownKeyValue') */ #}
        {% if 'DIM_' in this.table|upper %}
            {{ log(short_this~' test unknown row',true) }}
            {% set result = run_query("select * from "~this~" where "~skey~" = "~ var('cUnknownKeyValue')) %}
            {% if result|length != 1 %}
                {{ exceptions.raise_compiler_error(this.table~" does not have unknown row") }}
            {% endif %}
        {% endif %}

    {% endif %}

{% endmacro %}
