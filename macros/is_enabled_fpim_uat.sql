{% macro is_enabled_fpim_uat(verbose=false) %}

    {#
    -- defines whether or not FPIM UAT data is included in data pipelines.
    -- this should always be turned off in PROD.
    -- it is always turned on in ACC
    -- it can  be enabled/disabled in development with an environment variable
    #}

    {% if target.name == 'fdp_prod'  %}
        {% set res=false %}
        {{ log('if target.name is prod then always false',verbose) }}

    {% elif env_var('DBT_REGRESSION_FROM_DB', var('qa_db')) in ['psch'] %}
        {% set res=false %}
        {{ log('if doing regression against prod the false',verbose) }}

    {% elif env_var('FPIM_UAT_ENABLED','False') == 'True' %}
        {% set res=true %}
        {{ log('if target.name is not prod or acc then use env variable',verbose) }}

    {% else %} 
        {% set res=false %}
        {{ log('else false',verbose) }}

    {% endif %}

    {{ log('target.name: '~target.name,verbose) }}
    {{ log('$env:FPIM_UAT_ENABLED: '~env_var('FPIM_UAT_ENABLED','False'),verbose) }}

    {% if res %}
        {{ log('FPIM UAT enabled: '~res,verbose) }}
    {% else %}
        {{ log('FPIM UAT enabled: '~res,verbose) }}
    {% endif %}

    {{ return(res) }}

{% endmacro %}
