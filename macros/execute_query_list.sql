{% macro execute_query_list(query_list,comment,verbose=True) %}
    {#
    --
    -- Executes a list of querys outputting a comment at the start
    -- default will also output each query before it is executed
    -- this can be disabled by verbose argument.
    --
    #}


    {{ log(comment,info='true') }}

    {% for query in query_list %}
        {{ log(query,info=verbose) }}
        {% do run_query(query) %}
    {% endfor %}

{% endmacro %}




{#- /* End of File */ #}
