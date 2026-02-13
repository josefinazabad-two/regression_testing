{%- macro tr_include_regression(unknown_row=false,col_name="ssid",verbose=false) -%}

    {# /*
    -- Args:
        -- col_name is for ssid column, it is included as an option in case a more specific/alternative name is necessary

        -- unknown row should always be included in the regression test (this could possibly be deprecated, not 100% sure
        -- as it may have been used in some non-standard cases to achieve the same results. These non-standard cases
        -- may not be covered by the new code).

    -- UAT data gets excluded in this test due primarily due to its (default) exclusion in our dev environment.

    -- UAT has been default excluded because it has not been fully supported yet through the FPIM Data Platform share in the
    -- same way as the PROD data.
    */ #}

    {%- if unknown_row -%}
        1 as _include_regression
    {%- else -%}
        cast(coalesce({{ col_name }} != {{ var('ssid_ebs_fpim_uat') }} ,true) as number) as _include_regression
    {%- endif -%}
{%- endmacro %}
