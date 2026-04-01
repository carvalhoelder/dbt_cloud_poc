{% test check_lookup_value(column_name,model) %}

{% set model_name -%}
 {{model}}
{%- endset %} 

{% set model_name =  model_name|replace('"','')|replace(target.database ~ '.','') %}
{% set where_cond =  "SourceSystemTable='" ~ model_name ~ "' and SourceSystemColumn='" ~ column_name ~ "'" %}

with validation as (
    select
       distinct {{ column_name }} as test_field
    from {{ model }}
),

{% set lookup_values = get_column_values(table=source('MetaMaster', 'MidasLookupTable'), column='SourceValue',where=where_cond) %}

validation_errors as (
    select
        test_field
    from validation
    where test_field not in ( {{ lookup_values|join(',') }}  )

)

select *
from validation_errors

{% endtest %}