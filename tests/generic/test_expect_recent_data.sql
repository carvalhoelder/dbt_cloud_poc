{% test expect_recent_data(column_name,model,datepart,interval) %}

{% set query %}
with validation as (
    SELECT 
        datediff( {{datepart}}, max([{{column_name}}]),GETUTCDATE() ) the_diff
    FROM {{model}} )

    select the_diff from validation

{% endset %}    

 {% set result = get_single_value (query, default = 0 ) %}    

 {%- if result > interval -%}
 
 {{ generate_series(upper_bound=result) }} 
 {%- else -%}
 select 1 result where 1=2
 
 {% endif %}


{% endtest %}