{% test trn_rowcount(model,date_column,key_columns,bronze_table) %}

{% set date_columns -%}
    {%- if "None" not in date_column -%}
    ,max({{date_column}}) {{date_column}}
    {% else %}
    ,'dummycol' dummycol 
    {%- endif -%}
{%- endset %} 


select {{key_columns}} {{date_columns}}
from
{{bronze_table}}
group by {{key_columns}}
except
select   {{star_renamed(from=model,single_column=key_columns)}} {{date_columns}}
from
{{model}}
group by  {{star_renamed(from=model,single_column=key_columns)}}


{% endtest %}