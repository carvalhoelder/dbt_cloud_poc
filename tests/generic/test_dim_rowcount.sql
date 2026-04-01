{% test dim_rowcount(model,date_column,key_columns,bronze_table) %}

{% set date_columns -%}
    {%- if "None" not in date_column -%}
    ,max({{date_column}}) {{date_column}}
    {% else %}
    ,'dummycol' dummycol 
    {%- endif -%}
{%- endset %} 

{%- set select_key_columns %}
{%- set key_columns = key_columns.split(',') %}
{%- if key_columns is string %}
    "{{key_columns}}"
{%- elif key_columns is iterable %}
   {%- for item in key_columns %} "{{ item }}"{% if not loop.last %}, {% endif %}{% endfor %}
{%- endif %}
{%- endset %}





select 

{{select_key_columns}}  {{date_columns}}
from
{{bronze_table}}
group by 
{{select_key_columns}} 
except
select  {{star_renamed(from=model,single_column=key_columns)}} {{date_columns}}
from
{{model}}
group by {{star_renamed(from=model,single_column=key_columns)}}


{% endtest %}