{% test scd_rowcount(model,date_column,key_columns,bronze_table) %}

{% set date_column %}
    {%- if 'VersionEndUTC' in  date_column -%} 
    VersionEndUTC
    {% endif %}  
 {% endset %}  

select {{date_column}},{{key_columns}}
from
{{bronze_table}}
except
select {{date_column}},{{key_columns}}
from
{{model}}


{% endtest %}