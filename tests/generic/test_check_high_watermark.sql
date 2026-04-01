{% test check_high_watermark(column_name,model) %}

{% set query %}
 {%- if ('VersionStartUTC' ==  column_name) or ('VersionEndUTC' ==  column_name) -%} 
 select greatest(max(VersionStartUTC),max(case when VersionEndUTC<'9999-12-31' then VersionEndUTC else null end)) BronzeHw from  {{model}}
  {%- else -%}
 select max({{column_name}}) BronzeHw from {{model}}
 {% endif %}  
 {% endset %}  

select 
cast(BronzeHw as datetime) BronzeHw
from [MetaLoad].[HighWatermark]
where
'"'+db_name()+'"."'+schemaName+'"."'+TableName+'"'=' {{model}}'
except
{{query}}

{% endtest %}
		    