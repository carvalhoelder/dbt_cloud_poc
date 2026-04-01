{% macro log_results(results) %}

  {% if execute %}
  {{ log("========== Begin Summary ==========", info=True) }}
  {% for res in results -%}
    {% set line -%}
        node: {{ res.node.unique_id }}; status: {{ res.status }} (message: {{ res.message }}) number of records: {{res.adapter_response}} 
    {%- endset %}

    {{ log(line, info=True) }}
  {% endfor %}
  {{ log("========== End Summary ==========", info=True) }}
  {% endif %}

{%- endmacro %}