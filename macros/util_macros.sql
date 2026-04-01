{% macro generate_matched_condition(filtered_columns=[]) %}
 {{ log("filtered_columns: " ~ filtered_columns ) }}

  {#- Build the final string: OR NOT src.col <=> tgt.col -#}
  {%- set condition_parts = [] -%}
  {%- for col in filtered_columns -%}
    {%- do condition_parts.append(" OR NOT src." ~ col ~ " <=> tgt." ~ col) -%}
  {%- endfor -%}

  {{ return("tgt.__is_deleted = 'Y' " ~ condition_parts | join(' ')) }}
{% endmacro %}


{% macro get_model_config_values(model_ref) %}
    {%- set table_name = model_ref.identifier -%}

    {% for node in graph.nodes.values() %}
        {%- set model_name = node.unique_id.split('.')[-1] -%}
        {%- if table_name == model_name -%}
            {%- set model_config = node.config -%}
            {{ return(model_config) }}
        {%- endif -%}
    {% endfor %}
{% endmacro %}

{%- macro add_tech_columns(model_ref) -%}
current_timestamp() AS __inserted_date
,current_timestamp() AS __modified_date
,current_timestamp() AS curation_l3_ts
,current_timestamp() AS curation_l3_exec_ts
,'qvc_lx_dly' AS curation_l3_pipeline_name
,'scheduled__' AS curation_l3_pipeline_run_id
,'N' AS __is_deleted
{%- endmacro %}

{% macro get_model_meta(model_name,attribute,default_value) %}
{ {{ log("Model name is : " ~ model_name.name ) }}
  {% if execute %}
    {% set nodes = graph.nodes.values() | selectattr("name", "equalto", model_name) | list %}
    {% if nodes %}
      {% do return(nodes[0].config.meta.get(attribute)) %}
    {% endif %}
  {% endif %}
{% endmacro %}

{%- macro generate_invalidate_action(model_ref) -%}
update set tgt.__is_deleted = 'Y', tgt.__modified_date = current_timestamp()
{%- endmacro %}

{%- macro generate_not_matched_by_source_condition(model_ref=None, partition_column='__modified_date', interval=99, interval_type='years', interval_delimiter="'") -%}
    {#
        In config blocks the model's own config/meta values are not yet available when
        the macro is executed, so reading `model_ref.config.meta` will return an empty
        dict and fall back to the defaults above.  For that reason we allow callers to
        pass the four values explicitly and treat the `model_ref` argument as purely
        optional.  If a valid `model_ref` is supplied from a context where the config has
        already been merged (e.g. inside a post‑parse hook) we still try to merge the
        values from its meta.
    #}
    {%- if model_ref -%}
        {%- set cfg = model_ref.config.meta -%}
        {%- set partition_column = cfg.get('partition_column', partition_column) -%}
        {%- set interval = cfg.get('interval', interval) -%}
        {%- set interval_type = cfg.get('interval_type', interval_type) -%}
        {%- set interval_delimiter = cfg.get('interval_delimiter', interval_delimiter) -%}
    {%- endif -%}
    {{ return("tgt.__is_deleted = 'N' AND tgt." ~ partition_column ~ " < current_timestamp() - interval " ~ interval_delimiter ~ interval ~ interval_delimiter ~ " " ~ interval_type) }}
{%- endmacro %}