{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        schema='curated_l3',
        matched_condition = generate_matched_condition(['site_name', 'source_system_name']) ,
        tags = ['incremental'],
        target_alias = "tgt",
        source_alias = "src", 
        meta =  { 
        "partition_column": partition_col,
        "interval": interval, 
        "interval_type": interval_type,
        "interval_delimiter": interval_delimiter},
not_matched_by_source_condition = generate_not_matched_by_source_condition(model, partition_col, interval, interval_type, interval_delimiter),
not_matched_by_source_action = generate_invalidate_action(model),
        pre_hook = "  DELETE FROM {{ this }}
WHERE CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN site_name ELSE '{{var('site')}}' END LIKE CONCAT ('%', site_name, '%')
 AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN source_system_name ELSE '{{var('system')}}' END LIKE CONCAT ('%', source_system_name, '%')  " if is_incremental() else "",
        unique_key = ['site_code', 'system_code', 'block']
    )
}}

with block_data as (
    select site_name,source_system_name,block 
    from {{ ref('surface_manager_hole') }} AS A
    where A.__is_deleted = 'N'
    union 
    select site_name,source_system_name,block 
    from group_aadp_l3_l4_{{var('env')}}.staging_l3.rockma_hole AS A
    where A.__is_deleted = 'N'
    union 
    select site_name,source_system_name,block 
    from group_aadp_l3_l4_{{var('env')}}.staging_l3.modular_dispatch_hole AS A
    where A.__is_deleted = 'N' 
    union 
    select site_name,source_system_name,block 
    from group_aadp_l3_l4_{{var('env')}}.staging_l3.flanders_hole AS A
    where A.__is_deleted = 'N' 
)
select 
  sl.site_id AS site_code,
  sl.system_id AS system_code, 
  bd.block,
  bd.site_name, 
  bd.source_system_name,
  "N" AS _is_dbt,
  {{add_tech_columns(this)}}
from block_data AS bd
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS sl
ON lower(bd.site_name) = lower(sl.source_site_code) 
    AND lower(bd.source_system_name) = lower(sl.source_system_name)
    AND sl.`__is_deleted` = 'N' 