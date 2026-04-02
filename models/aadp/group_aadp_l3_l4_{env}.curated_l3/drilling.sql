{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        schema='curated_l3',
        matched_condition = generate_matched_condition(['site_name', 'source_system_name', 'is_aborted', 'is_redrill', 'is_manual_meter', 'is_autonomous_meter', 'is_one_touch_meter', 'drilled_meters', 'drilling_duration', 'aborted_meters']) ,
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
 AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN source_system_name ELSE '{{var('system')}}' END LIKE CONCAT ('%', source_system_name, '%') 
 AND start_time_drilling >= date_sub(current_date(), {{var('refresh_days')}}) " if is_incremental() else "",
        unique_key = ['site_code', 'system_code', 'reporting_date', 'asset_name', 'hole_id', 'hole_name', 'block', 'start_time_drilling', 'end_time_drilling']
    )
}}

WITH drilling_data AS (
  SELECT site_name,
         source_system_name,
         reporting_date, 
         asset_name, 
         hole_id,
         hole_name,
         block,
         cast (start_time_drilling as timestamp) AS start_time_drilling,
         cast (end_time_drilling as timestamp) AS end_time_drilling,
         0 AS is_aborted,
         0 AS is_redrill,
         0 AS is_manual_meter,
         NULL AS is_autonomous_meter,
         NULL AS is_one_touch_meter,
         drilled_meters,
         NULL AS aborted_meters
    from {{ ref('surface_manager_drilling') }} AS A
   where A.__is_deleted = 'N'

   UNION

  SELECT site_name, 
         source_system_name,
         Reporting_Date, 
         asset_name, 
         hole_id,
         hole_name,
         block,
         start_time_drilling,
         end_time_drilling,
         is_aborted,
         is_redrill, 
         is_manual_meter,
         NULL AS is_autonomous_meter,
         NULL AS is_one_touch_meter,
         drilled_meters,
         aborted_meters
    from group_aadp_l3_l4_{{var('env')}}.staging_l3.rockma_drilling AS A
   where A.__is_deleted = 'N'

   UNION

  SELECT site_name,
         source_system_name,
         Reporting_Date,
         asset_name,
         hole_id,
         hole_name,
         block,
         start_time_drilling,
         end_time_drilling,
         is_aborted,
         is_redrill,
         0 AS is_manual_meter,
         NULL AS is_autonomous_meter,
         NULL AS is_one_touch_meter,
         drilled_meters,
         NULL AS aborted_meters
    from group_aadp_l3_l4_{{var('env')}}.staging_l3.modular_dispatch_drilling AS A
   where A.__is_deleted = 'N'

   UNION

  SELECT site_name,
         source_system_name,
         Reporting_Date,
         asset_name,
         hole_id,
         hole_name,
         block,
         start_time_drilling,
         end_time_drilling,
         is_aborted,
         is_redrill,
         is_manual_meter,
         is_autonomous_meter,
         is_one_touch_meter,
         drilled_meters,
         NULL AS aborted_meters
    from group_aadp_l3_l4_{{var('env')}}.staging_l3.flanders_drilling AS A
   where A.__is_deleted = 'N'
)
SELECT sl.site_id AS site_code,
       sl.system_id AS system_code,
       dd.reporting_date,
       dd.asset_name,
       dd.hole_id,
       dd.hole_name,
       dd.block,
       dd.start_time_drilling,
       dd.end_time_drilling,
       dd.site_name,
       dd.source_system_name,
       dd.is_aborted,
       dd.is_redrill,
       dd.is_manual_meter,
       dd.is_autonomous_meter,
       dd.is_one_touch_meter,
       dd.drilled_meters,
       timestampdiff(SECOND, dd.start_time_drilling, dd.end_time_drilling) AS drilling_duration,
       dd.aborted_meters,
       {{add_tech_columns(this)}}
  from drilling_data AS dd
  JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS sl
    ON lower(dd.site_name) = lower(sl.source_site_code)
   AND lower(dd.source_system_name) = lower(sl.source_system_name)
   AND sl.`__is_deleted` = 'N' 