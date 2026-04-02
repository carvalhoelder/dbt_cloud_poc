{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "table",
        schema='staging_l3',
        tags = ['overwrite'],
        target_alias = "tgt",
        source_alias = "src", 
        meta =  { 
        "partition_column": partition_col,
        "interval": interval, 
        "interval_type": interval_type,
        "interval_delimiter": interval_delimiter},
        unique_key = ['site_name', 'source_system_name', 'hole_id', 'hole_name', 'block']
    )
}}

with qvc_drill_data AS (
select
   dh.site_name ,
   dh.source_system_name ,
   dh.hole_id AS Hole_Id,
   dh.hole_name AS Hole_Name,
   dp.Name AS block
FROM {{ source('surface_manager_l2', 'vw_drilled_hole') }} AS dh
  JOIN {{ source('surface_manager_l2', 'vw_drill_plan') }} AS dp ON dh.Drill_Plan_Id = dp.Id AND dp.`__is_deleted`='N'
  JOIN {{ source('surface_manager_l2', 'vw_rig') }} AS R ON R.Serial_Number = dh.Rig_Serial_Number AND R.`__is_deleted`='N'
  JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS msl ON msl.source_system_name = dh.source_system_name AND msl.source_site_code = dh.site_name and msl.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_system_shift_date AS sh ON sh.site_code = msl.site_id AND sh.system_code = msl.system_id AND ( sh.utc_start_date_time <= dh.start_hole_time and sh.utc_end_date_time >= dh.start_hole_time ) AND sh.__is_deleted = 'N'
  WHERE  dh.`__is_deleted` = 'N' 
  AND dh.site_name in ('quellaveco')
  AND (dh.Start_Point_Z - dh.End_Point_Z) >= 0 
  AND dh.start_hole_time >= '2021-01-01'
  AND CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN msl.site_id ELSE '{{var('site')}}' END LIKE CONCAT ('%', msl.site_id, '%')
  AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN msl.system_id ELSE '{{var('system')}}' END LIKE CONCAT ('%', msl.system_id, '%') 
--  AND cast(dh.start_hole_time as date) >= date_sub(current_date(), {{var('refresh_days')}})
 )
select distinct site_name, 
    source_system_name,
    hole_id,
    hole_name,
    block,
    {{add_tech_columns(this)}}
from qvc_drill_data AS A 