{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        schema='curated_l3',
        matched_condition = generate_matched_condition(['system_code', 'site_code', 'asset_code', 'functional_location_code', 'site_reporting_group_code', 'process_code', 'site_system_shift_date_code', 'department_code', 'scenario_code', 'start_date_time', 'end_date_time', 'local_start_date_time', 'local_end_date_time', 'reason_code', 'duration', 'remark_description', 'first_event_flag', 'is_active']) ,
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
        pre_hook = "  DELETE FROM {{ this }} AS tum
 WHERE CASE
        WHEN '{{var('system')}}' in ('ALL', '') then NVL(system_code, '')
                         ELSE '{{var('system')}}'
                       END LIKE CONCAT('%', NVL(system_code, ''), '%')
 AND EXISTS (SELECT 1
                 FROM group_aadp_l3_l4_{{var('env')}}.curated_l3.site_V2 AS st
                WHERE st.site_code = tum.site_code
                  AND st.__is_deleted = 'N'
                  AND CASE
                        WHEN '{{var('site')}}' in ('ALL', '') then NVL(st.site_code, '')
                        ELSE '{{var('site')}}'
                      END LIKE CONCAT('%', NVL(st.site_code, ''), '%')
              )
   AND start_date_time >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}}) " if is_incremental() else "",
        unique_key = ['surface_manager_tum_sk']
    )
}}

WITH curated_l3_surface_manager_tum
AS (
  SELECT st.system_code,
         stl.site_code,
         asset.asset_code,
         asset.functional_location AS functional_location_code,
         asset.site_reporting_group_code,
         asset.process_code,
         dc.site_system_shift_date_code AS site_system_shift_date_code,
         'NOT_APPLICABLE' AS department_code,
         'ACT' AS scenario_code,
         date_trunc('SECOND', CASE 
           WHEN dc.local_start_date_time > t.local_start_date_time
           THEN dc.local_start_date_time
           ELSE t.local_start_date_time
         END) AS local_start_date_time,
         date_trunc('SECOND', CASE 
           WHEN dc.local_end_date_time < t.local_end_date_time
           THEN dc.local_end_date_time
           ELSE t.local_end_date_time
         END) AS local_end_date_time,
         tuc.time_usage_category_code AS reason_code,
         t.remark_description,
         CASE WHEN ROW_NUMBER() OVER (PARTITION BY surface_manager_raw_tum_sk ORDER BY dc.local_start_date_time) = 1
              THEN 1 ELSE 0 END AS first_event_flag
    FROM {{ ref('surface_manager_raw_tum') }} AS t
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.asset_lookup AS assetl
      ON assetl.source_system_name = t.system_name
     AND assetl.source_site_code = t.site_name
     AND assetl.source_asset_id = t.asset_name
     AND assetl.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.asset AS asset
      ON asset.asset_code = assetl.asset_code
     AND asset.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_lookup AS stl
      ON stl.source_system_name = t.system_name
     AND stl.source_site_code = t.site_name
     AND stl.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.system AS st
      ON st.system_code = stl.system_code
     AND st.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.time_usage_category_v2 AS tuc
      ON tuc.category_code = t.reason_code
     AND tuc.process_code = 'MIN'
     AND tuc.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_system_shift_date AS dc
      ON dc.system_code = st.system_code
     AND dc.site_code = stl.site_code
     AND dc.local_start_date_time < t.local_end_date_time
     AND dc.local_end_date_time > t.local_start_date_time
     AND dc.__is_deleted = 'N'
   WHERE t.__is_deleted = 'N'
   
   UNION
   
   SELECT st.system_code,
         stl.site_code,
         asset.asset_code,
         asset.functional_location AS functional_location_code,
         asset.site_reporting_group_code,
         asset.process_code,
         dc.site_system_shift_date_code AS site_system_shift_date_code,
         'NOT_APPLICABLE' AS department_code,
         'ACT' AS scenario_code,
         date_trunc('SECOND', CASE 
           WHEN dc.local_start_date_time > t.local_start_date_time
           THEN dc.local_start_date_time
           ELSE t.local_start_date_time
         END) AS local_start_date_time,
         date_trunc('SECOND', CASE 
           WHEN dc.local_end_date_time < t.local_end_date_time
           THEN dc.local_end_date_time
           ELSE t.local_end_date_time
         END) AS local_end_date_time,
         tuc.time_usage_category_code AS reason_code,
         t.remark_description,
         CASE WHEN ROW_NUMBER() OVER (PARTITION BY surface_manager_raw_tum_sk ORDER BY dc.local_start_date_time) = 1
              THEN 1 ELSE 0 END AS first_event_flag
    FROM group_aadp_l3_l4_{{var('env')}}.curated_l3.surface_manager_raw_tum_lbr AS t
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.asset_lookup AS assetl
      ON assetl.source_system_name = t.system_name
     AND assetl.source_site_code = t.site_name
     AND assetl.source_asset_id = t.asset_name
     AND assetl.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.asset AS asset
      ON asset.asset_code = assetl.asset_code
     AND asset.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_lookup AS stl
      ON stl.source_system_name = t.system_name
     AND stl.source_site_code = t.site_name
     AND stl.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.system AS st
      ON st.system_code = stl.system_code
     AND st.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.time_usage_category_v2 AS tuc
      ON tuc.category_code = t.reason_code
     AND tuc.process_code = 'MIN'
     AND tuc.__is_deleted = 'N'
    JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_system_shift_date AS dc
      ON dc.system_code = st.system_code
     AND dc.site_code = stl.site_code
     AND dc.local_start_date_time < t.local_end_date_time
     AND dc.local_end_date_time > t.local_start_date_time
     AND dc.__is_deleted = 'N'
   WHERE t.__is_deleted = 'N'
  )
SELECT concat_ws('-',
                 t.system_code,
                 t.site_reporting_group_code,
                 t.local_start_date_time,
                 t.local_end_date_time,
                 t.asset_code,
                 t.reason_code) AS surface_manager_tum_sk, 
       t.system_code AS system_code,
       t.site_code AS site_code,
       NVL(t.asset_code,'NOT_APPLICABLE') AS asset_code,
       NVL(t.functional_location_code,'NOT_APPLICABLE') AS functional_location_code,
       NVL(t.site_reporting_group_code,'NOT_APPLICABLE') AS site_reporting_group_code,
       NVL(t.process_code,'NOT_APPLICABLE') AS process_code,
       NVL(t.site_system_shift_date_code,'NOT_APPLICABLE') AS site_system_shift_date_code,
       t.department_code AS department_code,
       t.scenario_code AS scenario_code,
       date_trunc('DAY', TO_UTC_TIMESTAMP(t.local_start_date_time, ms.time_zone_region_city)) AS date,
       TO_UTC_TIMESTAMP(t.local_start_date_time, ms.time_zone_region_city) AS start_date_time,
       TO_UTC_TIMESTAMP(t.local_end_date_time, ms.time_zone_region_city) AS end_date_time,
       t.local_start_date_time AS local_start_date_time,
       t.local_end_date_time AS local_end_date_time,
       t.reason_code AS reason_code,
       unix_timestamp(t.local_end_date_time) - unix_timestamp(t.local_start_date_time) AS duration,
       concat_ws(',', collect_list(t.remark_description)) as remark_description,
       sum(first_event_flag) AS first_event_flag,
       TRUE AS is_active,
       {{add_tech_columns(this)}}
  FROM curated_l3_surface_manager_tum AS t
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.system AS sys
    ON sys.system_code = t.system_code
   AND sys.__is_deleted = 'N'
   AND CASE
         WHEN '{{var('system')}}' in ('ALL', '') then NVL(sys.system_code, '')
         ELSE '{{var('system')}}'
       END LIKE CONCAT('%', NVL(sys.system_code, ''), '%')
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_v2 AS st
    ON st.site_code = t.site_code
   AND st.__is_deleted = 'N'
   AND CASE
         WHEN '{{var('site')}}' in ('ALL', '') then NVL(st.site_code, '')
         ELSE '{{var('site')}}'
       END LIKE CONCAT('%', NVL(st.site_code, ''), '%')
  JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS ms
    ON ms.site_code = st.site_code
   AND ms.__is_deleted = 'N'
 WHERE t.local_start_date_time >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}})
   AND t.local_start_date_time < t.local_end_date_time 
 group by t.system_code,
       t.site_code,
       t.asset_code,
       t.functional_location_code,
       t.site_reporting_group_code,
       t.process_code,
       t.site_system_shift_date_code,
       t.department_code,
       t.scenario_code,
       date_trunc('DAY', TO_UTC_TIMESTAMP(t.local_start_date_time, ms.time_zone_region_city)),
       TO_UTC_TIMESTAMP(t.local_start_date_time, ms.time_zone_region_city),
       TO_UTC_TIMESTAMP(t.local_end_date_time, ms.time_zone_region_city),
       t.local_start_date_time,
       t.local_end_date_time,
       t.reason_code 