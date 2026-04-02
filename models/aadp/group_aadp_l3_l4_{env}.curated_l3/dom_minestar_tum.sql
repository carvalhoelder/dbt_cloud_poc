{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        schema='curated_l3',
        matched_condition = generate_matched_condition(['system_code', 'site_code', 'asset_code', 'functional_location_code', 'site_reporting_group_code', 'process_code', 'site_system_shift_date_code', 'department_code', 'scenario_code', 'start_date_time', 'end_date_time', 'local_start_date_time', 'local_end_date_time', 'reason_code', 'duration', 'remark', 'remark_description', 'first_event_flag', 'is_active']) ,
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
        unique_key = ['dom_minestar_tum_sk']
    )
}}

WITH curated_l3_dom_minestar_tum
AS (
  SELECT DISTINCT st.system_code,
    stl.site_code,
    asset.asset_code,
    asset.functional_location AS functional_location_code,
    asset.site_reporting_group_code,
    asset.process_code,
    dc.site_system_shift_date_code AS site_system_shift_date_code,
    dc.local_start_date_time AS local_shift_start,
    dc.local_end_date_time AS local_shift_end,
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
    t.remark AS remark,
    t.remark_description AS remark_description,
    CASE WHEN ROW_NUMBER() OVER (PARTITION BY dom_minestar_raw_tum_sk ORDER BY dc.local_start_date_time) = 1
    THEN 1 ELSE 0 END AS first_event_flag,
    1 AS is_active
  FROM {{ ref('dom_minestar_raw_tum') }} AS t
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
      AND stl.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.system AS st
    ON st.system_code = stl.system_code
      AND st.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.time_usage_category_v2 AS tuc
    ON tuc.process_code = CASE 
        WHEN t.system_name = 'dynamo_operations_monitoring'
          THEN 'PRC'
        WHEN t.system_name = 'cat_minestar'
          THEN 'MIN'
        END
      AND tuc.category_code = t.reason_code
      AND tuc.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_system_shift_date AS dc
    ON dc.system_code = st.system_code
      AND dc.site_code = stl.site_code
      AND dc.local_start_date_time < t.local_end_date_time
      AND dc.local_end_date_time > t.local_start_date_time
      AND dc.__is_deleted = 'N'
  WHERE t.__is_deleted = 'N'
  ),
tum_keys
AS (
  SELECT DISTINCT system_code,
    site_code,
    asset_code,
    functional_location_code,
    site_reporting_group_code,
    process_code,
    department_code,
    scenario_code,
    is_active
  FROM curated_l3_dom_minestar_tum
  ),
curated_l3_dom_minestar_tum_times
AS (
  --- duration of events as they come from source
  SELECT system_code,
    site_code,
    asset_code,
    functional_location_code,
    site_reporting_group_code,
    process_code,
    site_system_shift_date_code,
    department_code,
    scenario_code,
    local_shift_start,
    local_shift_end,
    local_start_date_time,
    local_end_date_time,
    reason_code,
    remark,
    remark_description,
    first_event_flag,
    is_active
  FROM curated_l3_dom_minestar_tum
  
  UNION
  
  --- P200 for duration between previous event end & next event start
  SELECT system_code,
    site_code,
    asset_code,
    functional_location_code,
    site_reporting_group_code,
    process_code,
    site_system_shift_date_code,
    department_code,
    scenario_code,
    local_shift_start,
    local_shift_end,
    lag(local_end_date_time) OVER (
      PARTITION BY system_code,
      site_code,
      asset_code,
      functional_location_code,
      site_reporting_group_code,
      process_code,
      site_system_shift_date_code,
      department_code,
      scenario_code ORDER BY DATE_TRUNC('SECOND', local_start_date_time),
        DATE_TRUNC('SECOND', local_end_date_time)
      ) AS local_start_date_time,
    local_start_date_time AS local_end_date_time,
    '1000' AS reason_code,
    NULL AS remark,
    NULL AS remark_description,
    0 AS first_event_flag,
    is_active
  FROM curated_l3_dom_minestar_tum
  WHERE system_code = 'DOM'
    AND local_start_date_time >= local_shift_start
    AND local_end_date_time <= local_shift_end
  
  UNION
  
  --- P200 for duration between shift start & first event start
  SELECT DISTINCT system_code,
    site_code,
    asset_code,
    functional_location_code,
    site_reporting_group_code,
    process_code,
    site_system_shift_date_code,
    department_code,
    scenario_code,
    local_shift_start,
    local_shift_end,
    local_shift_start AS local_start_date_time,
    min(local_start_date_time) OVER (
      PARTITION BY system_code,
      site_code,
      asset_code,
      functional_location_code,
      site_reporting_group_code,
      process_code,
      site_system_shift_date_code,
      department_code,
      scenario_code ORDER BY DATE_TRUNC('SECOND', local_start_date_time) ASC
      ) AS local_end_date_time,
    '1000' AS reason_code,
    NULL AS remark,
    NULL AS remark_description,
    0 AS first_event_flag,
    is_active
  FROM curated_l3_dom_minestar_tum
  WHERE system_code = 'DOM'
    AND local_start_date_time >= local_shift_start
  
  UNION
  
  --- P200 for duration between last event end & shift end
  SELECT DISTINCT system_code,
    site_code,
    asset_code,
    functional_location_code,
    site_reporting_group_code,
    process_code,
    site_system_shift_date_code,
    department_code,
    scenario_code,
    local_shift_start,
    local_shift_end,
    max(local_end_date_time) OVER (
      PARTITION BY system_code,
      site_code,
      asset_code,
      functional_location_code,
      site_reporting_group_code,
      process_code,
      site_system_shift_date_code,
      department_code,
      scenario_code ORDER BY DATE_TRUNC('SECOND', local_end_date_time) DESC
      ) AS local_start_date_time,
    local_shift_end AS local_end_date_time,
    '1000' AS reason_code,
    NULL AS remark,
    NULL AS remark_description,
    0 AS first_event_flag,
    is_active
  FROM curated_l3_dom_minestar_tum
  WHERE system_code = 'DOM'
    AND local_end_date_time <= local_shift_end
  
  UNION
  
  --- P200 for missing data - Shifts w/o any events get a P200 of shift duration
  SELECT DISTINCT tum_ks.system_code,
    tum_ks.site_code,
    tum_ks.asset_code,
    tum_ks.functional_location_code,
    tum_ks.site_reporting_group_code,
    tum_ks.process_code,
    dc.site_system_shift_date_code AS site_system_shift_date_code,
    tum_ks.department_code,
    tum_ks.scenario_code,
    dc.local_start_date_time AS local_shift_start,
    dc.local_end_date_time AS local_shift_end,
    nvl(tum.local_start_date_time, dc.local_start_date_time) AS local_start_date_time,
    nvl(tum.local_end_date_time, dc.local_end_date_time) AS local_end_date_time,
    nvl(tum.reason_code, '1000') AS reason_code,
    NULL AS remark,
    NULL AS remark_description,
    nvl(tum.first_event_flag, 0) AS first_event_flag,
    tum_ks.is_active
  FROM tum_keys AS tum_ks
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_system_shift_date AS dc
    ON dc.system_code = tum_ks.system_code
      AND dc.site_code = tum_ks.site_code
  LEFT JOIN curated_l3_dom_minestar_tum AS tum
    ON dc.site_system_shift_date_code = tum.site_system_shift_date_code
      AND tum_ks.asset_code = tum.asset_code
  WHERE dc.system_code = 'DOM'
    AND dc.site_code = 'QVC'
    AND dc.reporting_date >= '2024-01-01'
  )
SELECT concat_ws('-', t.system_code, t.site_reporting_group_code, t.local_start_date_time, t.local_end_date_time, t.asset_code, t.reason_code) AS dom_minestar_tum_sk,
  t.system_code AS system_code,
  t.site_code AS site_code,
  t.asset_code AS asset_code,
  t.functional_location_code AS functional_location_code,
  t.site_reporting_group_code AS site_reporting_group_code,
  t.process_code AS process_code,
  t.site_system_shift_date_code AS site_system_shift_date_code,
  t.department_code AS department_code,
  t.scenario_code AS scenario_code,
  TO_UTC_TIMESTAMP(t.local_start_date_time, 'America/Lima') AS start_date_time,
  TO_UTC_TIMESTAMP(t.local_end_date_time, 'America/Lima') AS end_date_time,
  t.local_start_date_time AS local_start_date_time,
  t.local_end_date_time AS local_end_date_time,
  t.reason_code AS reason_code,
  unix_timestamp(t.local_end_date_time) - unix_timestamp(t.local_start_date_time) AS duration,
  t.remark AS remark,
  t.remark_description AS remark_description,
  t.first_event_flag AS first_event_flag,
  t.is_active AS is_active,
  {{add_tech_columns(this)}}
FROM curated_l3_dom_minestar_tum_times AS t
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
 WHERE t.local_start_date_time >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}})
   AND t.local_start_date_time < t.local_end_date_time 