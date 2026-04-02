{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        schema='curated_l4',
        matched_condition = generate_matched_condition(['is_aborted', 'is_redrill', 'is_manual_meter', 'is_autonomous_meter', 'drilled_meters', 'drilling_duration', 'aborted_meters', 'exception_flag', 'is_active', 'is_one_touch_meter']) ,
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
        pre_hook = "{% if is_incremental() %} DELETE FROM {{ this }} AS A
WHERE A.__is_deleted = 'N' AND EXISTS ( Select 1 FROM group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS s 
JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys ON sys.__is_deleted = 'N' and sys.system_sk = A.system_sk 
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS msl ON msl.__is_deleted = 'N' and msl.site_id = s.site_code 
WHERE s.__is_deleted = 'N' and s.site_sk = A.site_sk
 AND CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN msl.source_site_code ELSE '{{var('site')}}' END LIKE CONCAT ('%', msl.source_site_code, '%')
 AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN msl.system_id ELSE '{{var('system')}}' END LIKE CONCAT ('%', msl.system_id, '%') 
 AND start_time_drilling >= date_sub(current_date(),{{var('refresh_days')}})
) " if is_incremental() else "",
        unique_key = ['system_sk', 'site_sk', 'date_sk', 'site_system_shift_date_sk', 'asset_sk', 'scenario_sk', 'process_sk', 'department_sk', 'site_reporting_group_sk', 'reporting_period_sk', 'functional_location_sk', 'hole_sk', 'block_sk', 'start_time_drilling', 'end_time_drilling']
    )
}}

SELECT
  NVL(sys.system_sk, -1) AS system_sk,
  NVL(dsv2.site_sk, -1) AS site_sk,
  NVL(dd.date_sk, -1) AS date_sk,
  -1 AS site_system_shift_date_sk,
  NVL(asset.asset_sk, -1) AS asset_sk,
  NVL(ds.scenario_sk, -1) AS scenario_sk,
  NVL(asset.process_sk, -1) AS process_sk,
  -1 AS department_sk,
  NVL(asset.site_reporting_group_sk, -1) AS site_reporting_group_sk,
  NVL(dd.date_sk, -1) AS reporting_period_sk,
  NVL(asset.functional_location_sk, -1) AS functional_location_sk,
  NVL(dh.hole_sk, -1) AS hole_sk,
  NVL(db.block_sk, -1) AS block_sk,
  d.start_time_drilling,
  d.end_time_drilling,
  d.is_aborted,
  d.is_redrill,
  d.is_manual_meter,
  d.is_autonomous_meter,
  d.is_one_touch_meter,
  sum(d.drilled_meters) AS drilled_meters,
  d.drilling_duration,
  d.drilling_duration_rate,
  sum(d.aborted_meters) AS aborted_meters,
  TRUE AS is_active,
  0 AS exception_flag,
  {{add_tech_columns(this)}}
FROM {{ ref('drilling') }} AS d
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS dsv2 ON lower(dsv2.site_code) = lower(d.site_code) AND dsv2.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys ON lower(sys.system_code) = lower(d.system_code) AND sys.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_scenario AS ds ON ds.scenario_code = 'ACT' AND ds.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dd ON dd.DATE = d.reporting_date AND dd.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.asset_lookup AS al ON al.__is_deleted = 'N' AND al.source_system_name = d.source_system_name AND al.source_asset_id = d.asset_name
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_asset AS asset ON lower(asset.asset_code) = lower(al.asset_code) AND asset.__is_deleted = 'N' 
    AND asset.site_sk = dsv2.site_sk --AND asset.system_sk = sys.system_sk
  JOIN {{ ref('dim_hole') }} AS dh ON dh.__is_deleted = 'N'
    AND dh.site_code = d.site_code
    AND dh.system_code = d.system_code
    AND dh.hole_id = d.hole_id
    AND dh.hole_name = d.hole_name
    AND dh.block = d.block
  JOIN {{ ref('dim_block') }} AS db ON db.__is_deleted = 'N'
    AND db.site_code = d.site_code
    AND db.system_code = d.system_code
    AND db.block = d.block
WHERE
  d.__is_deleted = 'N'
  AND CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN d.site_code ELSE '{{var('site')}}' END LIKE CONCAT ('%', d.site_code, '%')
  AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN d.system_code ELSE '{{var('system')}}' END LIKE CONCAT ('%', d.system_code, '%') 
  AND d.start_time_drilling >= date_sub(current_date(), {{var('refresh_days')}})
GROUP BY
  NVL(sys.system_sk, -1),
  NVL(dsv2.site_sk, -1),
  NVL(asset.asset_sk, -1),
  NVL(dd.date_sk, -1),
  NVL(dh.hole_sk, -1),
  NVL(db.block_sk, -1),
  NVL(ds.scenario_sk, -1),
  NVL(asset.process_sk, -1),
  NVL(asset.site_reporting_group_sk, -1),
  NVL(asset.functional_location_sk, -1),
  d.start_time_drilling,
  d.end_time_drilling,
  d.is_aborted,
  d.is_redrill,
  d.is_manual_meter,
  d.is_autonomous_meter,
  d.is_one_touch_meter,
  d.drilling_duration,
  d.drilling_duration_rate