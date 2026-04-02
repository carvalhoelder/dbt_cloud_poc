{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        schema='curated_l4',
        matched_condition = generate_matched_condition(['tum_duration', 'tum_event_count', 'reason', 'remark', 'remark_description', 'is_active', 'exception_flag']) ,
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
        pre_hook = "  DELETE FROM {{this}} AS fact
WHERE EXISTS (
    SELECT 1
    FROM group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS site, group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys, group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dd
    WHERE fact.site_sk = site.site_sk
      AND site.__is_deleted = 'N'
      AND fact.system_sk = sys.system_sk
      AND sys.__is_deleted = 'N'
      AND fact.date_sk = dd.date_sk
      AND dd.__is_deleted = 'N'
      AND CASE 
        WHEN '{{var('system')}}' IN ('ALL', '')
          THEN NVL(sys.system_code, '')
        ELSE '{{var('system')}}'
        END LIKE CONCAT ('%', NVL(sys.system_code, ''), '%')
      AND CASE 
        WHEN '{{var('site')}}' IN ('ALL', '')
          THEN NVL(site.site_code, '')
        ELSE '{{var('site')}}'
        END LIKE CONCAT ('%', NVL(site.site_code, ''), '%')
      AND dd.DATE >= date_sub(CURRENT_DATE (), {{var('refresh_days')}})
    ) " if is_incremental() else "",
        unique_key = ['system_sk', 'site_sk', 'date_sk', 'site_system_shift_date_sk', 'asset_sk', 'scenario_sk', 'process_sk', 'department_sk', 'site_reporting_group_sk', 'reporting_period_sk', 'functional_location_sk', 'time_usage_category_sk', 'start_date_time', 'end_date_time']
    )
}}

SELECT nvl(sys.system_sk, - 1) AS system_sk,
  nvl(site.site_sk, - 1) AS site_sk,
  nvl(dim_date.date_sk, - 1) AS date_sk,
  nvl(shift.site_system_shift_date_sk, - 1) AS site_system_shift_date_sk,
  nvl(asset.asset_sk, - 1) AS asset_sk,
  nvl(scenario.scenario_sk, - 1) AS scenario_sk,
  nvl(asset.process_sk, - 1) AS process_sk,
  - 1 AS department_sk,
  nvl(asset.site_reporting_group_sk, - 1) AS site_reporting_group_sk,
  nvl(CAST(date_format(dim_date.start_of_month, 'yyyyMMdd') AS INT), - 1) AS reporting_period_sk,
  nvl(asset.functional_location_sk, - 1) AS functional_location_sk,
  nvl(tuc.time_usage_category_sk, - 1) AS time_usage_category_sk,
  tum.local_start_date_time AS start_date_time,
  tum.local_end_date_time AS end_date_time,
  tum.duration AS tum_duration,
  CASE 
    WHEN tuc.primary_category_code IN (
        'L100',
        'L200',
        'L300',
        'D100',
        'D200',
        'D300'
        )
      THEN 1
    ELSE 0
    END AS tum_event_count,
  tuc.category_name AS reason,
  tum.remark AS remark,
  tum.remark_description AS remark_description,
  1 AS is_active,
  0 AS exception_flag,
  {{add_tech_columns(this)}}
FROM {{ ref('dom_minestar_tum') }} AS tum
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys
  ON sys.system_code = tum.system_code
    AND sys.`__is_deleted` = 'N'
    AND CASE
         WHEN '{{var('system')}}' in ('ALL', '') then NVL(sys.system_code, '')
         ELSE '{{var('system')}}'
       END LIKE CONCAT('%', NVL(sys.system_code, ''), '%')
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS site
  ON site.site_code = tum.site_code
    AND site.`__is_deleted` = 'N'
    AND CASE
         WHEN '{{var('site')}}' in ('ALL', '') then NVL(site.site_code, '')
         ELSE '{{var('site')}}'
    END LIKE CONCAT('%', NVL(site.site_code, ''), '%')
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_asset AS asset
  ON asset.asset_code = tum.asset_code
    AND asset.`__is_deleted` = 'N'
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_scenario AS scenario
  ON scenario.scenario_code = tum.scenario_code
    AND scenario.__is_deleted = 'N'
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_time_usage_category AS tuc
  ON tuc.category_code = tum.reason_code
    AND tuc.`__is_deleted` = 'N'
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_process AS process
  ON process.process_sk = tuc.process_sk
    AND process.__is_deleted = 'N'
    AND process.process_code = CASE 
      WHEN tum.system_code = 'Mine_Star'
        THEN 'MIN'
      WHEN tum.system_code = 'DOM'
        THEN 'PRC'
      END
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_system_shift_date AS shift
  ON shift.site_system_shift_date_code = tum.site_system_shift_date_code
    AND shift.is_active = TRUE
    AND shift.__is_deleted = 'N'
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dim_date
  ON dim_date.`date` = shift.reporting_date
    AND dim_date.`__is_deleted` = 'N'
    AND dim_date.date >= date_sub(CURRENT_DATE(), {{var('refresh_days')}})
WHERE tum.__is_deleted = 'N'  