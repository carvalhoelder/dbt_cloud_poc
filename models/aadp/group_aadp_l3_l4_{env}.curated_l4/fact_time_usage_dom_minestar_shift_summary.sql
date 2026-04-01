{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['t000', 't100', 't200', 't300', 'p100', 'p200', 'l000', 'l100', 'l200', 'l300', 'd000', 'd100', 'd200', 'd300', 'n000', 'n100', 'n200', 'stop_count', 'd100_event_count', 'is_active', 'exception_flag']) ,
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
        pre_hook = "  DELETE FROM {{ this }} AS fact
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
        unique_key = ['system_sk', 'site_sk', 'date_sk', 'site_system_shift_date_sk', 'asset_sk', 'scenario_sk', 'process_sk', 'department_sk', 'site_reporting_group_sk', 'reporting_period_sk', 'functional_location_sk']
    )
}}

WITH DOM_MNSTR_KEYS_CTE
AS (
  SELECT concat_ws('-', summary.system_code, summary.site_code, summary.asset_code, shift.site_system_shift_date_sk) AS sk,
    NVL(sys.system_sk, - 1) AS system_sk,
    NVL(site.site_sk, - 1) AS site_sk,
    NVL(dim_date.date_sk, - 1) AS date_sk,
    NVL(shift.site_system_shift_date_sk, - 1) AS site_system_shift_date_sk,
    NVL(asset.asset_sk, - 1) AS asset_sk,
    NVL(scenario.scenario_sk, - 1) AS scenario_sk,
    NVL(asset.process_sk, - 1) AS process_sk,
    - 1 AS department_sk,
    NVL(asset.site_reporting_group_sk, - 1) AS site_reporting_group_sk,
    NVL(tuc.time_usage_category_sk, - 1) AS time_usage_category_sk,
    NVL(CAST(date_format(dim_date.start_of_month, 'yyyyMMdd') AS INT), - 1) AS reporting_period_sk,
    NVL(asset.functional_location_sk, - 1) AS functional_location_sk,
    NVL(tuc.primary_category_code, - 1) AS primary_category_code,
    NVL(summary.duration, 0) AS duration,
    NVL(shift.duration_in_secs, 0) AS shift_duration,
    CASE 
      WHEN tuc.primary_category_code IN (
          'L100',
          'L200',
          'L300',
          'D100',
          'D200',
          'D300'
          )
      AND summary.first_event_flag = 1
        THEN 1
      ELSE 0
      END AS stop_count,
    CASE 
      WHEN tuc.primary_category_code = 'D100'
      AND summary.first_event_flag = 1
        THEN 1
      ELSE 0
      END AS d100_event_count
  FROM {{ ref('dom_minestar_tum') }} AS summary
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys
    ON sys.system_code = summary.system_code
      AND sys.`__is_deleted` = 'N'
      AND CASE
           WHEN '{{var('system')}}' in ('ALL', '') then NVL(sys.system_code, '')
           ELSE '{{var('system')}}'
         END LIKE CONCAT('%', NVL(sys.system_code, ''), '%')
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS site
    ON site.site_code = summary.site_code
      AND site.`__is_deleted` = 'N'
      AND CASE
           WHEN '{{var('site')}}' in ('ALL', '') then NVL(site.site_code, '')
           ELSE '{{var('site')}}'
      END LIKE CONCAT('%', NVL(site.site_code, ''), '%')
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_asset AS asset
    ON asset.asset_code = summary.asset_code
      AND asset.`__is_deleted` = 'N'
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_scenario AS scenario
    ON scenario.scenario_code = summary.scenario_code
      AND scenario.__is_deleted = 'N'
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_time_usage_category AS tuc
    ON tuc.category_code = summary.reason_code
      AND tuc.`__is_deleted` = 'N'
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_process AS process
    ON process.process_sk = tuc.process_sk
      AND process.__is_deleted = 'N'
      AND process.process_code = CASE 
        WHEN summary.system_code = 'Mine_Star'
          THEN 'MIN'
        WHEN summary.system_code = 'DOM'
          THEN 'PRC'
        END
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_system_shift_date AS shift
    ON shift.site_system_shift_date_code = summary.site_system_shift_date_code
      AND shift.is_active = TRUE
      AND shift.__is_deleted = 'N'
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dim_date
    ON dim_date.`date` = shift.reporting_date
      AND dim_date.`__is_deleted` = 'N'
      AND dim_date.date >= date_sub(CURRENT_DATE(), {{var('refresh_days')}})
  WHERE summary.__is_deleted = 'N'
  ),
DOM_MNSTR_L4_CTE AS (
  SELECT sk, date_sk,
    COALESCE(SUM(CASE WHEN primary_category_code = 'D100' THEN duration END), 0) AS D100,
    COALESCE(SUM(CASE WHEN primary_category_code = 'D200' THEN duration END), 0) AS D200,
    COALESCE(SUM(CASE WHEN primary_category_code = 'D300' THEN duration END), 0) AS D300,
    COALESCE(SUM(CASE WHEN primary_category_code = 'P100' THEN duration END), 0) AS P100,
    COALESCE(SUM(CASE WHEN primary_category_code = 'P200' THEN duration END), 0) AS P200,
    COALESCE(SUM(CASE WHEN primary_category_code = 'L100' THEN duration END), 0) AS L100,
    COALESCE(SUM(CASE WHEN primary_category_code = 'L200' THEN duration END), 0) AS L200,
    COALESCE(SUM(CASE WHEN primary_category_code = 'L300' THEN duration END), 0) AS L300,
    COALESCE(SUM(CASE WHEN primary_category_code = 'N100' THEN duration END), 0) AS N100,
    COALESCE(SUM(CASE WHEN primary_category_code = 'N200' THEN duration END), 0) AS N200,
    SUM(stop_count) AS stop_count,
    SUM(d100_event_count) AS d100_event_count
  FROM DOM_MNSTR_KEYS_CTE
  GROUP BY sk, date_sk
  ),
DOM_MNSTR_DISTINCT_KEYS_CTE
AS (
  SELECT DISTINCT keys_cte.sk,
    keys_cte.system_sk,
    keys_cte.site_sk,
    keys_cte.date_sk,
    keys_cte.site_system_shift_date_sk,
    keys_cte.asset_sk,
    keys_cte.scenario_sk,
    keys_cte.process_sk,
    keys_cte.department_sk,
    keys_cte.site_reporting_group_sk,
    keys_cte.reporting_period_sk,
    keys_cte.functional_location_sk,
    keys_cte.shift_duration
  FROM DOM_MNSTR_KEYS_CTE AS keys_cte
  )
SELECT DISTINCT keys_cte.system_sk,
  keys_cte.site_sk,
  keys_cte.date_sk,
  keys_cte.site_system_shift_date_sk,
  keys_cte.asset_sk,
  keys_cte.scenario_sk,
  keys_cte.process_sk,
  keys_cte.department_sk,
  keys_cte.site_reporting_group_sk,
  keys_cte.reporting_period_sk,
  keys_cte.functional_location_sk,
  ROUND(keys_cte.shift_duration, 2) AS T000,
  ROUND((l4_cte.P100 + l4_cte.P200 + l4_cte.L100 + l4_cte.L200 + l4_cte.L300 + l4_cte.D100 + l4_cte.D200 + l4_cte.D300), 2) AS T100,
  ROUND((l4_cte.P100 + l4_cte.P200 + l4_cte.L100 + l4_cte.L200 + l4_cte.L300), 2) AS T200,
  ROUND((l4_cte.P100 + l4_cte.P200), 2) AS T300,
  ROUND(l4_cte.P100, 2) AS P100,
  ROUND(l4_cte.P200, 2) AS P200,
  ROUND((l4_cte.L100 + l4_cte.L200 + l4_cte.L300), 2) AS L000,
  ROUND(l4_cte.L100, 2) AS L100,
  ROUND(l4_cte.L200, 2) AS L200,
  ROUND(l4_cte.L300, 2) AS L300,
  ROUND((l4_cte.D100 + l4_cte.D200 + l4_cte.D300), 2) AS D000,
  --Business Rule: Fixed value for D100 aligned with Raquel Cerqueira. Technical debt: Value must be retrieved from a `static measure` solution
  ROUND(l4_cte.D100, 2) AS D100,
  ROUND(l4_cte.D200, 2) AS D200,
  ROUND(l4_cte.D300, 2) AS D300,
  ROUND((l4_cte.N100 + l4_cte.N200), 2) AS N000,
  ROUND(l4_cte.N100, 2) AS N100,
  ROUND(l4_cte.N200, 2) AS N200,
  l4_cte.stop_count,
  l4_cte.d100_event_count,
  TRUE AS is_active,
  0 AS exception_flag,
  {{add_tech_columns(this)}}
FROM DOM_MNSTR_DISTINCT_KEYS_CTE AS keys_cte
INNER JOIN DOM_MNSTR_L4_CTE AS l4_cte
  ON l4_cte.sk = keys_cte.sk 