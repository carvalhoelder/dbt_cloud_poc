{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['system_code', 'site_code', 'site_reporting_group_code', 'source_table_name', 'site_system_shift_date_code', 'department_code', 'process_code', 'asset_code', 'scenario_code', 'material_code', 'metric_code', 'reporting_date', 'amount', 'unit_of_measure_code', 'kda']) ,
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
        pre_hook = "  DELETE FROM {{ this }} AS mp
 WHERE CASE
        WHEN '{{var('system')}}' in ('ALL', '') then NVL(system_code, '')
                         ELSE '{{var('system')}}'
                       END LIKE CONCAT('%', NVL(system_code, ''), '%')
 AND EXISTS (SELECT 1
               FROM group_aadp_l3_l4_{{var('env')}}.curated_l3.site_reporting_group_V2 AS srg
              WHERE srg.site_reporting_group_code = mp.site_reporting_group_code
                AND srg.__is_deleted = 'N'
                AND CASE
                      WHEN '{{var('site')}}' in ('ALL', '') then NVL(srg.site_code, '')
                      ELSE '{{var('site')}}'
                    END LIKE CONCAT('%', NVL(srg.site_code, ''), '%')
            )
 AND reporting_date >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}}) " if is_incremental() else "",
        unique_key = ['mining_proc_daily_summary_dom_sk']
    )
}}

WITH CTE_MINING_PROC_DAILY_SUMMARY_DOM
AS (
  SELECT 
    l.system_code,
    l.site_code,
    l.site_reporting_group_code,
    m.source_table_name,
    'NOT_APPLICABLE' AS site_system_shift_date_code,
    l.department_code,
    a.process_code,
    a.asset_code,
    l.scenario_code,
    l.material_code,
    l.metric_code,
    m.reporting_date,
    m.amount,
    l.unit_of_measure_code,
    l.kda
  FROM {{ ref('dom_time_usage_raw') }} AS m
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l
    ON l.`__is_deleted` = 'N'
      AND l.is_active = TRUE
      AND lower(l.source_system_code) = lower(m.source_system_name)
      AND lower(l.source_site_name) = lower(m.site_name)
      AND lower(l.source_table_name) = lower(m.source_table_name)
      AND lower(l.source_mapping_code) = lower(m.measure_name)
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_asset_lookup AS lkp
    ON lower(m.source_asset_name) = lower(lkp.source_asset_id)
      AND lower(m.site_name) = lower(lkp.source_site_code)
      AND lower(m.source_system_name) = lower(lkp.source_system_name)
      AND lkp.`__is_deleted` = 'N'
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_asset AS a
    ON lkp.asset_id = a.asset_code
      AND a.`__is_deleted` = 'N'
  )
SELECT CONCAT_WS('-', P.SYSTEM_CODE, P.SITE_CODE, P.ASSET_CODE, P.PROCESS_CODE, P.METRIC_CODE, P.REPORTING_DATE) AS mining_proc_daily_summary_dom_sk,
  P.system_code,
  P.site_code, 
  P.site_reporting_group_code,
  P.source_table_name,
  P.site_system_shift_date_code,
  P.department_code,
  P.process_code,
  P.asset_code,
  P.scenario_code,
  P.material_code,
  P.metric_code,
  P.reporting_date,
  P.amount,
  P.unit_of_measure_code,
  P.kda,
  {{add_tech_columns(this)}}
FROM CTE_MINING_PROC_DAILY_SUMMARY_DOM P
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.system AS sys
  ON lower(sys.system_code) = lower(P.system_code)
    AND sys.__is_deleted = 'N'
    AND CASE WHEN CONCAT('{{var('system')}}',',') IN ('ALL,', '') THEN CONCAT(sys.system_code,',')
           ELSE CONCAT('{{var('system')}}',',')
      END LIKE CONCAT('%', sys.system_code,',%')
INNER JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_v2 AS st
  ON st.site_code = P.site_code
    AND st.__is_deleted = 'N'
    AND CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN NVL(st.site_code, '')
                     ELSE '{{var('site')}}'
                END LIKE CONCAT('%', NVL(st.site_code, ''), '%')
WHERE p.reporting_date >= DATE_SUB(CURRENT_DATE (), {{var('refresh_days')}}) 