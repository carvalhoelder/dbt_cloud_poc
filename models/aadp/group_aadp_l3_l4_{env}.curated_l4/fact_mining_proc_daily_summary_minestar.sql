{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['unit_of_measure_code', 'kda', 'amount']) ,
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
        unique_key = ['system_sk', 'site_sk', 'site_reporting_group_sk', 'site_system_shift_date_sk', 'department_sk', 'functional_location_sk', 'process_sk', 'asset_sk', 'scenario_sk', 'date_sk', 'material_sk', 'metric_code']
    )
}}

WITH CTE_MINING_PROC_DAILY_SUMMARY_MINESTAR
AS (
  SELECT mining_proc_daily_summary_minestar_sk,
    system_code,
    site_reporting_group_code,
    source_table_name,
    site_system_shift_date_code,
    department_code,
    process_code,
    asset_code,
    scenario_code,
    material_code,
    metric_code,
    reporting_date,
    amount,
    unit_of_measure_code,
    kda
  FROM {{ ref('mining_proc_daily_summary_minestar') }} AS m
  WHERE m.__is_deleted = 'N'
  
  UNION ALL
  
  SELECT mining_proc_daily_summary_dom_sk,
    system_code,
    site_reporting_group_code,
    source_table_name,
    site_system_shift_date_code,
    department_code,
    process_code,
    asset_code,
    scenario_code,
    material_code,
    metric_code,
    reporting_date,
    amount,
    unit_of_measure_code,
    kda
  FROM {{ ref('mining_proc_daily_summary_dom') }} AS d
  WHERE d.__is_deleted = 'N'
  )
  SELECT NVL(sys.system_sk, -1) AS system_sk
     , NVL(srg.site_sk, -1) AS site_sk
     , NVL(srg.site_reporting_group_sk, -1) AS site_reporting_group_sk
     , NVL(sssd.site_system_shift_date_sk, -1) AS site_system_shift_date_sk
     , -1 AS department_sk
     , -1 AS functional_location_sk
     , NVL(process.process_sk, -1) AS process_sk
     , NVL(asset.asset_sk, -1) AS asset_sk
     , NVL(scenario.scenario_sk, -1) AS scenario_sk
     , NVL(mt.material_sk, -1) AS material_sk
     , NVL(dim_date.date_sk, -1) AS date_sk
     , m.metric_code
     , m.amount
     , m.unit_of_measure_code
     , m.kda,
     {{add_tech_columns(this)}}
  FROM CTE_MINING_PROC_DAILY_SUMMARY_MINESTAR AS m
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys
    ON sys.system_code = m.system_code
   AND sys.__is_deleted = 'N'
   AND CASE
     WHEN '{{var('system')}}' in ('ALL', '') then NVL(sys.system_code, '')
     ELSE '{{var('system')}}'
   END LIKE CONCAT('%', NVL(sys.system_code, ''), '%')
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_reporting_group AS srg
    ON srg.site_reporting_group_code = m.site_reporting_group_code
   AND srg.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS site
    ON site.site_sk = srg.site_sk
   AND site.__is_deleted = 'N'
   AND CASE
     WHEN '{{var('site')}}' in ('ALL', '') then NVL(site.site_code, '')
     ELSE '{{var('site')}}'
    END LIKE CONCAT('%', NVL(site.site_code, ''), '%')
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dim_date
    ON dim_date.DATE = CAST(m.reporting_date AS DATE)
   AND dim_date.`__is_deleted` = 'N'
   AND dim_date.date >= date_sub(CURRENT_DATE(), {{var('refresh_days')}})
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_system_shift_date AS sssd
    ON sssd.site_system_shift_date_code = m.site_system_shift_date_code
   AND sssd.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_asset AS asset
    ON asset.asset_code = m.asset_code
   AND asset.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_process AS process
    ON process.process_code = m.process_code
   AND process.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_scenario AS scenario
    ON scenario.scenario_code = m.scenario_code
   AND scenario.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_material AS mt
    ON mt.material_code = m.material_code
   AND mt.__is_deleted = 'N' 