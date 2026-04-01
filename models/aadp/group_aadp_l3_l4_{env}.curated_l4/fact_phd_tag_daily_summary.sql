{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['system_sk', 'site_sk', 'site_reporting_group_sk', 'date_sk', 'site_system_shift_date_sk', 'asset_sk', 'process_sk', 'scenario_sk', 'material_sk', 'functional_location_sk', 'reporting_period_sk', 'department_sk', 'received_dispatched_site_reporting_group_sk', 'tag_code', 'tag_kda', 'metric_code', 'amount', 'unit_of_measure_code', 'unit_of_measure_name', 'reading_date_time_utc', 'reading_date_time_local', 'source_site_code', 'source_table_name', 'is_active', 'exception_flag']) ,
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
    FROM group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS site,
      group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys,
      group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dd
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
        unique_key = ['tag_daily_summary_sk', 'data_indicator_sk']
    )
}}

SELECT l3.tag_daily_summary_id AS tag_daily_summary_sk,
       COALESCE(sys.system_sk,-1) AS system_sk,
       COALESCE(site.site_sk,-1) AS site_sk,
       COALESCE(srg.site_reporting_group_sk,-1) AS  site_reporting_group_sk,
       COALESCE(dim_date.date_sk,-1) AS date_sk, -- will offset hours already applied early in lineage
       COALESCE(CAST(REPLACE(CAST(dim_date.start_of_month AS STRING), '-', '') AS INT), -1) AS reporting_period_sk,
       COALESCE(shift.site_system_shift_date_sk,-1) AS site_system_shift_date_sk,
       COALESCE(asset.asset_sk,-1) AS asset_sk,
       COALESCE(process.process_sk,-1) AS process_sk,
       COALESCE(material.material_sk,-1) AS material_sk,
       COALESCE(scenario.scenario_sk,-1) AS scenario_sk,
       COALESCE(ddi.data_indicator_sk,-1) AS data_indicator_sk,
       -1 AS functional_location_sk,
       -1 AS department_sk,
       -1 AS received_dispatched_site_reporting_group_sk,
       CONCAT(l3.site_code, '-', l3.tag_code) AS tag_code,
       l3.tag_kda,
       l3.metric_code,
       l3.amount,
       l3.unit_of_measure_code,
       uom.name AS unit_of_measure_name,
       l3.reading_date_time_utc,
       l3.reading_date_time_local,
       l3.source_site_code,
       l3.source_table_name,
       TRUE AS is_active,
       0 AS exception_flag,
       {{add_tech_columns(this)}}
  FROM {{ ref('phd_tag_daily_summary') }} AS l3
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys
    ON sys.system_code = l3.system_code
   AND sys.__is_deleted = 'N'
   AND CASE
    WHEN '{{var('system')}}' IN ('ALL', '')
      THEN NVL(sys.system_code, '')
    ELSE '{{var('system')}}'
    END LIKE CONCAT ('%', NVL(sys.system_code, ''), '%')
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS site
    ON site.site_code = l3.site_code
   AND site.__is_deleted = 'N'
   AND CASE
    WHEN '{{var('site')}}' IN ('ALL', '')
      THEN NVL(site.site_code, '')
    ELSE '{{var('site')}}'
    END LIKE CONCAT ('%', NVL(site.site_code, ''), '%')
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_reporting_group AS srg
    ON srg.site_reporting_group_code = l3.site_reporting_group_code
   AND srg.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dim_date
    ON dim_date.DATE = l3.DATE
   AND dim_date.__is_deleted = 'N'
   AND dim_date.`date` >= date_sub(CURRENT_DATE (), {{var('refresh_days')}})
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_data_indicator AS ddi
    ON ddi.data_indicator_code = l3.data_indicator_code
   AND ddi.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_asset AS asset
    ON asset.asset_code = l3.asset_code
   AND asset.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_process AS process
    ON process.process_code = l3.process_code
   AND process.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_material AS material
    ON material.material_code = l3.material_code
   AND material.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_scenario AS scenario
    ON scenario.scenario_code = l3.scenario_code
   AND scenario.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_system_shift_date AS shift
    ON shift.site_sk = site.site_sk
   AND shift.system_sk = sys.system_sk
   AND l3.reading_date_time_utc >= shift.utc_start_date_time AND l3.reading_date_time_utc < shift.utc_end_date_time
   AND shift.is_active = TRUE
   AND shift.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.code_name_unit_of_measure AS uom
    ON uom.code = l3.unit_of_measure_code
   AND uom.__is_deleted = 'N'
 WHERE l3.__is_deleted = 'N' 