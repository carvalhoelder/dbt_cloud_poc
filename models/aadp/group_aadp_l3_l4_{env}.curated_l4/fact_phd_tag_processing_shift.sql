{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['reading_date_time_utc', 'reading_date_time_local', 'tag_kda', 'aggregation_type', 'amount', 'unit_of_measure_code', 'unit_of_measure_name', 'processing_sequence', 'is_active', 'exception_flag']) ,
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
        unique_key = ['system_sk', 'site_sk', 'site_reporting_group_sk', 'date_sk', 'reporting_period_sk', 'site_system_shift_date_sk', 'asset_sk', 'process_sk', 'material_sk', 'scenario_sk', 'functional_location_sk', 'department_sk', 'received_dispatched_site_reporting_group_sk', 'data_indicator_sk', 'tag_code', 'dictionary', 'metric_code']
    )
}}

WITH cte_fact_phd_tag_processing AS (
SELECT COALESCE(sys.system_sk,-1) AS system_sk,
       COALESCE(site.site_sk,-1) AS site_sk,
       COALESCE(srg.site_reporting_group_sk,-1) AS site_reporting_group_sk,
       COALESCE(dim_date.date_sk,-1) AS date_sk,
       COALESCE(CAST(REPLACE(CAST(dim_date.start_of_month AS STRING), '-', '') AS INT), -1) AS reporting_period_sk,
       COALESCE(shift.site_system_shift_date_sk,-1) AS site_system_shift_date_sk,
       COALESCE(asset.asset_sk,-1) AS asset_sk,
       COALESCE(process.process_sk,-1) AS process_sk,
       COALESCE(material.material_sk,-1) AS material_sk,
       COALESCE(scenario.scenario_sk,-1) AS scenario_sk,
       -1 AS functional_location_sk,
       -1 AS department_sk,
       -1 AS received_dispatched_site_reporting_group_sk,
       COALESCE(ddi.data_indicator_sk,-1) AS data_indicator_sk,
       CONCAT(l3.site_code, '-', l3.tag_code) AS tag_code,
       l3.dictionary,
       l3.tag_kda,
       l3.aggregation_type,
       l3.metric_code,
       CASE WHEN UPPER(l3.aggregation_type) IN ('','SUM','NO GROUPING') THEN SUM(l3.amount)
            WHEN UPPER(l3.aggregation_type) IN ('AVG') THEN AVG(l3.amount)
            WHEN UPPER(l3.aggregation_type) IN ('MAX') THEN MAX(l3.amount)
            WHEN UPPER(l3.aggregation_type) IN ('MIN') THEN MIN(l3.amount)
            WHEN UPPER(l3.aggregation_type) IN ('LAST', 'LAST(VALUES>0)') THEN SUM(CASE WHEN l3.row_number_last = 1 THEN l3.amount ELSE NULL END)
            WHEN UPPER(l3.aggregation_type) IN ('FIRST', 'FIRST(VALUES>0)') THEN SUM(CASE WHEN l3.row_number_first = 1 THEN l3.amount ELSE NULL END)
            WHEN UPPER(l3.aggregation_type) IN ('COUNT') THEN COUNT(l3.amount)
            WHEN UPPER(l3.aggregation_type) IN ('MAX(VALUES>0)') THEN MAX(CASE WHEN l3.amount>0 THEN l3.amount ELSE NULL END)
            WHEN UPPER(l3.aggregation_type) IN ('AVG(VALUES>0)') THEN AVG(CASE WHEN l3.amount>0 THEN l3.amount ELSE NULL END)
            WHEN UPPER(l3.aggregation_type) IN ('COUNT (VALUES >0)', 'COUNT>0', 'COUNT(VALUES>0)') THEN COUNT(l3.amount>0)
            ELSE SUM(l3.amount)
       END AS amount,
       l3.unit_of_measure_code,
       uom.name AS unit_of_measure_name,
       shift.utc_start_date_time AS reading_date_time_utc,
       shift.local_start_date_time AS reading_date_time_local,
       l3.processing_sequence,
       TRUE AS is_active,
       0 AS exception_flag
  FROM {{ ref('phd_tag_processing') }} AS l3
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys
    ON sys.system_code = l3.system_code
   AND sys.__is_deleted = 'N'
   AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN NVL(sys.system_code, '') ELSE '{{var('system')}}'
       END LIKE CONCAT ('%', NVL(sys.system_code, ''), '%')
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS site
    ON site.site_code = l3.site_code
   AND site.__is_deleted = 'N'
   AND CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN NVL(site.site_code, '') ELSE '{{var('site')}}'
       END LIKE CONCAT ('%', NVL(site.site_code, ''), '%')
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_reporting_group AS srg
    ON srg.site_reporting_group_code = l3.site_reporting_group_code
   AND srg.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_system_shift_date AS shift
    ON shift.site_system_shift_date_code = l3.site_system_shift_date_code
   AND shift.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dim_date
    ON dim_date.date = shift.reporting_date
   AND dim_date.__is_deleted = 'N'
   AND dim_date.date >= date_sub(CURRENT_DATE (), {{var('refresh_days')}})
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_asset AS asset
    ON asset.asset_code = l3.asset_code
   AND asset.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_process AS process
    ON process.process_code = l3.process_code
   AND process.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_material AS material
    ON material.material_code = l3.material_code
   AND material.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_scenario AS scenario
    ON scenario.scenario_code = l3.scenario_code
   AND scenario.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_data_indicator AS ddi
    ON ddi.data_indicator_code = l3.data_indicator_code
   AND ddi.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.code_name_unit_of_measure AS uom
    ON uom.code = l3.unit_of_measure_code
   AND uom.__is_deleted = 'N'
 WHERE 1=1
 GROUP BY sys.system_sk,
       site.site_sk,
       srg.site_reporting_group_sk,
       dim_date.date_sk,
       dim_date.start_of_month,
       shift.site_system_shift_date_sk,
       asset.asset_sk,
       process.process_sk,
       material.material_sk,
       scenario.scenario_sk,
       ddi.data_indicator_sk,
       l3.site_code,
       l3.tag_code,
       l3.dictionary,
       l3.tag_kda,
       l3.metric_code,
       l3.aggregation_type,
       l3.unit_of_measure_code,
       uom.name,
       shift.utc_start_date_time,
       shift.local_start_date_time,
       l3.processing_sequence
)
SELECT l3.system_sk, l3.site_sk, l3.site_reporting_group_sk, l3.date_sk, l3.reporting_period_sk, l3.site_system_shift_date_sk, l3.asset_sk,
       l3.process_sk, l3.material_sk, l3.scenario_sk, l3.functional_location_sk, l3.department_sk, l3.received_dispatched_site_reporting_group_sk, l3.data_indicator_sk,
       l3.tag_code, l3.dictionary, l3.tag_kda, l3.aggregation_type, l3.metric_code, l3.amount, l3.unit_of_measure_code, l3.unit_of_measure_name,
       l3.reading_date_time_utc, l3.reading_date_time_local, l3.processing_sequence, l3.is_active, l3.exception_flag,
       {{add_tech_columns(this)}}
  FROM cte_fact_phd_tag_processing AS l3
 WHERE l3.amount IS NOT NULL 