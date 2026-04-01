{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['system_code', 'site_code', 'site_reporting_group_code', 'scenario_code', 'site_system_shift_date_code', 'asset_code', 'process_code', 'material_code', 'data_indicator_code', 'metric_code', 'tag_code', 'dictionary', 'aggregation_type', 'time_scale', 'processing_sequence', 'reporting_date', 'reading_date_time_utc', 'reading_date_time_local', 'amount', 'unit_of_measure_code', 'tag_kda', 'row_number_last', 'row_number_first']) ,
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
        pre_hook = "  DELETE FROM {{ this }} AS tg
WHERE CASE
        WHEN '{{var('system')}}' in ('ALL', '') then NVL(system_code, '')
                         ELSE '{{var('system')}}'
                       END LIKE CONCAT('%', NVL(system_code, ''), '%')
AND EXISTS (SELECT 1
                 FROM group_aadp_l3_l4_{{var('env')}}.curated_l3.site_V2 AS st
                WHERE st.site_code = tg.site_code
                  AND st.__is_deleted = 'N'
                  AND CASE
                        WHEN '{{var('site')}}' in ('ALL', '') then NVL(st.site_code, '')
                        ELSE '{{var('site')}}'
                      END LIKE CONCAT('%', NVL(st.site_code, ''), '%')
              )
AND tg.reporting_date >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}}) " if is_incremental() else "",
        unique_key = ['phd_tag_processing_sk']
    )
}}

WITH phd_tag_reduction_cleansed AS (
  SELECT raw.system_code, raw.site_code, raw.site_system_shift_date_code, raw.dictionary, raw.tag_code
       , raw.aggregation, raw.time_scale, raw.reporting_date, raw.reading_date_time_utc, raw.reading_date_time_local
       , raw.dep1_tag_code, raw.dep2_tag_code, raw.metric_code, raw.tag_value
       , ROW_NUMBER() OVER (PARTITION BY raw.site_system_shift_date_code, raw.dictionary, raw.tag_code, raw.aggregation 
                                ORDER BY raw.reporting_date DESC) AS row_number_last
       , ROW_NUMBER() OVER (PARTITION BY raw.site_system_shift_date_code, raw.dictionary, raw.tag_code, raw.aggregation 
                                ORDER BY raw.reporting_date ASC) AS row_number_first
    FROM {{ ref('phd_tag_reduction_cleansed') }} AS raw
   WHERE raw.__is_deleted = 'N'
     AND raw.reporting_date >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}})
)
, phd_tag_reduction_cleansed_processing_sequence AS (
  SELECT CONCAT_WS('-', tag.system_code, tag.site_code, tag.site_reporting_group_code, tag.planning_scenario_code,
         phd.site_system_shift_date_code, tag.asset_code, tag.process_code, tag.material_code, tag.data_indicator_code,
         tag.dictionary, phd.time_scale, tag.tag_code, tag.metric_code, phd.reporting_date
       ) AS phd_tag_processing_sk
       , NVL(tag.system_code, 'NOT_APPLICABLE') AS system_code
       , NVL(tag.site_code, 'NOT_APPLICABLE') AS site_code
       , NVL(tag.site_reporting_group_code, 'NOT_APPLICABLE') AS site_reporting_group_code
       , NVL(tag.planning_scenario_code, 'NOT_APPLICABLE') AS scenario_code
       , NVL(phd.site_system_shift_date_code, 'NOT_APPLICABLE') AS site_system_shift_date_code
       , NVL(tag.asset_code, 'NOT_APPLICABLE') AS asset_code
       , NVL(tag.process_code, 'NOT_APPLICABLE') AS process_code
       , NVL(tag.material_code, 'NOT_APPLICABLE') AS material_code
       , NVL(tag.data_indicator_code, 'NOT_APPLICABLE') AS data_indicator_code
       , tag.metric_code
       , tag.tag_code
       , tag.dictionary
       , tag.aggregation_type
       , phd.time_scale
       , tag.processing_sequence
       , phd.reporting_date AS reporting_date
       , phd.reading_date_time_utc AS reading_date_time_utc
       , phd.reading_date_time_local AS reading_date_time_local
       , CASE WHEN UPPER(tag.aggregation_type) IN ('PRODUCT') THEN SUM(phd.tag_value) * SUM(dep1_cleansed.tag_value)
              WHEN UPPER(tag.aggregation_type) IN ('DIVIDE', 'DIV') THEN SUM(phd.tag_value) / SUM(dep1_cleansed.tag_value)
              WHEN UPPER(tag.aggregation_type) IN ('SUM') THEN SUM(phd.tag_value) + SUM(dep1_cleansed.tag_value)
              WHEN UPPER(tag.aggregation_type) IN ('MINUS') THEN SUM(phd.tag_value) - SUM(dep1_cleansed.tag_value)
              ELSE SUM(phd.tag_value)
         END  * COALESCE(NULLIF(tag.calculation,''),1) AS amount
       , tag.unit_of_measure_code
       , tag.tag_kda
       , phd.row_number_last
       , phd.row_number_first
    FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS tag
    JOIN phd_tag_reduction_cleansed AS phd
      ON UPPER(tag.tag_code) = UPPER(phd.tag_code)
     AND UPPER(tag.dictionary) = UPPER(phd.dictionary)
     AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN NVL(tag.system_code, '')
              ELSE '{{var('system')}}'
         END LIKE CONCAT ('%', NVL(tag.system_code, ''), '%')
     AND CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN NVL(tag.site_code, '')
              ELSE '{{var('site')}}'
         END LIKE CONCAT ('%', NVL(tag.site_code, ''), '%')
    LEFT JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS dep1_tag 
      ON tag.processing_tag_code = dep1_tag.tag_code
     AND tag.dictionary = dep1_tag.dictionary
     AND tag.system_code = dep1_tag.system_code
     AND tag.site_code = dep1_tag.site_code
     AND tag.site_reporting_group_code = dep1_tag.site_reporting_group_code
     AND tag.planning_scenario_code = dep1_tag.planning_scenario_code
     AND tag.asset_code = dep1_tag.asset_code
     AND tag.process_code = dep1_tag.process_code
     AND tag.material_code = dep1_tag.material_code
     AND tag.data_indicator_code = dep1_tag.data_indicator_code
     AND dep1_tag.is_active = TRUE
     AND dep1_tag.__is_deleted = 'N'
    LEFT JOIN phd_tag_reduction_cleansed AS dep1_cleansed
      ON UPPER(dep1_cleansed.tag_code) = UPPER(dep1_tag.tag_code)
     AND UPPER(dep1_cleansed.dictionary) = UPPER(dep1_tag.dictionary)
     AND dep1_cleansed.reporting_date = phd.reporting_date
   WHERE tag.is_active = TRUE
     AND tag.__is_deleted = 'N'
     AND tag.processing_sequence = 2
     AND ((
             -- case for start and end period
             DATE_TRUNC('DAY', phd.reporting_date) >= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_start_date, 'yyyyMMdd')),  'yyyy-MM-dd')
             AND DATE_TRUNC('DAY', phd.reporting_date) <= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_end_date,  'yyyyMMdd')), 'yyyy-MM-dd')
           )
           OR (
             -- case for no start and no end period
             tag.range_start_date IS NULL
             AND tag.range_end_date IS NULL
           )
           OR (
             -- case for start and no end period
             DATE_TRUNC('DAY', phd.reporting_date) >= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_start_date, 'yyyyMMdd')),  'yyyy-MM-dd')
             AND tag.range_end_date IS NULL
         ))
   GROUP BY tag.system_code, tag.site_code, tag.site_reporting_group_code, tag.planning_scenario_code, phd.site_system_shift_date_code, tag.asset_code,
         tag.process_code, tag.material_code, tag.data_indicator_code, tag.metric_code, tag.dictionary, tag.tag_code, tag.aggregation_type, phd.time_scale,
         tag.processing_sequence, phd.reporting_date, phd.reading_date_time_utc, phd.reading_date_time_local, tag.calculation, tag.unit_of_measure_code, tag.tag_kda,
         phd.row_number_last, phd.row_number_first
), phd_tag_processing AS (
SELECT CONCAT_WS('-', tag.system_code, tag.site_code, tag.site_reporting_group_code, tag.planning_scenario_code,
         phd.site_system_shift_date_code, tag.asset_code, tag.process_code, tag.material_code, tag.data_indicator_code, tag.dictionary, phd.time_scale,
         tag.tag_code, tag.metric_code, phd.reporting_date
       ) AS phd_tag_processing_sk
     , NVL(tag.system_code, 'NOT_APPLICABLE') AS system_code
     , NVL(tag.site_code, 'NOT_APPLICABLE') AS site_code
     , NVL(tag.site_reporting_group_code, 'NOT_APPLICABLE') AS site_reporting_group_code
     , NVL(tag.planning_scenario_code, 'NOT_APPLICABLE') AS scenario_code
     , NVL(phd.site_system_shift_date_code, 'NOT_APPLICABLE') AS site_system_shift_date_code
     , NVL(tag.asset_code, 'NOT_APPLICABLE') AS asset_code
     , NVL(tag.process_code, 'NOT_APPLICABLE') AS process_code
     , NVL(tag.material_code, 'NOT_APPLICABLE') AS material_code
     , NVL(tag.data_indicator_code, 'NOT_APPLICABLE') AS data_indicator_code
     , tag.metric_code
     , tag.tag_code
     , tag.dictionary
     , tag.aggregation_type
     , tag.time_scale
     , tag.processing_sequence
     , phd.reporting_date AS reporting_date
     , phd.reading_date_time_utc AS reading_date_time_utc
     , phd.reading_date_time_local AS reading_date_time_local
     , phd.tag_value * COALESCE(NULLIF(tag.calculation,''),1) AS amount
     , tag.unit_of_measure_code
     , tag.tag_kda
     , phd.row_number_last
     , phd.row_number_first
  FROM phd_tag_reduction_cleansed AS phd
  JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS tag
    ON UPPER(tag.tag_code) = UPPER(phd.tag_code)
   AND UPPER(tag.dictionary) = UPPER(phd.dictionary)
   AND tag.is_active = TRUE
   AND tag.__is_deleted = 'N'
   AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN NVL(tag.system_code, '')
            ELSE '{{var('system')}}'
       END LIKE CONCAT ('%', NVL(tag.system_code, ''), '%')
   AND CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN NVL(tag.site_code, '')
            ELSE '{{var('site')}}'
       END LIKE CONCAT ('%', NVL(tag.site_code, ''), '%')
   AND COALESCE(tag.processing_sequence,1) = 1
 WHERE 1=1
   AND ((
           -- case for start and end period
           DATE_TRUNC('DAY', phd.reporting_date) >= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_start_date, 'yyyyMMdd')),  'yyyy-MM-dd')
           AND DATE_TRUNC('DAY', phd.reporting_date) <= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_end_date,  'yyyyMMdd')), 'yyyy-MM-dd')
        )
        OR (
           -- case for no start and no end period
           tag.range_start_date IS NULL
           AND tag.range_end_date IS NULL
        )
        OR (
           -- case for start and no end period
           DATE_TRUNC('DAY', phd.reporting_date) >= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_start_date, 'yyyyMMdd')),  'yyyy-MM-dd')
           AND tag.range_end_date IS NULL
       ))

UNION

SELECT phd_tag_processing_sk, system_code, site_code, site_reporting_group_code, scenario_code, site_system_shift_date_code, asset_code, process_code
     , material_code, data_indicator_code, metric_code, tag_code, dictionary, aggregation_type, time_scale, processing_sequence
     , reporting_date, reading_date_time_utc, reading_date_time_local, amount, unit_of_measure_code, tag_kda, row_number_last, row_number_first
  FROM phd_tag_reduction_cleansed_processing_sequence )
  select
phd_tag_processing_sk, system_code, site_code, site_reporting_group_code, scenario_code, site_system_shift_date_code, asset_code, process_code
     , material_code, data_indicator_code, metric_code, tag_code, dictionary, aggregation_type, time_scale, processing_sequence
     , reporting_date, reading_date_time_utc, reading_date_time_local, amount, unit_of_measure_code, tag_kda, row_number_last, row_number_first,
  {{add_tech_columns(this)}}
  from phd_tag_processing
