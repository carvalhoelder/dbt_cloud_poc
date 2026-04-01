{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['system_code', 'site_code', 'site_reporting_group_code', 'date', 'asset_code', 'process_code', 'material_code', 'metric_code', 'scenario_code', 'data_indicator_code', 'tag_code', 'tag_kda', 'amount', 'unit_of_measure_code', 'reading_date_time_utc', 'reading_date_time_local', 'source_site_code', 'source_table_name']) ,
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
        pre_hook = "  DELETE FROM {{ this}} AS tg
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
AND tg.date >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}}) " if is_incremental() else "",
        unique_key = ['tag_daily_summary_id']
    )
}}

WITH phd_tag_daily_summary
AS (
SELECT CONCAT_WS('-',
                 tag.system_code,
                 tag.site_code,
                 tag.site_reporting_group_code,
                 tag.process_code,
                 tag.material_code,
                 tag.tag_code,
                 reading.reading_date_time_utc,
                 tag.metric_code) AS tag_daily_summary_id,
        tag.system_code,
        tag.site_code,
        tag.site_reporting_group_code,
        DATE_TRUNC('DAY', reading.reading_date_time_local + (INTERVAL 1 SECOND) * NVL(tag.date_offset_secs,0)) AS date,
        tag.asset_code,
        tag.process_code,
        tag.material_code,
        tag.metric_code,
        tag.planning_scenario_code AS scenario_code,
        NVL(tag.data_indicator_code, 'NOT_APPLICABLE') AS data_indicator_code,
        tag.tag_code,
        tag.tag_kda,
        reading.tag_value * COALESCE(NULLIF(calculation,''),1) AS amount,
        tag.unit_of_measure_code,
        reading.reading_date_time_utc AS reading_date_time_utc,
        reading.reading_date_time_local AS reading_date_time_local,
        reading.source_site_code,
        reading.source_table_name
   FROM {{ ref('phd_tag_reading') }} AS reading
   JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS tag
     ON tag.tag_code = reading.tag_code
    AND tag.dictionary = 'NOT_APPLICABLE'
    AND NVL(tag.time_scale,'') = NVL(reading.time_scale,'')
    AND tag.system_code = 'PHD'
    AND tag.source_site_code = reading.source_site_code
    AND tag.is_active
    AND tag.__is_deleted = 'N'
WHERE reading.tag_value IS NOT NULL
  AND reading.__is_deleted = 'N'
  
UNION ALL

SELECT CONCAT_WS('-',
                 tag.system_code,
                 tag.site_code,
                 tag.site_reporting_group_code,
                 tag.process_code,
                 tag.material_code,
                 tag.tag_code,
                 reading_w.reading_date_time_utc,
                 tag.metric_code) AS tag_daily_summary_id,
        tag.system_code,
        tag.site_code,
        tag.site_reporting_group_code,
        DATE_TRUNC('DAY', reading_w.reading_date_time_local + (INTERVAL 1 SECOND) * NVL(tag.date_offset_secs,0)) AS date,
        tag.asset_code,
        tag.process_code,
        tag.material_code,
        tag.metric_code,
        tag.planning_scenario_code AS scenario_code,
        NVL(tag.data_indicator_code, 'NOT_APPLICABLE') AS data_indicator_code,
        tag.tag_code,
        tag.tag_kda,
        reading_w.tag_value * COALESCE(NULLIF(calculation,''),1) AS amount,
        tag.unit_of_measure_code,
        reading_w.reading_date_time_utc AS reading_date_time_utc,
        reading_w.reading_date_time_local AS reading_date_time_local,
        reading_w.source_site_code,
        reading_w.source_table_name
   FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.wonderware_tag_reading AS reading_w
   JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS tag
     ON UPPER(tag.tag_code) = UPPER(reading_w.tag_code)
    AND tag.dictionary = 'NOT_APPLICABLE'
    AND NVL(tag.time_scale,'') = NVL(reading_w.time_scale,'')
    AND UPPER(tag.system_code) = UPPER(reading_w.source_system_name)
    AND tag.source_site_code = reading_w.source_site_code
    AND tag.is_active
    AND tag.__is_deleted = 'N'
WHERE reading_w.tag_value IS NOT NULL
  AND reading_w.__is_deleted = 'N' 
)
SELECT t.tag_daily_summary_id,
  t.system_code,
  t.site_code,
  t.site_reporting_group_code,
  t.DATE,
  t.asset_code,
  t.process_code,
  t.material_code,
  t.metric_code,
  t.scenario_code,
  t.data_indicator_code,
  t.tag_code,
  t.tag_kda,
  t.amount,
  t.unit_of_measure_code,
  t.reading_date_time_utc,
  t.reading_date_time_local,
  t.source_site_code,
  t.source_table_name,
  {{add_tech_columns(this)}}
FROM phd_tag_daily_summary AS t
JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.system AS sys
  ON sys.system_code = t.system_code
    AND sys.__is_deleted = 'N'
    AND CASE 
      WHEN '{{var('system')}}' IN ('ALL', '')
        THEN NVL(sys.system_code, '')
      ELSE '{{var('system')}}'
      END LIKE CONCAT ('%', NVL(sys.system_code, ''), '%')
JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_v2 AS st
  ON st.site_code = t.site_code
    AND st.__is_deleted = 'N'
    AND CASE 
      WHEN '{{var('site')}}' IN ('ALL', '')
        THEN NVL(st.site_code, '')
      ELSE '{{var('site')}}'
      END LIKE CONCAT ('%', NVL(st.site_code, ''), '%')
WHERE t.date >= DATE_SUB(CURRENT_DATE (), {{var('refresh_days')}}) 