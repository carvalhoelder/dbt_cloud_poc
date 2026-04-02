{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "table",
        schema='staging_l3',
        tags = ['overwrite'],
        target_alias = "tgt",
        source_alias = "src", 
        meta =  { 
        "partition_column": partition_col,
        "interval": interval, 
        "interval_type": interval_type,
        "interval_delimiter": interval_delimiter},
        unique_key = ['phd_tag_reading_id']
    )
}}

SELECT DISTINCT phd.sp_reduction_sk AS phd_tag_reading_id,
  'process_history_database.vw_sp_reduction' AS source_table_name,
  phd.source_system_name,
  phd.site_name AS source_site_code,
  TO_UTC_TIMESTAMP(DATE_TRUNC('SECOND', phd.timestamp), 'America/Lima') AS reading_date_time_utc,
  DATE_TRUNC('SECOND', phd.timestamp) AS reading_date_time_local,
  UPPER(phd.tagname) AS tag_code,
  phd.value AS tag_value,
  phd.time_scale AS time_scale,
  {{add_tech_columns(this)}}
FROM {{ source('process_history_database_l2', 'vw_sp_reduction') }} AS phd
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS tag
  ON UPPER(tag.tag_code) = UPPER(phd.tagname)
    AND tag.dictionary = 'NOT_APPLICABLE'
    AND NVL(tag.time_scale,'') = NVL(phd.time_scale,'')
    AND tag.system_code = 'PHD'
    AND tag.source_site_code = phd.site_name
    AND tag.is_active
    AND tag.__is_deleted = 'N'
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS l
  ON phd.source_system_name = l.source_system_name
    AND phd.site_name = l.source_site_code
    AND CASE 
      WHEN '{{var('system')}}' IN ('ALL', '')
        THEN l.system_id
      ELSE '{{var('system')}}'
      END LIKE CONCAT ('%', l.system_id, '%')
    AND CASE 
      WHEN '{{var('site')}}' IN ('ALL', '')
        THEN l.site_id
      ELSE '{{var('site')}}'
      END LIKE CONCAT ('%', l.site_id, '%')
    AND l.__is_deleted = 'N'
WHERE 1 = 1
  AND phd.timestamp >= '2021-01-01'
  AND (
    (
      -- case for start and end period
      DATE_TRUNC('SECOND', phd.timestamp) >= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_start_date, 'yyyyMMdd')), 'yyyy-MM-dd')
      AND DATE_TRUNC('SECOND', phd.timestamp) <= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_end_date, 'yyyyMMdd')), 'yyyy-MM-dd')
      )
    OR (
      -- case for no start and no end period
      tag.range_start_date IS NULL
      AND tag.range_end_date IS NULL
      )
    OR (
      -- case for start and no end period
      DATE_TRUNC('SECOND', phd.TIMESTAMP) >= DATE_FORMAT(FROM_UNIXTIME(UNIX_TIMESTAMP(tag.range_start_date, 'yyyyMMdd')), 'yyyy-MM-dd')
      AND tag.range_end_date IS NULL
      )
    )
  AND (
    (
      -- case for shift readings
      tag.is_shift_reading = 'Y'
      AND extract(HOUR FROM phd.timestamp) IN (7, 19)
      )
    OR tag.is_shift_reading = 'N'
    )
  AND phd.__is_deleted = 'N'
  AND cast(FROM_UTC_TIMESTAMP(phd.timestamp, 'America/Lima') AS DATE) >= date_sub(CURRENT_DATE(), {{var('refresh_days')}}) 