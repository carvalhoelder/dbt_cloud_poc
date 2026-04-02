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
        unique_key = ['osipi_tag_reading_id']
    )
}}

/*-----------
-- OSIPI --
-----------*/
WITH CTE_tag_lookup AS (
  SELECT *
    FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS tag
   WHERE tag.__is_deleted = 'N' AND tag.is_active = TRUE
     AND tag.dictionary = 'NOT_APPLICABLE'
), 
vw_pi_int_data_analytics_v1 AS (
    SELECT pi_int_data_analytics_v1_sk AS osipi_tag_reading_id,
           tag_pi_point,
           tag_status,
           DATE_TRUNC('SECOND', time_stamp) AS reading_date_time_utc,
           FROM_UTC_TIMESTAMP(DATE_TRUNC('SECOND', time_stamp), 'Africa/Johannesburg') AS reading_date_time_local,
           tag_value,
           site_name AS source_site_code,
           source_system_name,
           'osisoft_pi.vw_pi_int_data_analytics_v1' AS source_table_name,
           RANK() OVER (PARTITION BY pi_int_data_analytics_v1_sk ORDER BY pi_int_shape_id DESC,ingest_ts DESC) AS record_rank
      FROM {{ source('osisoft_pi_l2', 'vw_pi_int_data_analytics_v1') }} AS vwv1
      JOIN CTE_tag_lookup AS tag
        ON tag.tag_code = vwv1.tag_pi_point
       AND CASE
             WHEN '{{var('system')}}' in ('ALL', '') then NVL(tag.system_code, '')
             ELSE '{{var('system')}}'
           END LIKE CONCAT('%', NVL(tag.system_code, ''), '%')
       AND CASE
             WHEN '{{var('site')}}' in ('ALL', '') THEN tag.site_code
             ELSE '{{var('site')}}'
           END LIKE concat('%', tag.site_code, '%')
     WHERE vwv1.__is_deleted = 'N' AND vwv1.tag_value IS NOT NULL
       AND vwv1.time_stamp >= '2021-01-01'
       AND cast(vwv1.time_stamp as date) >= date_sub(current_date(), {{var('refresh_days')}})
),
vw_pi_int_data_analytics_v1_day_5 AS (
    SELECT pi_int_data_analytics_v1_day_5_sk AS osipi_tag_reading_id,
          tag_pi_point,
          tag_status,
          DATE_TRUNC('SECOND', time_stamp) AS reading_date_time_utc,
          FROM_UTC_TIMESTAMP(DATE_TRUNC('SECOND', time_stamp), 'Africa/Johannesburg') AS reading_date_time_local,
          tag_value,
          site_name AS source_site_code,
          source_system_name,
          'osisoft_pi.vw_pi_int_data_analytics_v1_day_5' AS source_table_name,
          RANK() OVER (PARTITION BY pi_int_data_analytics_v1_day_5_sk ORDER BY pi_int_shape_id DESC,ingest_ts DESC) AS record_rank
      FROM {{ source('osisoft_pi_l2', 'vw_pi_int_data_analytics_v1_day_5') }} AS vwv1
      JOIN CTE_tag_lookup AS tag
        ON tag.tag_code = vwv1.tag_pi_point
       AND CASE
             WHEN '{{var('system')}}' in ('ALL', '') then NVL(tag.system_code, '')
             ELSE '{{var('system')}}'
           END LIKE CONCAT('%', NVL(tag.system_code, ''), '%')
       AND CASE
             WHEN '{{var('site')}}' in ('ALL', '') THEN tag.site_code
             ELSE '{{var('site')}}'
           END LIKE concat('%', tag.site_code, '%')
     WHERE vwv1.__is_deleted = 'N' AND vwv1.tag_value IS NOT NULL
       AND vwv1.time_stamp >= '2021-01-01'
       AND cast(vwv1.time_stamp as date) >= date_sub(current_date(), {{var('refresh_days')}})
),
vw_pi_int_data_analytics_v1_day_6 AS (
    SELECT pi_int_data_analytics_v1_day_6_sk AS osipi_tag_reading_id,
           tag_pi_point,
           tag_status,
           DATE_TRUNC('SECOND', time_stamp) AS reading_date_time_utc,
           FROM_UTC_TIMESTAMP(DATE_TRUNC('SECOND', time_stamp), 'Africa/Johannesburg') AS reading_date_time_local,
           tag_value,
           site_name AS source_site_code,
           source_system_name,
           'osisoft_pi.vw_pi_int_data_analytics_v1_day_6' AS source_table_name,
           RANK() OVER (PARTITION BY pi_int_data_analytics_v1_day_6_sk ORDER BY pi_int_shape_id DESC,ingest_ts DESC) AS record_rank
      FROM {{ source('osisoft_pi_l2', 'vw_pi_int_data_analytics_v1_day_6') }} AS vwv1
      JOIN CTE_tag_lookup AS tag
        ON tag.tag_code = vwv1.tag_pi_point
       AND CASE
             WHEN '{{var('system')}}' in ('ALL', '') then NVL(tag.system_code, '')
             ELSE '{{var('system')}}'
           END LIKE CONCAT('%', NVL(tag.system_code, ''), '%')
       AND CASE
             WHEN '{{var('site')}}' in ('ALL', '') THEN tag.site_code
             ELSE '{{var('site')}}'
           END LIKE concat('%', tag.site_code, '%')
     WHERE vwv1.__is_deleted = 'N' AND vwv1.tag_value IS NOT NULL
       AND vwv1.time_stamp >= '2021-01-01'
       AND cast(vwv1.time_stamp as date) >= date_sub(current_date(), {{var('refresh_days')}})
),
-- remove duplicate TAG Readings comming from vw_pi_int_data_analytics_v1 and *v1_day_6 - Bug fix: 756042
v1_except_v6 AS (
  SELECT tag_pi_point,
         DATE_TRUNC('Day', reading_date_time_utc) AS reading_date
    FROM vw_pi_int_data_analytics_v1
  EXCEPT
  SELECT tag_pi_point,
         DATE_TRUNC('Day', reading_date_time_utc) AS reading_date
    FROM vw_pi_int_data_analytics_v1_day_6
),
-- remove duplicate TAG Readings comming from vw_pi_int_data_analytics_v1 and *v1_day_5 - Bug fix: 756042
v1_except_v5 AS (
  SELECT tag_pi_point,
         reading_date
    FROM v1_except_v6
  EXCEPT
  SELECT tag_pi_point,
         DATE_TRUNC('Day', reading_date_time_utc) AS reading_date
    FROM vw_pi_int_data_analytics_v1_day_5
),

final_union AS (
SELECT v5.osipi_tag_reading_id, v5.tag_pi_point, v5.tag_status, v5.reading_date_time_utc, v5.reading_date_time_local, v5.tag_value, v5.source_site_code, v5.source_system_name, v5.source_table_name
  FROM vw_pi_int_data_analytics_v1_day_5 AS v5
  WHERE v5.record_rank = 1

UNION

SELECT v6.osipi_tag_reading_id, v6.tag_pi_point, v6.tag_status, v6.reading_date_time_utc, v6.reading_date_time_local, v6.tag_value, v6.source_site_code, v6.source_system_name, v6.source_table_name
  FROM vw_pi_int_data_analytics_v1_day_6 AS v6
  WHERE v6.record_rank = 1

UNION

SELECT v1.osipi_tag_reading_id, v1.tag_pi_point, v1.tag_status, v1.reading_date_time_utc, v1.reading_date_time_local, v1.tag_value, v1.source_site_code, v1.source_system_name, v1.source_table_name
  FROM vw_pi_int_data_analytics_v1 AS v1
  JOIN v1_except_v5 AS ex
    ON ex.tag_pi_point = v1.tag_pi_point
   AND ex.reading_date = DATE_TRUNC('Day', v1.reading_date_time_utc) 
 WHERE v1.record_rank = 1

-----------
--  QVC  --
-----------
UNION

(
  WITH curated_qvc_pa_log_sheet_data_p AS (
    SELECT CONCAT(qvc.attribute_id,'-',DATE_TRUNC('SECOND', qvc.log_time)) AS osipi_tag_reading_id,
           qvc.attribute_id AS tag_pi_point,
           'Good' AS tag_status,
           DATE_TRUNC('SECOND', qvc.log_time) AS reading_date_time_utc,
           FROM_UTC_TIMESTAMP(DATE_TRUNC('SECOND', qvc.log_time), 'America/Lima') AS reading_date_time_local,
           qvc.attribute_value AS tag_value,
           qvc.site_name AS source_site_code,
           qvc.source_system_name,
           'production_accounting.vw_log_sheet_data_p' AS source_table_name,
           RANK() OVER (PARTITION BY qvc.attribute_id, DATE_FORMAT(CAST(qvc.log_time as timestamp), 'yyyyMMdd') ORDER BY DATE_TRUNC('SECOND', qvc.log_time) DESC, qvc.ingest_ts DESC) AS record_rank
      FROM {{ source('production_accounting_l2', 'vw_log_sheet_data_p') }} AS qvc
      JOIN CTE_tag_lookup AS tag
        ON tag.tag_code = qvc.attribute_id
     WHERE qvc.__is_deleted = 'N'
       AND to_date(qvc.log_time) >= '2022-05-31'
       AND qvc.set_id IN ('PUB_AAQ.SUM.DY.S')
  )
  SELECT qvc.osipi_tag_reading_id, qvc.tag_pi_point, qvc.tag_status, qvc.reading_date_time_utc, qvc.reading_date_time_local, qvc.tag_value, qvc.source_site_code, qvc.source_system_name, qvc.source_table_name
    FROM curated_qvc_pa_log_sheet_data_p AS qvc
    JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS l
      ON qvc.source_system_name = l.source_system_name
     AND qvc.source_site_code = l.source_site_code
     AND CASE
             WHEN '{{var('system')}}' in ('ALL', '') then NVL(l.system_id, '')
             ELSE '{{var('system')}}'
            END LIKE CONCAT('%', NVL(l.system_id, ''), '%')
     AND CASE
           WHEN '{{var('site')}}' in ('ALL', '') THEN l.site_id
           ELSE '{{var('site')}}'
         END LIKE concat('%', l.site_id, '%')
     AND l.__is_deleted = 'N'
   WHERE qvc.record_rank = 1
     AND cast(qvc.reading_date_time_utc as date) >= date_sub(current_date(), {{var('refresh_days')}})
) )
SELECT 
osipi_tag_reading_id, tag_pi_point, tag_status, reading_date_time_utc, reading_date_time_local, tag_value, source_site_code, source_system_name, source_table_name,
{{add_tech_columns(this)}}
FROM final_union