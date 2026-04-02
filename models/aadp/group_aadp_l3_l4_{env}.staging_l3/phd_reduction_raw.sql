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
        unique_key = ['phd_reduction_raw_sk']
    )
}}

WITH cte_phd_lookup AS (
  SELECT DISTINCT m.tag_code, m.dictionary, m.reduction_entry_type, tag.date_offset_secs
    FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.man_phd_lookup AS m
    LEFT JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS tag
      ON UPPER(tag.tag_code) = UPPER(m.tag_code)
     AND tag.dictionary = m.dictionary
     AND tag.is_active = TRUE
     AND tag.__is_deleted = 'N'
   WHERE m.__is_deleted = 'N'
)
, phd_reduction_raw AS (
SELECT CONCAT_WS('-', 'vw_sp_reduction'
                    , phd.sp_reduction_sk
                    , cte.dictionary
                    , phd.time_scale
       ) AS phd_reduction_raw_sk
     , phd.source_system_name
     , phd.site_name
     , phd.tagname AS tag_code
     , cte.dictionary
     , phd.time_scale
     , phd.reduction_entry_type
     , DATE_FORMAT(phd.timestamp + (INTERVAL 1 SECOND) * NVL(cte.date_offset_secs,0), 'yyyy-MM-dd HH:mm:ss') AS reporting_date
     , phd.range_start_time
     , phd.range_end_time
     , phd.value AS tag_value
     , phd.confidence
     , phd.units
     , RANK() OVER (PARTITION BY phd.sp_reduction_sk ORDER BY ingest_ts DESC) AS record_rank
  FROM {{ source('process_history_database_l2', 'vw_sp_reduction') }} AS phd
  JOIN cte_phd_lookup AS cte
    ON cte.tag_code = phd.tagname
   AND cte.reduction_entry_type = phd.reduction_entry_type
 WHERE phd.__is_deleted = 'N'
   AND phd.timestamp >= '2021-01-01'
   AND CAST(FROM_UTC_TIMESTAMP(phd.timestamp, 'America/Lima') AS DATE) >= date_sub(CURRENT_DATE(), {{var('refresh_days')}})

UNION ALL

SELECT CONCAT_WS('-', 'vw_sp_reduction_operational'
                    , phd.sp_reduction_operational_sk
                    , cte.dictionary
                    , phd.time_scale
       ) AS phd_reduction_raw_sk
     , phd.source_system_name
     , phd.site_name
     , phd.tagname AS tag_code
     , cte.dictionary
     , phd.time_scale
     , phd.reduction_entry_type
     , DATE_FORMAT(phd.timestamp + (INTERVAL 1 SECOND) * NVL(cte.date_offset_secs,0), 'yyyy-MM-dd HH:mm:ss') AS reporting_date
     , phd.range_start_time
     , phd.range_end_time
     , phd.value AS tag_value
     , phd.confidence
     , phd.units
     , RANK() OVER (PARTITION BY phd.sp_reduction_operational_sk ORDER BY ingest_ts DESC) AS record_rank
  FROM {{ source('process_history_database_l2', 'vw_sp_reduction_operational') }} AS phd
  JOIN cte_phd_lookup AS cte
    ON cte.tag_code = phd.tagname
   AND cte.reduction_entry_type = phd.reduction_entry_type
 WHERE phd.__is_deleted = 'N'
   AND phd.timestamp >= '2021-01-01'
   AND CAST(FROM_UTC_TIMESTAMP(phd.timestamp, 'America/Lima') AS DATE) >= date_sub(CURRENT_DATE(), {{var('refresh_days')}})
)
SELECT DISTINCT phd_reduction_raw_sk
     , source_system_name
     , site_name
     , tag_code
     , dictionary
     , time_scale
     , reduction_entry_type
     , reporting_date
     , range_start_time
     , range_end_time
     , tag_value
     , confidence
     , units,
     {{add_tech_columns(this)}}
  FROM phd_reduction_raw
 WHERE record_rank = 1 