{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "table",
        tags = ['overwrite'],
        target_alias = "tgt",
        source_alias = "src", 
        meta =  { 
        "partition_column": partition_col,
        "interval": interval, 
        "interval_type": interval_type,
        "interval_delimiter": interval_delimiter},
        unique_key = ['tag_daily_summary_id']
    )
}}

SELECT CONCAT_WS('-',
                 tag.tag_code,
                 date_format(DATE_TRUNC('DAY', reading.reading_date_time_utc + (INTERVAL 1 SECOND) * NVL(tag.date_offset_secs,0)),'yyyyMMdd'),
                 reading.source_site_code,
                 tag.tag_kda,
                 tag.process_code,
                 tag.material_code,
                 tag.metric_code) AS tag_daily_summary_id,
       tag.system_code,
       tag.site_code,
       tag.site_reporting_group_code,
       tag.received_dispatched_site_reporting_group_code,
       DATE_TRUNC('DAY', reading.reading_date_time_utc + (INTERVAL 1 SECOND) * NVL(tag.date_offset_secs,0)) AS date,
       -- *** issue of adding extra second for -6 hours to make 6am move to prevoius day?  Note that for +24 offset we dont want to skip a day. 
       --    Either: change business reqs to treat 6am as same day.  Or change to 'Offset Seconds' to give full control in the tag data.
       --   note also: in Databricks runtime 10.4 can use TIMESTAMPADD
       tag.asset_code,
       tag.process_code,
       tag.material_code,
       tag.metric_code,
       tag.planning_scenario_code AS scenario_code,
       tag.tag_code,
       tag.tag_kda,
       SUM(reading.tag_value) AS amount,
       tag.unit_of_measure_code,
       MAX(reading.reading_date_time_utc) AS reading_date_time_utc, -- already truncated to seconds  
       MAX(reading.reading_date_time_local) AS reading_date_time_local, -- already truncated to seconds   
       reading.source_site_code,
       reading.source_table_name,
       {{add_tech_columns(this)}}
  FROM {{ ref('osipi_tag_reading') }} AS reading
  JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tag_lookup AS tag
    ON tag.tag_code = reading.tag_pi_point
   AND tag.dictionary = 'NOT_APPLICABLE'
   AND tag.source_site_code = reading.source_site_code
   AND tag.is_active
   AND tag.__is_deleted = 'N'
   AND CASE
         WHEN '{{var('system')}}' in ('ALL', '') then NVL(tag.system_code, '')
         ELSE '{{var('system')}}'
       END LIKE CONCAT('%', NVL(tag.system_code, ''), '%')
   AND CASE
         WHEN '{{var('site')}}' in ('ALL', '') THEN tag.site_code
         ELSE '{{var('site')}}'
       END LIKE concat('%', tag.site_code, '%')
 WHERE reading.tag_value IS NOT NULL -- tag_reading is already filtering out NULLs, but check anyway.
   AND reading.__is_deleted = 'N'
   AND DATE_TRUNC('DAY', reading.reading_date_time_utc) >= date_sub(current_date(), {{var('refresh_days')}})
 GROUP BY 
       tag.system_code,
       tag.site_code,
       tag.site_reporting_group_code,
       tag.received_dispatched_site_reporting_group_code,
       DATE_TRUNC('DAY', reading.reading_date_time_utc + (INTERVAL 1 SECOND) * NVL(tag.date_offset_secs,0)),
       tag.asset_code,
       tag.process_code,
       tag.material_code,
       tag.metric_code,
       tag.planning_scenario_code,
       tag.tag_code,
       tag.tag_kda,
       tag.unit_of_measure_code,
       reading.source_site_code,
       reading.source_table_name 