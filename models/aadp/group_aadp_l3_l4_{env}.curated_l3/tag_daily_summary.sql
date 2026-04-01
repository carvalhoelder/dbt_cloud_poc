{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['system_code', 'site_code', 'site_reporting_group_code', 'received_dispatched_site_reporting_group_code', 'date', 'asset_code', 'process_code', 'material_code', 'scenario_code', 'data_indicator_code', 'tag_code', 'tag_kda', 'metric_code', 'amount', 'unit_of_measure_code', 'reading_date_time_utc', 'reading_date_time_local', 'source_site_code', 'source_table_name']) ,
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
        pre_hook = " DELETE FROM {{ this }} AS tds
 WHERE CASE
        WHEN '{{var('system')}}' in ('ALL', '') then NVL(system_code, '')
        ELSE '{{var('system')}}'
      END LIKE CONCAT('%', NVL(system_code, ''), '%')
   AND CASE
         WHEN '{{var('site')}}' in ('ALL', '') then NVL(tds.site_code, '')
         ELSE '{{var('site')}}'
       END LIKE CONCAT('%', NVL(tds.site_code, ''), '%')
   AND tds.date >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}}) " if is_incremental() else "",
        unique_key = ['tag_daily_summary_sk']
    )
}}

SELECT tag_daily_summary_id AS tag_daily_summary_sk,
       system_code,
       site_code,
       site_reporting_group_code,
       received_dispatched_site_reporting_group_code,
       date,
       asset_code,
       process_code,
       material_code,
       metric_code,
       scenario_code,
       'NOT_APPLICABLE' AS data_indicator_code,
       tag_code,
       tag_kda,
       amount,
       unit_of_measure_code,
       reading_date_time_utc,
       reading_date_time_local,
       source_site_code,
       source_table_name,
        {{add_tech_columns(this)}}
  FROM {{ ref('osipi_tag_daily_summary') }} AS osipi_tag_daily_summary
 WHERE osipi_tag_daily_summary.__is_deleted = 'N'
   AND CASE
             WHEN '{{var('system')}}' in ('ALL', '') then NVL(osipi_tag_daily_summary.system_code, '')
             ELSE '{{var('system')}}'
            END LIKE CONCAT('%', NVL(osipi_tag_daily_summary.system_code, ''), '%')
   AND CASE
         WHEN '{{var('site')}}' in ('ALL', '') then NVL(osipi_tag_daily_summary.site_code, '')
         ELSE '{{var('site')}}'
       END LIKE CONCAT('%', NVL(osipi_tag_daily_summary.site_code, ''), '%')
   AND osipi_tag_daily_summary.date >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}}) 