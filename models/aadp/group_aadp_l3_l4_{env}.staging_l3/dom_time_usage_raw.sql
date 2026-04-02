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
        unique_key = ['dom_time_usage_raw_sk']
    )
}}

WITH CTE_DOM_TIME_USAGE_RAW
AS (
  SELECT cc.source_system_name,
    cc.site_name,
    'dynamo_operations_monitoring.vw_sp_availability_calc_cache'  AS source_table_name,
    cast(from_date AS DATE) AS reporting_date,
    CONCAT (cc.area,'_', cc.group_idx ) AS source_asset_name,
    inline(arrays_zip(array('utilization', 'availability'), array(utilization, availability))) AS (
      measure_name,
      amount
      )
  FROM {{ source('dynamo_operations_monitoring_l2', 'vw_sp_availability_calc_cache') }} AS cc
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS l
    ON cc.source_system_name = l.source_system_name
    AND cc.site_name = l.source_site_code
    AND CASE 
      WHEN '{{var('system')}}' IN ('ALL','') THEN l.system_id 
      ELSE '{{var('system')}}' 
      END LIKE CONCAT ('%',l.system_id,'%')
    AND CASE 
      WHEN '{{var('site')}}' IN ('ALL','') THEN l.site_id 
      ELSE '{{var('site')}}' 
      END LIKE CONCAT ('%',l.site_id,'%')
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_asset_lookup AS lkp
  ON CONCAT (area,'_', group_idx) = lkp.source_asset_id
    AND cc.site_name = lkp.source_site_code
    AND cc.source_system_name = lkp.source_system_name
    AND lkp.`__is_deleted` = 'N'
WHERE cc.__is_deleted = 'N'
    AND cast(from_date AS DATE) >= '2021-01-01')
SELECT CONCAT_WS ('-', source_system_name, site_name, source_table_name, reporting_date, source_asset_name, measure_name) AS dom_time_usage_raw_sk,
  source_system_name,
  site_name,
  source_table_name,
  reporting_date,
  source_asset_name,
  measure_name,
  amount,
  {{add_tech_columns(this)}}
FROM CTE_DOM_TIME_USAGE_RAW 