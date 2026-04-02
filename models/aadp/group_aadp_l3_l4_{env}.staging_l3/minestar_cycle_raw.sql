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
        unique_key = ['minestar_cycle_raw_sk']
    )
}}

WITH CTE_curated_qvc_minestar_cycle_s_fact_main
AS (
  SELECT f.site_name,
    f.source_system_name,
    'cat_minestar.vw_cycle_s_fact_main' AS source_table_name,
    f.cycle_type AS source_cycle_type_name,
    to_date(SUBSTR(f.calendar_day, 5), 'dd MMM yyyy') AS reporting_date,
    f.primary_machine_name AS asset_name,
    loader_material_group_level1 AS source_material_group_name,
    left(f.source_block_name, 1) AS source_block_code,
    CASE 
      WHEN lower(f.sink_destination_name) LIKE '%chanca%'
        THEN 'Crusher'
      WHEN lower(f.sink_destination_name) LIKE '%st%'
        THEN 'Stock'
      ELSE 'NOT_APPLICABLE'
      END AS source_material_destination,
    f.payload_q AS amount
  FROM {{ source('cat_minestar_l2', 'vw_cycle_s_fact_main') }} AS f
  WHERE f.__is_deleted = 'N'
    AND f.payload_q IS NOT NULL
    AND cast(f.start_time AS DATE) >= '2021-01-01'
  ),
CTE_minestar_cycle_raw
AS (
  --Mineral/Desmonte
  SELECT CTE.site_name,
    CTE.source_system_name,
    CTE.source_table_name,
    CTE.source_cycle_type_name,
    CTE.reporting_date,
    CTE.asset_name,
    CTE.source_material_group_name,
    CTE.source_material_destination,
    'NOT_APPLICABLE' AS source_block_code,
    CTE.amount
  FROM CTE_curated_qvc_minestar_cycle_s_fact_main AS CTE
  
  UNION ALL
  
  --Rehandle/Ex pit tonnes moved
  SELECT CTE.site_name,
    CTE.source_system_name,
    CTE.source_table_name,
    CTE.source_cycle_type_name,
    CTE.reporting_date,
    CTE.asset_name,
    'NOT_APPLICABLE' AS source_material_group_name,
    'NOT_APPLICABLE' AS source_material_destination,
    CTE.source_block_code,
    CTE.amount
  FROM CTE_curated_qvc_minestar_cycle_s_fact_main AS CTE
  
  UNION ALL
  
  --Total Movement
  SELECT CTE.site_name,
    CTE.source_system_name,
    CTE.source_table_name,
    CTE.source_cycle_type_name,
    CTE.reporting_date,
    CTE.asset_name,
    'NOT_APPLICABLE' AS source_material_group_name,
    'NOT_APPLICABLE' AS source_material_destination,
    'NOT_APPLICABLE' AS source_block_code,
    CTE.amount
  FROM CTE_curated_qvc_minestar_cycle_s_fact_main AS CTE
  )
SELECT CONCAT_WS('-', a.site_name, a.source_system_name, a.source_cycle_type_name, a.reporting_date, a.asset_name, a.source_material_group_name, a.source_material_destination, a.source_block_code) AS minestar_cycle_raw_sk,
 a.site_name,
 a.source_system_name,
 a.source_table_name,
 a.source_cycle_type_name,
 a.reporting_date,
 a.asset_name,
 a.source_material_group_name,
 a.source_material_destination,
 a.source_block_code,
  SUM(a.amount) AS amount,
  {{add_tech_columns(this)}}
FROM CTE_minestar_cycle_raw AS a
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS l
   ON a.source_system_name = l.source_system_name
  AND a.site_name = l.source_site_code
  AND CASE
  WHEN '{{var('system')}}' IN ('ALL', '') THEN l.system_id
  ELSE '{{var('system')}}'
  END LIKE CONCAT ('%', l.system_id, '%')
  AND CASE
        WHEN '{{var('site')}}' in ('ALL', '') THEN l.site_id
        ELSE '{{var('site')}}'
      END LIKE concat('%', l.site_id, '%')
  AND l.__is_deleted = 'N'
WHERE a.reporting_date >= date_sub(current_date(), {{var('refresh_days')}}) 
GROUP BY a.site_name,
  a.source_system_name,
  a.source_table_name,
  a.source_cycle_type_name,
  a.reporting_date,
  a.asset_name,
  a.source_material_group_name,
  a.source_material_destination,
  a.source_block_code 