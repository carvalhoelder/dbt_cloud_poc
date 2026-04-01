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
        unique_key = ['dom_minestar_raw_tum_sk']
    )
}}

WITH CTE_dom_raw_tum
AS (
  SELECT CONCAT_WS('-', t.source_system_name, t.site_name, t.asset, t.DowntimeStartTime, tuml.tum_category_code, t.reasonidx) AS dom_minestar_raw_tum_sk,
    t.source_system_name AS system_name,
    t.site_name AS site_name,
    t.asset AS asset_name,
    t.DowntimeStartTime AS local_start_date_time,
    t.DowntimeEndTime AS local_end_date_time,
    tuml.tum_category_code AS reason_code,
    SUM(t.reasonduration * 60) OVER (
      PARTITION BY t.source_system_name,
      t.site_name,
      t.asset,
      t.DowntimeStartTime ORDER BY t.reasonidx ASC
      ) AS cumulative_duration,
    SUM(t.reasonduration * 60) OVER (
      PARTITION BY t.source_system_name,
      t.site_name,
      t.asset,
      t.DowntimeStartTime ORDER BY t.reasonidx ASC
      ) - (t.reasonduration * 60) AS idx_duration,
    t.reasonidx
  FROM {{ source('dynamo_operations_monitoring_l2', 'vw_vwdowntimeevents_adicflag') }} AS t
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tum_lookup AS tuml
    ON tuml.source_system_code = t.source_system_name
      AND tuml.source_site_code = t.site_name
      AND tuml.source_category_code = SUBSTRING(t.reasongroup, 1, 4)
      AND tuml.process_code = 'PRC'
      AND tuml.__is_deleted = 'N'
  WHERE t.__is_deleted = 'N'
    AND t.DowntimeStartTime >= '2021-01-01'
    AND CAST(SUBSTRING(t.reasongroup, 1, 4) AS INT) IS NOT NULL
  ),
CTE_minestar_raw_tum
AS (
  SELECT DISTINCT ac.source_system_name AS system_name,
    ac.site_name AS site_name,
    f.primary_machine_name AS asset_name,
    tuml.tum_category_code AS reason_code,
    ac.start_time_utc AS start_date_time,
    ac.end_time_utc AS end_date_time,
    FROM_UTC_TIMESTAMP(ac.start_time_utc, 'America/Lima') AS local_start_date_time,
    FROM_UTC_TIMESTAMP(ac.end_time_utc, 'America/Lima') AS local_end_date_time,
    NULL AS remark,
    NULL AS remark_description
  FROM {{ source('cat_minestar_l2', 'vw_cycle_activity_component') }} AS ac
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tum_lookup AS tuml
    ON tuml.source_system_code = ac.source_system_name
      AND tuml.source_site_code = ac.site_name
      AND tuml.source_category_code = ac.name
      AND tuml.process_code = 'MIN'
      AND tuml.__is_deleted = 'N'
  LEFT JOIN {{ source('cat_minestar_l2', 'vw_cycle_s_fact_main') }} AS f
    ON ac.oid = f.cycle_oid
      AND f.__is_deleted = 'N'
  WHERE ac.__is_deleted = 'N'
    AND f.cycle_type IN (
      'TruckCycle',
      'LoaderCycle',
      'AuxMobileCycle'
      )
    AND FROM_UTC_TIMESTAMP(ac.start_time_utc, 'America/Lima') >= '2024-01-01'
  
  UNION
  
  SELECT DISTINCT dclass.source_system_name AS system_name,
    dclass.site_name AS site_name,
    f.primary_machine_name AS asset_name,
    nvl(tuml.tum_category_code, CASE cd.delay_category
      WHEN 'Perdida Interna' --L200
        THEN nvl(cast(dclass.externalref AS INT), 4500)
      WHEN 'Perdida de Produccion' --L300
        THEN nvl(cast(dclass.externalref AS INT), 3000)
      WHEN 'Mantenimiento No Programado' --D100
        THEN nvl(cast(dclass.externalref AS INT), 7180)
      ELSE cast(dclass.externalref AS INT)
      END) AS reason_code,
    cd.start_time_utc AS start_date_time,
    cd.end_time_utc AS end_date_time,
    FROM_UTC_TIMESTAMP(cd.start_time_utc, 'America/Lima') AS local_start_date_time,
    FROM_UTC_TIMESTAMP(cd.end_time_utc, 'America/Lima') AS local_end_date_time,
    cd.delay_class_desc as remark,
    dl.description AS remark_description
  FROM {{ source('cat_minestar_l2', 'vw_cycle_delay') }} AS cd
  INNER JOIN {{ source('cat_minestar_l2', 'vw_delayclass') }} AS dclass
    ON cd.delay_class_oid = dclass.delayclass_oid
      AND dclass.__is_deleted = 'N'
  LEFT JOIN {{ source('cat_minestar_l2', 'vw_cycle_s_fact_main') }} AS f
    ON cd.oid = f.cycle_oid
      AND f.__is_deleted = 'N'
  LEFT JOIN {{ source('cat_minestar_l2', 'vw_cycle_activity_component') }} AS ac
    ON cd.start_time_utc = ac.start_time_utc
      AND cd.end_time_utc = ac.end_time_utc
      AND ac.name = 'Machine.Delay'
      AND ac.__is_deleted = 'N'
  LEFT JOIN {{ source('cat_minestar_l2', 'vw_delay') }} AS dl
    ON cd.delay_oid = dl.delay_oid
      AND dl.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tum_lookup AS tuml
    ON tuml.source_system_code = dclass.source_system_name
      AND tuml.source_site_code = dclass.site_name
      AND tuml.source_category_code = cast(dclass.externalref AS INT)
      AND tuml.process_code = 'MIN'
      AND tuml.__is_deleted = 'N'
  WHERE cd.__is_deleted = 'N'
    AND f.cycle_type IN (
      'TruckCycle',
      'LoaderCycle',
      'AuxMobileCycle'
      )
    AND FROM_UTC_TIMESTAMP(cd.start_time_utc, 'America/Lima') >= '2024-01-01'
  ),
dom_minestar_raw_tum
AS (
  SELECT dom_minestar_raw_tum_sk,
    system_name,
    site_name,
    asset_name,
    TO_UTC_TIMESTAMP(to_timestamp(unix_timestamp(local_start_date_time) + COALESCE(idx_duration, 0)), 'America/Lima') AS start_date_time,
    TO_UTC_TIMESTAMP(to_timestamp(unix_timestamp(local_start_date_time) + COALESCE(cumulative_duration, 0)), 'America/Lima') AS end_date_time,
    to_timestamp(unix_timestamp(local_start_date_time) + COALESCE(idx_duration, 0)) AS local_start_date_time,
    to_timestamp(unix_timestamp(local_start_date_time) + COALESCE(cumulative_duration, 0)) AS local_end_date_time,
    reason_code,
    NULL AS remark,
    NULL AS remark_description
  FROM CTE_dom_raw_tum AS raw
  WHERE reason_code IS NOT NULL
  
  UNION ALL
  
  SELECT concat_ws('-', system_name, site_name, asset_name, local_start_date_time, local_end_date_time, reason_code) AS dom_minestar_raw_tum_sk,
    system_name,
    site_name,
    asset_name,
    start_date_time,
    end_date_time,
    local_start_date_time,
    local_end_date_time,
    reason_code,
    remark,
    remark_description
  FROM CTE_minestar_raw_tum AS raw
  WHERE reason_code IS NOT NULL
  )
SELECT dom_minestar_raw_tum_sk,
  system_name,
  site_name,
  asset_name,
  start_date_time,
  end_date_time,
  local_start_date_time,
  local_end_date_time,
  reason_code,
  remark,
  remark_description,
  {{add_tech_columns(this)}}
FROM dom_minestar_raw_tum AS t
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS l
  ON t.system_name = l.source_system_name
    AND t.site_name = l.source_site_code
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
WHERE cast(t.start_date_time AS DATE) >= date_sub(CURRENT_DATE (), {{var('refresh_days')}}) 