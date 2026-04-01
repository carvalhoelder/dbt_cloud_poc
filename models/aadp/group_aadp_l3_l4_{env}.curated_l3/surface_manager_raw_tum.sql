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
        unique_key = ['surface_manager_raw_tum_sk']
    )
}}

SELECT DISTINCT 
       CONCAT_WS('-',
                 t.source_system_name,
                 t.site_name,
                 RI.Name,
                 t.TIME,
                 t.end_time,
                 CAST(substring(t.data FROM 1 FOR 4) AS INTEGER)
       ) AS surface_manager_raw_tum_sk,
       t.source_system_name AS system_name,
       t.site_name AS site_name,
       RI.Name AS asset_name,
       t.TIME AS start_date_time,
       t.end_time AS end_date_time,
       FROM_UTC_TIMESTAMP(t.TIME, ms.time_zone_region_city) AS local_start_date_time,
       FROM_UTC_TIMESTAMP(t.end_time, ms.time_zone_region_city) AS local_end_date_time,
       CAST(substring(t.data FROM 1 FOR 4) AS INTEGER) AS reason_code,
       concat_ws(',', collect_list(REGEXP_REPLACE(TRIM(t.comment), '[ ]+', ' ')) OVER (PARTITION BY 
                 t.source_system_name,
                 t.site_name,
                 RI.Name,
                 t.TIME,
                 t.end_time,
                 CAST(substring(t.data FROM 1 FOR 4) AS INTEGER)
       )) as remark_description,
       {{add_tech_columns(this)}}
  FROM {{ source('surface_manager_l2', 'vw_rig_event') }} AS t
  LEFT JOIN {{ source('surface_manager_l2', 'vw_rig_delay_code') }} AS RDC
    ON substring(rdc.Name FROM 1 FOR 4) = substring(t.data FROM 1 FOR 4)
   AND RDC.site_name = t.site_name
   AND RDC.__is_deleted = 'N'
  JOIN {{ source('surface_manager_l2', 'vw_rig') }} AS RI
    ON RI.Id = t.rig_id
   AND RI.__is_deleted = 'N'
   AND RI.site_name = t.site_name
  JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS l
    ON t.source_system_name = l.source_system_name
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
  JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS ms
    ON ms.site_code = l.site_id
   AND ms.__is_deleted = 'N'
 WHERE t.__is_deleted = 'N'
   AND cast(t.TIME AS DATE) >= '2021-01-01'
   AND CAST(substring(t.data FROM 1 FOR 4) AS INTEGER) IS NOT NULL 
   AND cast(t.TIME AS DATE) >= date_sub(CURRENT_DATE (), {{var('refresh_days')}}) 