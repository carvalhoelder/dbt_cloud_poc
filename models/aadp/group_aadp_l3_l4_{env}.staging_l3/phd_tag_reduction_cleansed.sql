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
        unique_key = ['phd_tag_reduction_cleansed_sk']
    )
}}

WITH cte_tag (
  SELECT /*+ BROADCAST(man) */
         man.tag_code, man.dictionary, man.reduction_entry_type, man.time_scale, man.aggregation, man.measure_name_kpi
       , man.dependency_1, man.dependency_2, man.dependency_1_lower_limit, man.dependency_1_upper_limit, man.dependency_1_condition_and_or
       , man.dependency_2_lower_limit, man.dependency_2_upper_limit, man.tag_type
       , man.lower_limit, man.upper_limit
       , CASE WHEN UPPER(time_scale) IN ('3MIN','3MINUTE','3 MINUTE') THEN 180
              WHEN UPPER(time_scale) IN ('15MIN','15MINUTE','15 MINUTE') THEN 900
              WHEN UPPER(time_scale) IN ('30MIN', '30MINUTE', '30 MINUTE') THEN 1800
              WHEN UPPER(time_scale) IN ('MINUTE', '1MINUTE', '1 MINUTE') THEN 60
              WHEN UPPER(time_scale) IN ('1HOUR', 'HOUR', 'HR') THEN 3600
              WHEN UPPER(time_scale) IN ('SHIFT', 'DAY') THEN 1
              ELSE 1
         END AS SecInternval
    FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.man_phd_lookup AS man
   WHERE man.__is_deleted = 'N'
)
, cte_site_lookup AS (
  SELECT /*+ BROADCAST(msl) */
         msl.source_system_name, msl.source_site_code, 
         msl.site_id, msl.system_id
    FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS msl
   WHERE msl.__is_deleted = 'N'
)
, cte_shift_dates AS (
  SELECT ss.site_code, ss.system_code, ss.site_system_shift_date_code,
         ss.local_start_date_time, ss.local_end_date_time
    FROM group_aadp_l3_l4_{{var('env')}}.curated_l3.site_system_shift_date AS ss
   WHERE ss.__is_deleted = 'N'
     AND ss.local_start_date_time >= date_sub(CURRENT_DATE(), CAST({{var('refresh_days')}} + 1 AS INT))
)
, cte_phd_reduction_raw AS (
  SELECT /*+ SHUFFLE_HASH(raw, cte) */
         raw.source_system_name, raw.site_name, raw.tag_code, raw.dictionary, raw.reduction_entry_type
       , raw.reporting_date, raw.range_start_time, raw.tag_value, cte.tag_type
       , cte.dependency_1, cte.dependency_2
       , cte.dependency_1_lower_limit, cte.dependency_1_upper_limit
       , cte.dependency_2_lower_limit, cte.dependency_2_upper_limit
       , cte.lower_limit, cte.upper_limit
    FROM {{ ref('phd_reduction_raw') }} AS raw
    JOIN cte_tag AS cte
      ON cte.tag_code = raw.tag_code
     AND cte.dictionary = raw.dictionary
     AND cte.reduction_entry_type = raw.reduction_entry_type
   WHERE raw.__is_deleted = 'N'
     AND CAST(FROM_UTC_TIMESTAMP(raw.reporting_date, 'America/Lima') AS DATE) >= date_sub(CURRENT_DATE(), {{var('refresh_days')}})
     -- Apply value filters early to reduce data volume
     AND raw.tag_value >= COALESCE(cte.lower_limit, raw.tag_value)
     AND raw.tag_value <= COALESCE(cte.upper_limit, raw.tag_value)
)
, cte_phd_with_site AS (
  SELECT /*+ BROADCAST(msl), MERGE(ss) */
         msl.site_id AS site_code,
         msl.system_id AS system_code,
         ss.site_system_shift_date_code,
         ss.local_start_date_time,
         phd.tag_code, phd.dictionary, phd.reduction_entry_type,
         phd.reporting_date, phd.tag_value, phd.tag_type,
         phd.dependency_1, phd.dependency_2,
         phd.dependency_1_lower_limit, phd.dependency_1_upper_limit,
         phd.dependency_2_lower_limit, phd.dependency_2_upper_limit
    FROM cte_phd_reduction_raw AS phd
   INNER JOIN cte_site_lookup AS msl
      ON msl.source_system_name = phd.source_system_name
     AND msl.source_site_code = phd.site_name
   INNER JOIN cte_shift_dates AS ss
      ON ss.site_code = msl.site_id
     AND ss.system_code = msl.system_id
     AND phd.reporting_date >= ss.local_start_date_time
     AND phd.reporting_date < ss.local_end_date_time
)
, cte_phd_cleansed AS (
  SELECT phd.site_code, phd.system_code, phd.site_system_shift_date_code
       , phd.dictionary, cte.measure_name_kpi AS metric_code, phd.tag_code, cte.aggregation, cte.time_scale
       , date_format(
           DATEADD(SECOND,(FLOOR(DATEDIFF(second,'1900-01-01',
                     CASE WHEN UPPER(cte.time_scale) IN ('SHIFT', 'DAY') THEN phd.local_start_date_time 
                          ELSE phd.reporting_date 
                     END )/SecInternval)*SecInternval) % 86400,
           DATEADD(day,(FLOOR(DATEDIFF(second,'1900-01-01',
                                CASE WHEN UPPER(cte.time_scale) IN ('SHIFT', 'DAY') THEN phd.local_start_date_time
                                     ELSE phd.reporting_date
                                END )/SecInternval)*SecInternval) / 86400, TIMESTAMP'1900-01-01 00:00:00')
         ),'yyyy-MM-dd HH:mm:ss') AS reporting_date_calc
       , phd.reporting_date AS reporting_date
       , cte.dependency_1 AS dep1_tag_code, cte.dependency_2 AS dep2_tag_code
       , cte.aggregation
       , CASE WHEN lower(phd.tag_type) = 'diferencia' THEN (phd.tag_value - NVL(dep1.tag_value,0)) ELSE phd.tag_value END AS tag_value
       , CASE WHEN UPPER(cte.aggregation) IN ('LAST(VALUES>0)', 'FIRST(VALUES>0)', 'LAST(VALUES!=0)', 'FIRST(VALUES!=0)') AND phd.tag_value = 0 THEN NULL
              WHEN UPPER(cte.aggregation) IN ('FIRST', 'FIRST(VALUES>0)', 'FIRST(VALUES!=0)') THEN
                   ROW_NUMBER() OVER (PARTITION BY phd.site_code, phd.system_code, phd.site_system_shift_date_code, phd.tag_code, phd.dictionary,
                   date_format(
                     DATEADD(SECOND,(FLOOR(DATEDIFF(second,'1900-01-01',
                               CASE WHEN UPPER(cte.time_scale) IN ('SHIFT', 'DAY') THEN phd.local_start_date_time ELSE phd.reporting_date 
                               END )/SecInternval)*SecInternval) % 86400,
                     DATEADD(day,(FLOOR(DATEDIFF(second,'1900-01-01',
                               CASE WHEN UPPER(cte.time_scale) IN ('SHIFT', 'DAY') THEN phd.local_start_date_time ELSE phd.reporting_date
                               END )/SecInternval)*SecInternval) / 86400, TIMESTAMP'1900-01-01 00:00:00')
                   ),'yyyy-MM-dd HH:mm:ss') ORDER BY phd.reporting_date ASC)
              ELSE ROW_NUMBER() OVER (PARTITION BY phd.site_code, phd.system_code, phd.site_system_shift_date_code, phd.tag_code, phd.dictionary,
                   date_format(
                     DATEADD(SECOND,(FLOOR(DATEDIFF(second,'1900-01-01',
                               CASE WHEN UPPER(cte.time_scale) IN ('SHIFT', 'DAY') THEN phd.local_start_date_time ELSE phd.reporting_date 
                               END )/SecInternval)*SecInternval) % 86400,
                     DATEADD(day,(FLOOR(DATEDIFF(second,'1900-01-01',
                               CASE WHEN UPPER(cte.time_scale) IN ('SHIFT', 'DAY') THEN phd.local_start_date_time ELSE phd.reporting_date
                               END )/SecInternval)*SecInternval) / 86400, TIMESTAMP'1900-01-01 00:00:00')
                   ),'yyyy-MM-dd HH:mm:ss') ORDER BY phd.reporting_date DESC)
         END AS rn
    FROM cte_phd_with_site AS phd
   INNER JOIN cte_tag AS CTE
      ON cte.tag_code = phd.tag_code
     AND cte.dictionary = phd.dictionary
     AND cte.reduction_entry_type = phd.reduction_entry_type
    LEFT JOIN cte_phd_reduction_raw AS dep1
      ON dep1.tag_code = cte.dependency_1 
     AND dep1.tag_value >= COALESCE(cte.dependency_1_lower_limit, dep1.tag_value) 
     AND dep1.tag_value <= COALESCE(cte.dependency_1_upper_limit, dep1.tag_value)
     AND dep1.reporting_date = phd.reporting_date
    LEFT JOIN cte_phd_reduction_raw AS dep2
      ON dep2.tag_code = cte.dependency_2
     AND dep2.tag_value >= COALESCE(cte.dependency_2_lower_limit, dep2.tag_value) 
     AND dep2.tag_value <= COALESCE(cte.dependency_2_upper_limit, dep2.tag_value)
     AND dep2.reporting_date = phd.reporting_date
   WHERE 1=1
     AND phd.tag_value >= COALESCE(cte.lower_limit, phd.tag_value) AND phd.tag_value <= COALESCE(cte.upper_limit, phd.tag_value)
     AND CASE WHEN (UPPER(COALESCE(cte.dependency_1_condition_and_or,''))='OR' AND COALESCE(cte.dependency_2,'')<>'' )
                THEN (COALESCE(dep1.tag_code,'') = COALESCE(cte.dependency_1,'') OR COALESCE(dep2.tag_code,'') = COALESCE(cte.dependency_2,''))
              ELSE (COALESCE(dep1.tag_code,'') = COALESCE(cte.dependency_1,'') 
                AND COALESCE(dep2.tag_code,'') = COALESCE(cte.dependency_2,''))
         END
)
, phd_tag_reduction_cleansed AS (
SELECT CONCAT_WS('-', site_code, system_code, site_system_shift_date_code,
         dictionary, tag_code, metric_code, reporting_date_calc
       ) AS phd_tag_reduction_cleansed_sk
     , site_code, system_code, site_system_shift_date_code
     , dictionary, tag_code, aggregation, time_scale
     , reporting_date_calc AS reporting_date
     , DATE_FORMAT(TO_UTC_TIMESTAMP(TO_TIMESTAMP(reporting_date_calc, 'yyyy-MM-dd HH:mm:ss'), 'America/Lima'),'yyyy-MM-dd HH:mm:ss') AS reading_date_time_utc
     , DATE_FORMAT(TO_TIMESTAMP(reporting_date_calc, 'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd HH:mm:ss') AS reading_date_time_local
     , dep1_tag_code, dep2_tag_code
     , metric_code
     , CASE WHEN UPPER(aggregation) IN ('','SUM','NO GROUPING') THEN SUM(tag_value)
            WHEN UPPER(aggregation) IN ('AVG') THEN AVG(tag_value)
            WHEN UPPER(aggregation) IN ('MAX') THEN MAX(tag_value)
            WHEN UPPER(aggregation) IN ('MIN') THEN MIN(tag_value)
            WHEN UPPER(aggregation) IN ('LAST', 'LAST(VALUES>0)', 'LAST(VALUES!=0)', 'FIRST', 'FIRST(VALUES>0)', 'FIRST(VALUES!=0)') THEN SUM(CASE WHEN rn = 1 THEN tag_value ELSE NULL END)
            WHEN UPPER(aggregation) IN ('COUNT') THEN COUNT(tag_value)
            WHEN UPPER(aggregation) IN ('MAX(VALUES>0)') THEN MAX(CASE WHEN tag_value>0 THEN tag_value ELSE NULL END)
            WHEN UPPER(aggregation) IN ('MAX(VALUES!=0)') THEN MAX(CASE WHEN tag_value!=0 THEN tag_value ELSE NULL END)
            WHEN UPPER(aggregation) IN ('AVG(VALUES>0)') THEN AVG(CASE WHEN tag_value>0 THEN tag_value ELSE NULL END)
            WHEN UPPER(aggregation) IN ('AVG(VALUES!=0)') THEN AVG(CASE WHEN tag_value!=0 THEN tag_value ELSE NULL END)
            WHEN UPPER(aggregation) IN ('COUNT (VALUES >0)', 'COUNT>0', 'COUNT(VALUES>0)') THEN COUNT(tag_value>0)
            ELSE SUM(tag_value)
       END AS tag_value
  FROM cte_phd_cleansed AS c
 WHERE c.rn IS NOT NULL
 GROUP BY site_code, system_code, site_system_shift_date_code
     , dictionary, tag_code, aggregation, time_scale
     , reporting_date_calc, dep1_tag_code, dep2_tag_code
     , metric_code
)
SELECT phd_tag_reduction_cleansed_sk, site_code, system_code, site_system_shift_date_code, dictionary, tag_code, aggregation, time_scale
     , reporting_date, reading_date_time_utc, reading_date_time_local, dep1_tag_code, dep2_tag_code
     , metric_code, tag_value,
     {{add_tech_columns(this)}}
  FROM phd_tag_reduction_cleansed
 WHERE tag_value IS NOT NULL 