{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['source_table_name', 'source_system_name', 'site_name', 'reporting_date_month', 'asset_code', 'kda', 'material_code', 'measure_name', 'measure_code', 'measure_value']) ,
        tags = ['refresh'],
        target_alias = "tgt",
        source_alias = "src", 
        meta =  { 
        "partition_column": partition_col,
        "interval": interval, 
        "interval_type": interval_type,
        "interval_delimiter": interval_delimiter},
not_matched_by_source_condition = generate_not_matched_by_source_condition(model, partition_col, interval, interval_type, interval_delimiter),
not_matched_by_source_action = generate_invalidate_action(model),
        unique_key = ['sgm_qvc_mining_proc_data_sk']
    )
}}

WITH SGM_QVC_MINING_PROC_DATA_CTE
AS (
  SELECT DISTINCT LKP.source_table_name,
    pf.source_system_name,
    pf.site_name,
    to_date(mes, 'yyyy-MM-dd') AS reporting_date,
    LKP.asset_code,
    LKP.kda,
    lkp.material_code,
    LKP.metric_code AS measure_name,
    lkp.unit_of_measure_code,
    cast((CASE WHEN LKP.metric_code = 'Availability' THEN availability_t200_t000 
    WHEN LKP.metric_code = 'Use of Availability' THEN uoa_t300_l300_t200 
    WHEN lkp.metric_code = 'Operating Efficiency' THEN operating_efficiency_t300_t300_l300 END) AS DECIMAL(18, 6)) AS measure_value
  FROM {{ source('documents_mine_plan_l2', 'vw_mineav_haul') }} AS pf
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS LKP
    ON lower(LKP.source_site_name) = lower(pf.site_name)
      AND lower(LKP.source_system_code) = lower(pf.source_system_name)
      AND lower(lkp.source_model) = lower(pf.fleet)
      AND LKP.source_table_name = 'documents_mine_plan.vw_mineav_haul'
      AND LKP.`__is_deleted` = 'N'
      AND LKP.is_active  = TRUE
  WHERE pf.__is_deleted = 'N'
    AND to_date(mes, 'yyyy-MM-dd') >= '2021-01-01'

  UNION ALL
  
  SELECT DISTINCT LKP.source_table_name,
    pf.source_system_name,
    pf.site_name,
    to_date(date_trunc('MONTH', pf.new_fecha), 'YYYY-MM-DD') AS reporting_date,
    LKP.asset_code,
    LKP.kda,
    lkp.material_code,
    LKP.metric_code AS measure_name,
    lkp.unit_of_measure_code,
    cast((CASE WHEN LKP.metric_code = 'Tons Milled' THEN SUM(pf.tonelaje_molido_ktms) 
    WHEN LKP.metric_code = 'Bulk Copper Recovery' THEN AVG(pf.cu_fino_producido_tms / (pf.ley_cu_p * pf.tonelaje_molido_ktms)) 
    WHEN lkp.metric_code = 'Fine Copper Produced' THEN SUM(pf.cu_fino_producido_tms) 
    WHEN lkp.metric_code = 'Ore Grade' THEN AVG(pf.ley_cu_p) END) AS DECIMAL(18, 6)) AS measure_value
  FROM {{ source('documents_mine_plan_l2', 'vw_meta_m_bdgt') }} AS pf
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS LKP
    ON lower(LKP.source_site_name) = lower(pf.site_name)
      AND lower(LKP.source_system_code) = lower(pf.source_system_name)
      AND LKP.source_table_name = 'documents_mine_plan.vw_meta_m_bdgt'
      AND LKP.`__is_deleted` = 'N'
      AND LKP.is_active  = TRUE
  WHERE pf.__is_deleted = 'N'
    AND to_date(pf.new_fecha, 'yyyy-MM-dd') >= '2021-01-01'
  GROUP BY LKP.source_table_name,
    pf.source_system_name,
    pf.site_name,
    date_trunc('MONTH', pf.new_fecha),
    LKP.asset_code,
    LKP.kda,
    lkp.material_code,
    LKP.metric_code,
    lkp.unit_of_measure_code

  UNION ALL
  
  SELECT DISTINCT LKP.source_table_name,
    pf.source_system_name,
    pf.site_name,
    to_date(pf.month, 'yyyy-MM-dd') AS reporting_date,
    LKP.asset_code,
    LKP.kda,
    lkp.material_code,
    LKP.metric_code AS measure_name,
    lkp.unit_of_measure_code,
    cast((CASE 
          WHEN LKP.metric_code = 'Meters Drilled'
            THEN pf.metros_produccion_m
          WHEN LKP.metric_code = 'Availability'
            THEN pf.disponibilidad_p
          END ) AS DECIMAL(18, 6)) AS measure_value
  FROM {{ source('documents_mine_plan_l2', 'vw_mineav_drill') }} AS pf
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS LKP
    ON lower(LKP.source_site_name) = lower(pf.site_name)
      AND lower(LKP.source_system_code) = lower(pf.source_system_name)
      AND lower(lkp.source_model) = lower(pf.fleet)
      AND LKP.source_table_name = 'documents_mine_plan.vw_mineav_drill'
      AND LKP.`__is_deleted` = 'N'
      AND LKP.is_active  = TRUE
  WHERE pf.__is_deleted = 'N'
    AND to_date(pf.month, 'yyyy-MM-dd') >= '2021-01-01'
  
  UNION ALL
  
  SELECT DISTINCT LKP.source_table_name,
    pf.source_system_name,
    pf.site_name,
    to_date(mes, 'yyyy-MM-dd') AS reporting_date,
    LKP.asset_code,
    LKP.kda,
    lkp.material_code,
    LKP.metric_code AS measure_name,
    lkp.unit_of_measure_code,
    cast(total_moved_kt AS DECIMAL(18, 6)) AS measure_value
  FROM {{ source('documents_mine_plan_l2', 'vw_minemov') }} AS pf
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS LKP
    ON lower(LKP.source_site_name) = lower(pf.site_name)
      AND lower(LKP.source_system_code) = lower(pf.source_system_name)
      AND LKP.source_table_name = 'documents_mine_plan.vw_minemov'
      AND LKP.`__is_deleted` = 'N'
      AND LKP.is_active  = TRUE
  WHERE pf.__is_deleted = 'N'
    AND to_date(mes, 'yyyy-MM-dd') >= '2021-01-01'
  
  UNION ALL
  
  SELECT lkp.source_table_name,
    pf.source_system_name,
    pf.site_name,
    to_date(date_trunc('month', to_date(fecha)), 'yyyy-MM-dd') AS reporting_date,
    lkp.asset_code,
    LKP.kda,
    lkp.material_code,
    LKP.metric_code AS measure_name,
    lkp.unit_of_measure_code,
    CAST(AVG(CASE WHEN PROCESS_CODE = 'PRC_GRD_MillingLine1' THEN disponibilidad_molienda_l1_p 
    WHEN PROCESS_CODE = 'PRC_GRD_MillingLine2' THEN disponibilidad_molienda_l2_p 
    WHEN PROCESS_CODE = 'PRC_PCR' THEN disponibilidad_chancado_primario_p END) / 100 AS DECIMAL(18, 6)) AS Avg_Availability
  FROM {{ source('documents_mine_plan_l2', 'vw_plant_av') }} AS pf
  INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS LKP
    ON lower(LKP.source_site_name) = lower(pf.site_name)
      AND lower(LKP.source_system_code) = lower(pf.source_system_name)
      AND LKP.source_table_name = 'documents_mine_plan.vw_plant_av'
      AND LKP.`__is_deleted` = 'N'
      AND LKP.is_active  = TRUE
  WHERE pf.__is_deleted = 'N'
    AND to_date(fecha,'yyyy-MM-dd') >= '2021-01-01'
  GROUP BY LKP.source_table_name,
    pf.source_system_name,
    pf.site_name,
    to_date(date_trunc('month', to_date(fecha)), 'yyyy-MM-dd'),
    lkp.asset_code,
    LKP.kda,
    lkp.material_code,
    LKP.metric_code,
    lkp.unit_of_measure_code
   )
SELECT CONCAT_WS('-', source_table_name, source_system_name, site_name, reporting_date, asset_code, measure_name) AS sgm_qvc_mining_proc_data_sk,
  source_table_name AS source_table_name,
  source_system_name AS source_system_name,
  site_name AS site_name,
  reporting_date AS reporting_date_month,
  asset_code AS asset_code,
  kda AS kda,
  material_code AS material_code,
  measure_name AS measure_name,
  unit_of_measure_code AS measure_code,
  measure_value AS measure_value,
  {{add_tech_columns(this)}}
FROM SGM_QVC_MINING_PROC_DATA_CTE 