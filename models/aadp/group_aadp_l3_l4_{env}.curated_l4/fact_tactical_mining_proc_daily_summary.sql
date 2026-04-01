{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['measure_value', 'is_active', 'exception_flag']) ,
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
        unique_key = ['system_sk', 'site_sk', 'date_sk', 'site_system_shift_date_sk', 'asset_sk', 'scenario_sk', 'process_sk', 'department_sk', 'site_reporting_group_sk', 'reporting_period_sk', 'functional_location_sk', 'material_sk', 'kda', 'measure_name', 'data_indicator_sk', 'unit_of_measure_code']
    )
}}

SELECT DISTINCT NVL(ds.System_sk, -1) AS system_sk
     , NVL(dsi.Site_sk, -1) AS site_sk
     , NVL(dd.date_sk, -1) AS date_sk
     , NVL(dsssd.site_system_shift_date_sk, -1) AS site_system_shift_date_sk
     , NVL(dat.asset_sk, -1) AS asset_sk
     , NVL(dsc.scenario_sk, -1) AS scenario_sk
     , NVL(dp.process_sk, -1) AS process_sk
     , -1 AS department_sk
     , NVL(dsrg.site_reporting_group_sk, - 1) AS site_reporting_group_sk 
     , NVL(dd.date_sk, -1) AS reporting_period_sk
     , NVL(dfl.functional_location_sk, -1) AS functional_location_sk
     , NVL(dm.material_sk,-1) AS material_sk 
     , ci.kda AS kda
     , ci.measure_name AS measure_name
     , NVL(di.data_indicator_sk,-1) AS data_indicator_sk
     , ci.unit_of_measure_code AS unit_of_measure_code
     , CASE when ci.unit_of_measure_code in ('Waste:Ore','PCT') then avg(ci.measure_value) else SUM(ci.measure_value) end AS measure_value
     , TRUE AS Is_Active
     , 0 AS Exception_Flag
     ,{{add_tech_columns(this)}}
  FROM {{ ref('tactical_mining_proc_daily_summary') }} AS ci
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS ds ON lower(ds.System_Code) = lower(ci.system_code) AND ds.__is_deleted = 'N' 
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS dsi ON lower(dsi.site_name) = lower(ci.site_name) AND dsi.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_date AS dd ON dd.DATE = date(ci.date) AND dd.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_system_shift_date AS dsssd ON dsssd.__is_deleted = 'N' AND dsssd.site_system_shift_date_code = ci.site_system_shift_date_code
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_asset AS dat ON dat.asset_code = ci.asset_code AND dat.__is_deleted = 'N'
  --LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_scenario AS dsc ON dsc.scenario_code = case ci.scenario_code when 'Actuals' THEN 'ACT' when 'Budget' THEN 'BUD' ELSE 'OMS' END  AND dsc.__is_deleted = 'N' -- 'Actual'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_scenario AS dsc ON dsc.scenario_code = ci.scenario_code AND dsc.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_process AS dp ON dp.process_code = ci.process_code AND dp.__is_deleted = 'N'
  --LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_department AS dep ON dep.department_code = ci.department_code AND dep.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_reporting_group AS dsrg ON dsrg.site_reporting_group_code = ci.site_reporting_group_code AND dsrg.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_functional_location AS dfl ON dfl.functional_location = ci.functional_location_code AND dfl.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_material AS dm ON dm.material_code = ci.material_code AND dm.__is_deleted = 'N'
  LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_data_indicator AS di  ON di.data_indicator_code = ci.data_indicator_code AND di.__is_deleted = 'N'
  WHERE ci.__is_deleted = 'N'
  GROUP BY ds.System_sk
         , dsi.Site_sk
         , dd.date_sk
         , dsssd.site_system_shift_date_sk
         , dat.asset_sk
         , dsc.scenario_sk
         , dfl.functional_location_sk
         , dp.process_sk
         , dsrg.site_reporting_group_sk
         --, dep.department_sk
         , ci.kda
         , ci.measure_name
         , di.data_indicator_sk
         , ci.unit_of_measure_code  
         , dm.material_sk 