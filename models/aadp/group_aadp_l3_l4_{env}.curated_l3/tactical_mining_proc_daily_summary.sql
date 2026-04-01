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
        unique_key = ['system_code', 'site_name', 'date', 'site_system_shift_date_code', 'asset_code', 'scenario_code', 'process_code', 'department_code', 'site_reporting_group_code', 'reporting_period_code', 'functional_location_code', 'kda', 'material_code', 'measure_name', 'data_indicator_code', 'unit_of_measure_code']
    )
}}


with all_data as (
/*Bluesheets*/
SELECT
  l.system_code,
  l.source_site_name AS site_name,
  b.reporting_date AS date,
  'NOT_APPLICABLE' AS site_system_shift_date_code,
  l.asset_code,
  sc.scenario_code,
  l.process_code,
  l.department_code,
  b.site_reporting_group_code,
  b.reporting_period AS reporting_period_code,
  'NOT_APPLICABLE' AS functional_location_code,
  l.kda,
  l.material_code,
  l.metric_code AS measure_name,
  l.data_indicator_code,
  l.unit_of_measure_code,
  case when unit_of_measure_code in ('Waste:Ore','PCT') then avg(measure_value * l.cal_multiplier) else sum(measure_value * l.cal_multiplier) end AS measure_value,
  TRUE AS is_active,
  0 AS exception_flag
FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.bluesheets_measures AS b
Inner join group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON l.__is_deleted = 'N'
  and is_active = TRUE
  and lower(b.source_table_name) = lower(l.source_table_name)
  and lower(b.load_name) like concat('%', lower(l.source_mapping_code), '%')
  and lower(b.site) = lower(l.source_site_name)
  and lower(b.plant_pit) = lower(l.source_mine_plant)
  and lower(b.asset_code) = lower(l.source_equipment)
  and lower(b.source_system_name) = lower(l.source_system_code)
  --  avoid scenario code in the join to pull all scenarios
  and lower(b.material_code) = lower(l.source_material_code)
  and lower(b.process_code) = lower(l.source_process_code)
  and lower(b.site_reporting_group_code) = lower(l.site_reporting_group_code)
  and lower(b.measure_name) = lower(l.metric_code)
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_planning_scenario AS sc on  sc.`__is_deleted` = 'N'
        AND concat('|', lower(trim(sc.scenario_code)), '|', lower(trim(sc.lookup_values)), '|')
                        like concat('%|', lower(trim(b.scenario_code)),'|%')
WHERE b.__is_deleted = 'N'
GROUP BY
  l.system_code, l.source_site_name, b.reporting_date, l.asset_code,
  sc.scenario_code, l.process_code, l.department_code, b.site_reporting_group_code,
  b.reporting_period, l.kda,  l.material_code, l.metric_code, l.data_indicator_code, l.unit_of_measure_code

-- Reconcilor
UNION ALL
SELECT
  lkp.system_code,
  s.site_name,
  rec.period_end_date metric_date,
  'NOT_APPLICABLE' as site_system_shift_date_code,
  lkp.asset_code as asset_code,
  lkp.scenario_code as scenario_code,
  lkp.process_code as process_code,
  lkp.department_code as department_code,
  lkp.site_reporting_group_code as site_reporting_group_code,
  '' as reporting_period_code,
  'NOT_APPLICABLE' as functional_location_code,
  lkp.kda as kda,
  lkp.material_code,
  lkp.metric_code as measure_name,
  lkp.data_indicator_code,
  lkp.unit_of_measure_code,
  CAST((rec.value * lkp.cal_multiplier) AS DECIMAL(18,6)) as measure_value,
  TRUE as is_active,
  0 AS exception_flag
FROM  {{ ref('rec_sord_data') }}  AS rec
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS lkp ON 1=1
      AND lkp.is_active = TRUE
      AND lkp.__is_deleted = 'N'
      AND lower(lkp.source_table_name) = lower(rec.source_table_name)
      AND lower(lkp.source_site_name) = lower(rec.source_site_name)
      AND lower(lkp.source_system_code) = lower(rec.source_system_name)
      AND lower(lkp.source_material_code) = lower(rec.material_classification)
      AND lower(lkp.metric_code) = lower(rec.metric_code)
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS s on lkp.site_code=s.site_code and s.`__is_deleted`='N'
WHERE rec.`__is_deleted` = 'N'

-- OMS LBR
UNION ALL
SELECT
  l.system_code,
  v.site_name,
  s.metric_date,
  'NOT_APPLICABLE' AS site_system_shift_date_code,
  l.asset_code,
  sc.scenario_code,
  l.process_code,
  l.department_code,
  l.site_reporting_group_code,
  '' AS reporting_period_code,
  'NOT_APPLICABLE' AS functional_location_code,
  l.kda,
  l.material_code,
  l.metric_code as measure_name,
  l.data_indicator_code,
  l.unit_of_measure_code,
  (s.metric_value * l.cal_multiplier) as measure_value,
  TRUE AS is_active,
  0 AS exception_flag
FROM  group_aadp_l3_l4_{{var('env')}}.staging_l3.oms_mining_processing AS s
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON l.__is_deleted = 'N'
        AND l.is_active = TRUE
        AND l.source_table_name = s.source_table_name
        AND l.source_site_name = s.source_site_name
        AND l.source_system_code = s.source_system_name
        AND lower(l.source_mapping_code) = lower(s.source_mapping_code)
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS v ON v.__is_deleted = 'N'
        AND v.site_code = l.site_code
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_planning_scenario AS sc on  sc.`__is_deleted` = 'N'
        AND concat('|', lower(trim(sc.scenario_code)), '|', lower(trim(sc.lookup_values)), '|')
                        like concat('%|', lower(trim(s.scenario_code)),'|%')
WHERE s.__is_deleted = 'N'

-- Deswik
UNION ALL
Select
  l.system_code,
  v.site_name,
  s.reporting_date AS date,
  'NOT_APPLICABLE' AS site_system_shift_date_code,
  l.asset_code,
  l.scenario_code,
  l.process_code,
  l.department_code,
  l.site_reporting_group_code,
  '' AS reporting_period_code,
  'NOT_APPLICABLE' AS functional_location_code,
  l.kda,
  s.material_code,
  l.metric_code as measure_name,
  l.data_indicator_code,
  l.unit_of_measure_code,
  sum(s.tonnes * l.cal_multiplier) as measure_value,
  TRUE AS is_active,
  0 AS exception_flag
FROM  group_aadp_l3_l4_{{var('env')}}.staging_l3.deswik_mining_metrics AS s
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON l.__is_deleted = 'N'
        AND l.is_active= TRUE
        AND lower(l.source_system_code) = lower(s.source_system_name)
        AND lower(l.source_site_name) = lower(s.site_name)
        AND lower(l.source_table_name) = lower(s.source_table_name)
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS v ON v.__is_deleted = 'N'
        AND lower(v.site_code) = lower(l.site_code)
group by
  l.system_code, v.site_name, s.reporting_date, l.asset_code,
  l.scenario_code, l.process_code, l.department_code, l.site_reporting_group_code,
  l.kda, s.material_code, l.metric_code, l.data_indicator_code, l.unit_of_measure_code

-- plan forms measures
UNION ALL
SELECT
  l.system_code as system_code,
  v.site_name,
  p.measure_date as metric_date,
  'NOT_APPLICABLE' AS site_system_shift_date_code,
  l.asset_code,
  sc.scenario_code as scenario_code,
  l.process_code,
  l.department_code,
  l.site_reporting_group_code,
  '' AS reporting_period_code,
  'NOT_APPLICABLE' AS functional_location_code,
  l.kda,
  l.material_code,
  l.metric_code as measure_name,
  l.data_indicator_code,
  l.unit_of_measure_code,
  SUM((p.measure_value * l.cal_multiplier)) as measure_value,
  TRUE AS is_active,
  0 AS exception_flag
FROM  group_aadp_l3_l4_{{var('env')}}.staging_l3.plan_forms_measures AS p
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON l.__is_deleted = 'N'
        AND l.is_active = TRUE
        AND l.source_site_name = p.site_name
        AND l.system_code = p.source_system_name
        AND l.metric_code = p.measure_name
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS v ON v.__is_deleted = 'N'
        AND v.site_code = l.site_code
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_planning_scenario AS sc on  sc.`__is_deleted` = 'N'
        AND concat('|', lower(trim(sc.scenario_code)), '|', lower(trim(sc.lookup_values)), '|')
                        like concat('%|', lower(trim(p.plan_type)),'|%')
WHERE p.__is_deleted = 'N'
GROUP BY l.system_code, v.site_name, p.measure_date, l.asset_code,
sc.scenario_code, l.process_code, l.department_code, l.site_reporting_group_code,
l.kda, l.material_code, l.metric_code, l.data_indicator_code, l.unit_of_measure_code

UNION ALL
--------------SGM QVC MINING PROCESSING DATA
SELECT l.system_code AS system_code,
  v.site_name,
  s.reporting_date_month AS DATE,
  'NOT_APPLICABLE' AS site_system_shift_date_code,
  l.asset_code,
  l.scenario_code,
  l.process_code,
  l.department_code,
  l.site_reporting_group_code,
  '' AS reporting_period_code,
  'NOT_APPLICABLE' AS functional_location_code,
  l.kda,
  s.material_code,
  s.measure_name,
  l.data_indicator_code,
  l.unit_of_measure_code,
  nvl((s.measure_value * l.cal_multiplier), 0) as measure_value,
  TRUE AS is_active,
  0 AS exception_flag
FROM {{ ref('sgm_qvc_mining_proc_data') }} AS s
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON l.`__is_deleted` = 'N'
  AND l.is_active = TRUE
  AND lower(l.source_system_code) = lower(s.source_system_name)
  AND lower(l.source_site_name) = lower(s.site_name)
  AND lower(l.source_table_name) = lower(s.source_table_name)
  AND lower(s.asset_code) = lower(l.asset_code)
  AND lower(s.measure_name) = lower(L.metric_code)
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS v ON v.__is_deleted = 'N'
  AND lower(v.site_code) = lower(l.site_code)
WHERE s.`__is_deleted` = 'N'

UNION ALL

---------OPCO WATER MINING PROCESSING DATA 
SELECT l.system_code AS system_code,
  v.site_name,
  s.reporting_date_month AS DATE,
  'NOT_APPLICABLE' AS site_system_shift_date_code,
  l.asset_code,
  l.scenario_code,
  l.process_code,
  l.department_code,
  l.site_reporting_group_code,
  '' AS reporting_period_code,
  'NOT_APPLICABLE' AS functional_location_code,
  l.kda,
  s.material_code,
  s.measure_name,
  l.data_indicator_code,
  l.unit_of_measure_code,
  (s.measure_value * l.cal_multiplier) as measure_value,
  TRUE AS is_active,
  0 AS exception_flag
FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.opco_water_mining_proc_data AS s
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON l.`__is_deleted` = 'N'
  AND l.is_active = TRUE
  AND lower(l.source_system_code) = lower(s.source_system_name)
  AND lower(l.source_site_name) = lower(s.site_name)
  AND lower(l.source_table_name) = lower(s.source_table_name)
  AND lower(s.asset_code) = lower(l.asset_code)
  AND lower(s.measure_name) = lower(L.metric_code)
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS v ON v.__is_deleted = 'N'
  AND lower(v.site_code) = lower(l.site_code)
WHERE s.`__is_deleted` = 'N' 

UNION ALL

---------starlims MINING PROCESSING DATA 
SELECT l.system_code AS system_code,
  v.site_name,
  s.reporting_date_month AS DATE,
  'NOT_APPLICABLE' AS site_system_shift_date_code,
  l.asset_code,
  l.scenario_code,
  l.process_code,
  l.department_code,
  l.site_reporting_group_code,
  '' AS reporting_period_code,
  'NOT_APPLICABLE' AS functional_location_code,
  l.kda,
  s.material_code,
  s.measure_name,
  l.data_indicator_code,
  l.unit_of_measure_code,
  (s.measure_value * l.cal_multiplier) as measure_value,
  TRUE AS is_active,
  0 AS exception_flag
FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.starlims_mining_proc_data AS s
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON l.`__is_deleted` = 'N'
  AND lower(l.source_system_code) = lower(s.source_system_name)
  AND lower(l.source_site_name) = lower(s.site_name)
  AND lower(l.source_table_name) = lower(s.source_table_name)
  AND lower(l.material_code) = lower(s.material_code)
  AND lower(s.measure_name) = lower(L.metric_code)
  AND l.is_active = TRUE
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS v ON v.__is_deleted = 'N'
  AND lower(v.site_code) = lower(l.site_code)
WHERE s.`__is_deleted` = 'N' 

UNION ALL

---------sap site equipment activity DATA 
SELECT l.system_code AS system_code,
  v.site_name,
  s.reporting_date_month AS DATE,
  'NOT_APPLICABLE' AS site_system_shift_date_code,
  l.asset_code,
  l.scenario_code,
  l.process_code,
  l.department_code,
  l.site_reporting_group_code,
  '' AS reporting_period_code,
  'NOT_APPLICABLE' AS functional_location_code,
  l.kda,
  s.material_code,
  s.measure_name,
  l.data_indicator_code,
  l.unit_of_measure_code,
  (s.measure_value * l.cal_multiplier) as measure_value,
  TRUE AS is_active,
  0 AS exception_flag
FROM group_aadp_l3_l4_{{var('env')}}.staging_l3.sap_site_equip_activity_raw AS s
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON l.`__is_deleted` = 'N'
  AND lower(l.source_system_code) = lower(s.source_system_name)
  AND lower(l.source_site_name) = lower(s.site_name)
  AND lower(l.source_table_name) = lower(s.source_table_name)
  AND lower(l.material_code) = lower(s.material_code)
  AND lower(s.measure_name) = lower(L.metric_code)
  AND lower(s.site_reporting_group_code) = lower(L.site_reporting_group_code)
  AND lower(s.data_indicator_code) = lower(L.data_indicator_code)
  AND l.is_active = TRUE
INNER JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_v2 AS v ON v.__is_deleted = 'N'
  AND lower(v.site_code) = lower(l.site_code)
WHERE s.`__is_deleted` = 'N'  )
SELECT
  system_code,
  site_name,
  date,
  site_system_shift_date_code,
  asset_code,
  scenario_code,
  process_code,
  department_code,
  site_reporting_group_code,
  reporting_period_code,
  functional_location_code,
  kda,
  material_code,
  measure_name,
  data_indicator_code,
  unit_of_measure_code,
  measure_value,
  is_active,
  exception_flag,
{{add_tech_columns(this)}}
FROM all_data