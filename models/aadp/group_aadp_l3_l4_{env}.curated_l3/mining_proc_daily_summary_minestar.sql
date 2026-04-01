{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['system_code', 'site_reporting_group_code', 'source_table_name', 'site_system_shift_date_code', 'department_code', 'process_code', 'asset_code', 'scenario_code', 'material_code', 'metric_code', 'reporting_date', 'amount', 'unit_of_measure_code', 'kda']) ,
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
        pre_hook = "  DELETE FROM {{ this }} AS mp
 WHERE CASE
        WHEN '{{var('system')}}' in ('ALL', '') then NVL(system_code, '')
                         ELSE '{{var('system')}}'
                       END LIKE CONCAT('%', NVL(system_code, ''), '%')
 AND EXISTS (SELECT 1
               FROM group_aadp_l3_l4_{{var('env')}}.curated_l3.site_reporting_group_V2 AS srg
              WHERE srg.site_reporting_group_code = mp.site_reporting_group_code
                AND srg.__is_deleted = 'N'
                AND CASE
                      WHEN '{{var('site')}}' in ('ALL', '') then NVL(srg.site_code, '')
                      ELSE '{{var('site')}}'
                    END LIKE CONCAT('%', NVL(srg.site_code, ''), '%')
            )
 AND reporting_date >= DATE_SUB(CURRENT_DATE(), {{var('refresh_days')}}) " if is_incremental() else "",
        unique_key = ['mining_proc_daily_summary_minestar_sk']
    )
}}

WITH operational_times
AS (
  SELECT os.MACHINE,
    os.source_system_name,
    os.site_name,
    os.LOGIN_UTC,
    os.logout_utc,
    dc.site_system_shift_date_code,
    dc.reporting_date,
    ROW_NUMBER() OVER (
      PARTITION BY os.OPERATORSHIFT_OID,
      os.MACHINE ORDER BY greatest(0, unix_timestamp(least(os.LOGOUT_UTC, dc.utc_end_date_time)) - unix_timestamp(greatest(os.LOGIN_UTC, dc.utc_start_date_time))) DESC,
        abs(unix_timestamp(dc.utc_start_date_time) - unix_timestamp(os.LOGIN_UTC)) ASC
      ) AS rn
  FROM {{ source('cat_minestar_l2', 'vw_operator_shift') }} AS os
  JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS stl
    ON stl.source_system_name = os.source_system_name
      AND stl.source_site_code = os.site_name
      AND stl.__is_deleted = 'N'
  JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.site_system_shift_date AS dc
    ON dc.system_code = stl.system_id
      AND dc.site_code = stl.site_id
      AND dc.utc_start_date_time < os.logout_utc
      AND dc.utc_end_date_time > os.LOGIN_UTC
      AND dc.__is_deleted = 'N'
  WHERE os.__is_deleted = 'N'
    AND os.LOGIN_UTC >= '2021-01-01'
  ),
b2b_times
AS (
  SELECT cy.site_name,
    cy.source_system_name,
    cy.primary_machine_name AS asset_name,
    date_add(HOUR, - 5, min(ca.START_TIME_UTC)) AS local_start,
    date_add(HOUR, - 5, max(ca.END_TIME_UTC)) AS local_end,
    os.site_system_shift_date_code,
    os.reporting_date
  FROM {{ source('cat_minestar_l2', 'vw_cycle') }} AS cy
  INNER JOIN {{ source('cat_minestar_l2', 'vw_cycle_activity_component') }} AS ca
    ON ca.OID = cy.cycle_OID
      AND cy.source_system_name = ca.source_system_name
      AND cy.site_name = ca.site_name
      AND ca.`__is_deleted` = 'N'
  LEFT JOIN operational_times AS os
    ON cy.PRIMARY_MACHINE = os.MACHINE
      AND cy.source_system_name = os.source_system_name
      AND cy.site_name = os.site_name
      AND ca.START_TIME_UTC <= os.LOGOUT_UTC
      AND ca.END_TIME_UTC >= os.LOGIN_UTC
      AND rn = 1
  WHERE cy.ECF_CLASS_ID = 'XAEntity.Cycle.ProductionCycle.LoaderCycle'
    AND ca.NAME = 'Loading'
    AND cy.`__is_deleted` = 'N'
    AND ca.START_TIME_UTC >= '2021-01-01'
  GROUP BY cy.source_system_name,
    cy.site_name,
    cy.primary_machine_name,
    os.LOGIN_UTC,
    os.LOGOUT_UTC,
    os.site_system_shift_date_code,
    os.reporting_date
  ),
b2b_calc
AS (
  SELECT DISTINCT ca.site_name,
    ca.source_system_name,
    'cat_minestar.vw_cycle_activity_component' AS source_table_name,
    'NOT_APPLICABLE' AS source_cycle_type_name,
    ca.reporting_date,
    ca.site_system_shift_date_code,
    ca.asset_name,
    'NOT_APPLICABLE' AS material_code,
    'NOT_APPLICABLE' AS source_block_code,
    ca.local_start,
    timestampdiff(second, lag(local_end) OVER (
        PARTITION BY asset_name ORDER BY local_start ASC
        ), local_start) AS amount
  FROM b2b_times AS ca
  ),
CTE_qvc_minestar_miningblock
AS (
  SELECT DISTINCT bl.site_name,
    bl.source_system_name,
    'cat_minestar.vw_v_miningblock' AS source_table_name,
    'NOT_APPLICABLE' AS source_cycle_type_name,
    date_format(bl.create_time, 'yyyy-MM-dd') AS reporting_date,
    'NOT_APPLICABLE' AS site_system_shift_date_code,
    'NOT_APPLICABLE' AS asset_name,
    bl.material_N AS material_code,
    'NOT_APPLICABLE' AS source_block_code,
    'NOT_APPLICABLE' AS source_mapping_code,
    bl.original_mass_q - (
    CASE 
      WHEN bl.original_mass_q > bl.mined_mass_q
        THEN (
            CASE 
              WHEN bl.mined_mass_q > 0
                THEN bl.mined_mass_q
              ELSE 0
              END
            )
      ELSE bl.original_mass_q
      END
    ) AS amount
  FROM {{ source('cat_minestar_l2', 'vw_v_miningblock') }} AS bl
  WHERE bl.`__is_deleted` = 'N'

  UNION
  
  SELECT A.site_name,
    a.source_system_name,
    'cat_minestar.vw_cycle_s_fact_main' AS source_table_name,
    A.cycle_type AS source_cycle_type_name,
    to_timestamp(replace(a.calendar_day, 'Day ', ''), 'dd MMM yyyy') AS reporting_date,
    'NOT_APPLICABLE' AS site_system_shift_date_code,
    A.secondary_machine_name AS asset_name,
    'NOT_APPLICABLE' AS material_code,
    'NOT_APPLICABLE' AS source_block_code,
    inline(arrays_zip(array('equivalent_distance', 'equivalent_distance_count'), array(sum(A.total_efh_length), count(1)))) AS (
      source_mapping_code,
      amount
      )
  FROM {{ source('cat_minestar_l2', 'vw_cycle_s_fact_main') }} AS A
  WHERE A.cycle_type = 'TruckCycle'
    AND A.__is_deleted = 'N'
    AND to_timestamp(replace(a.calendar_day, 'Day ', ''), 'dd MMM yyyy') >= '2025-01-01'
    AND a.PAYLOAD > 0
  GROUP BY all

  UNION

  SELECT site_name,
    source_system_name,
    source_table_name,
    source_cycle_type_name,
    reporting_date,
    site_system_shift_date_code,
    asset_name,
    material_code,
    source_block_code,
    inline(arrays_zip(array('previous_to_next_load', 'previous_to_next_load_count'), array(sum(amount), count(1)))) AS (
      source_mapping_code,
      amount
      )
  FROM b2b_calc AS b2b
  WHERE amount IS NOT NULL
  GROUP BY all
    ),
CTE_mining_proc_daily_summary 
AS (
SELECT CONCAT_WS('-', l.system_code, l.site_reporting_group_code, m.reporting_date, l.site_system_shift_date_code, NVL(asset.asset_code, 'NOT_APPLICABLE'), l.material_code, l.metric_code) AS mining_proc_daily_summary_minestar_sk,
  l.system_code,
  l.site_reporting_group_code,
  l.source_table_name,
  l.site_system_shift_date_code,
  l.department_code,
  l.process_code,
  NVL(asset.asset_code, 'NOT_APPLICABLE') AS asset_code,
  l.scenario_code,
  l.material_code,
  l.metric_code,
  m.reporting_date,
  SUM(m.amount) AS amount,
  l.unit_of_measure_code,
  l.kda
FROM {{ ref('minestar_cycle_raw') }} AS m
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_minestar_lookup AS l
  ON l.__is_deleted = 'N'
    AND m.source_table_name = l.source_table_name
    AND m.source_system_name = l.source_system_name
    AND m.site_name = l.source_site_code    
    AND m.source_cycle_type_name = l.source_cycle_type_name
    AND m.source_material_group_name = l.source_material_group_name
    AND m.source_material_destination = l.source_material_destination
    AND CASE 
      WHEN l.source_block_operator = '='
        THEN m.source_block_code = l.source_block_code
      WHEN l.source_block_operator = '<>'
        THEN m.source_block_code <> l.source_block_code
          AND m.source_block_code <> 'NOT_APPLICABLE'
      ELSE m.source_block_code = l.source_block_code
      END
    AND l.source_mapping_code = 'NOT_APPLICABLE'
    AND l.is_active = TRUE
LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.asset AS asset
  ON m.asset_name = asset.asset_name
    AND asset.__is_deleted = 'N'
WHERE m.__is_deleted = 'N'
GROUP BY l.system_code,
  l.site_reporting_group_code,
  l.source_table_name,
  l.site_system_shift_date_code,
  l.department_code,
  l.process_code,
  asset.asset_code,
  l.scenario_code,
  l.material_code,
  l.metric_code,
  m.reporting_date,
  l.unit_of_measure_code,
  l.kda

UNION

SELECT CONCAT_WS('-', l.system_code, l.site_reporting_group_code, m.reporting_date, m.site_system_shift_date_code, NVL(asset.asset_code, 'NOT_APPLICABLE'), NVL(m.material_code, 'NOT_APPLICABLE'), l.metric_code) AS mining_proc_daily_summary_minestar_sk,
  l.system_code,
  l.site_reporting_group_code,
  l.source_table_name,
  m.site_system_shift_date_code,
  l.department_code,
  l.process_code,
  NVL(asset.asset_code, 'NOT_APPLICABLE') AS asset_code,
  l.scenario_code,
  NVL(m.material_code, 'NOT_APPLICABLE') AS material_code,
  l.metric_code,
  m.reporting_date,
  SUM(m.amount) AS amount,
  l.unit_of_measure_code,
  l.kda
FROM CTE_qvc_minestar_miningblock AS m
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_minestar_lookup AS l
  ON m.source_table_name = l.source_table_name
    AND m.source_system_name = l.source_system_name
    AND m.site_name = l.source_site_code
    AND m.source_cycle_type_name = l.source_cycle_type_name
    AND m.source_mapping_code = l.source_mapping_code
    AND l.is_active = TRUE
    AND l.__is_deleted = 'N'
LEFT JOIN group_aadp_l3_l4_{{var('env')}}.curated_l3.asset AS asset
  ON m.asset_name = asset.asset_name
    AND asset.__is_deleted = 'N'
GROUP BY l.system_code,
  l.site_reporting_group_code,
  l.source_table_name,
  m.site_system_shift_date_code,
  l.department_code,
  l.process_code,
  asset.asset_code,
  l.scenario_code,
  m.material_code,
  l.metric_code,
  m.reporting_date,
  l.unit_of_measure_code,
  l.kda
)
SELECT m.mining_proc_daily_summary_minestar_sk,
  m.system_code,
  m.site_reporting_group_code,
  m.source_table_name,
  m.site_system_shift_date_code,
  m.department_code,
  m.process_code,
  m.asset_code,
  m.scenario_code,
  m.material_code,
  m.metric_code,
  m.reporting_date,
  m.amount,
  m.unit_of_measure_code,
  m.kda,
  {{add_tech_columns(this)}}
FROM CTE_mining_proc_daily_summary AS m
JOIN group_aadp_l3_l4_{{var('env')}}.staging_l3.man_site_lookup AS l
  ON m.system_code = l.system_id
 AND m.site_reporting_group_code = l.site_rep_grp_id
 AND CASE
       WHEN '{{var('system')}}' IN ('ALL', '') THEN l.system_id
       ELSE '{{var('system')}}'
     END LIKE CONCAT ('%', l.system_id, '%')
 AND CASE
       WHEN '{{var('site')}}' in ('ALL', '') THEN l.site_id
       ELSE '{{var('site')}}'
     END LIKE concat('%', l.site_id, '%')
 AND l.__is_deleted = 'N'
WHERE m.reporting_date >= date_sub(current_date(), {{var('refresh_days')}}) 