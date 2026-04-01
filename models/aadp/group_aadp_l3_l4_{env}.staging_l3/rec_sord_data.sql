{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['source_table_name', 'source_site_name', 'source_system_name', 'metric_name', 'material_classification', 'ore_stream', 'attribute', 'period_start_date', 'period_end_date', 'metric_code', 'value']) ,
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
        unique_key = ['rec_sord_data_id']
    )
}}

with src as (
      Select
      sord_data_sk,
      metric_name,
      attribute,
      period_start,
      period_end,
      YEAR(period_start) as period_start_yr,
      YEAR(period_end) as period_end_yr,
      datediff(MONTH, period_start, period_end) as period_diff_in_months,
      case
            when datediff(MONTH, period_start, period_end) = 0 then concat_ws(' - ', metric_name, attribute, 'Monthly')
            when datediff(MONTH, period_start, period_end) between 1 and 12 AND YEAR(period_start) =  YEAR(period_end) then concat_ws(' - ', metric_name, attribute, 'YTD')
            when datediff(MONTH, period_start, period_end) = 11 AND YEAR(period_start) <  YEAR(period_end) then concat_ws(' - ', metric_name, attribute, 'Rolling12Months')
      else 'New cat' end as metric_code,
      material_classification,
      ore_stream,
      value,
      site_name,
      source_system_name
      from {{ source('reconcilor_l2', 'vw_sord_data') }} as r
      where `__is_deleted` = 'N'
)
/*
There are 2 scenarios where the source record fits in two categories when identified based on the preiod calculation.  
1) For January priod 01-01-2024 to 31-01-2024 record can be identified as both YTD as well as Monthly record. 
2) 01-01-2024 to 31-12-2024 record can be identified as 12MonthsRolling as well as YTD.   
But we only see one entry from the source and hence need to be duplicated the value to fit into two periods.  This is achieved in below CTE (adjustment_rec)
*/
,adjustment_rec as (
Select sord_data_sk,
      metric_name,
      attribute,
      period_start,
      period_end, 
      period_start_yr, 
      period_end_yr, 
      period_diff_in_months, 
      case 
            when metric_code like '%- Monthly' and month(period_start) = 1 and month(period_end) = 1 then replace(metric_code, '- Monthly', '- YTD') 
            when metric_code like '%- YTD' and month(period_start) = 1 and month(period_end) = 12  then replace(metric_code,'- YTD', '- Rolling12Months')
      end as metric_code,
      material_classification,
      ore_stream,
      value,
      site_name,
      source_system_name
FROM  src 
where (metric_code like '%- Monthly' and month(period_start) = 1 and month(period_end) = 1 )
      or
      (metric_code like '%- YTD' and month(period_start) = 1 and month(period_end) = 12)
)
,r as (
-- Union the both datasets to get the complete result set
Select * from src
UNION all
Select * from adjustment_rec
)
Select
      concat_ws('_', l.source_table_name, r.site_name, r.metric_code, r.period_end) as rec_sord_data_id,
      l.source_table_name AS source_table_name,
      r.site_name AS source_site_name,
      r.source_system_name,
      r.metric_name,
      r.material_classification,
      r.ore_stream,
      r.attribute,
      r.period_start AS period_start_date,
      r.period_end AS period_end_date,
      r.metric_code,
      CAST(r.value AS DECIMAL(18,6)) AS value,
      {{add_tech_columns(this)}}
FROM  r
JOIN  group_aadp_l3_l4_{{var('env')}}.staging_l3.man_tactical_lookup AS l ON 1=1
      AND l.__is_deleted = 'N'
      AND l.is_active  = TRUE
      AND lower(l.source_table_name) = 'reconcilor.vw_sord_data'
      AND lower(l.source_site_name) = lower(r.site_name)
      AND lower(l.source_material_code) = lower(r.material_classification)
      AND l.metric_code = r.metric_code  