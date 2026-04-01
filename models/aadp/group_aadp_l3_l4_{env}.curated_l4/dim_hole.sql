{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "incremental",
        matched_condition = generate_matched_condition(['site_code', 'site_name', 'system_code', 'source_system_name', 'hole_id', 'hole_name', 'block']) ,
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
        pre_hook = "  DELETE FROM {{ this }}
WHERE CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN site_name ELSE '{{var('site')}}' END LIKE CONCAT ('%', site_name, '%')
 AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN source_system_name ELSE '{{var('system')}}' END LIKE CONCAT ('%', source_system_name, '%')   " if is_incremental() else "",
        unique_key = ['hole_sk']
    )
}}

select --row_number() over(order by site_code, system_code,hole_id,hole_name,block) as hole_sk,
(100000000000 * cast(site.site_sk AS BIGINT) + 1000000000 * cast(sys.system_sk AS BIGINT)) + 10000000 * crc32(CONCAT(A.hole_id, A.hole_name)) + crc32(A.block) AS hole_sk
,A.site_code
,A.site_name
,A.system_code
,A.source_system_name
,A.hole_id
,A.hole_name
,A.block,
{{add_tech_columns(this)}}
from {{ ref('hole') }} AS A
JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_site_v2 AS site
  ON site.site_code = a.site_code
    AND site.__is_deleted = 'N'
JOIN group_aadp_l3_l4_{{var('env')}}.curated_l4.dim_system AS sys
  ON sys.system_code = a.system_code
    AND sys.__is_deleted = 'N'
WHERE A.__is_deleted = 'N'
AND CASE WHEN '{{var('site')}}' IN ('ALL', '') THEN A.site_code ELSE '{{var('site')}}' END LIKE CONCAT ('%', A.site_code, '%')
AND CASE WHEN '{{var('system')}}' IN ('ALL', '') THEN A.system_code ELSE '{{var('system')}}' END LIKE CONCAT ('%', A.system_code, '%')  