{%- set partition_col     = "__modified_date" -%}
{%- set interval          = 99 -%}
{%- set interval_type     = "years" -%}
{%- set interval_delimiter = "'" -%}
{{
    config(
        materialized = "table",
        schema='staging_l3',
        tags = ['resfresh'],
        target_alias = "tgt",
        source_alias = "src", 
        meta =  { 
        "partition_column": partition_col,
        "interval": interval, 
        "interval_type": interval_type,
        "interval_delimiter": interval_delimiter},
        unique_key = ['dbt_test_sk']
    )
}}

WITH date_sequence as(
    SELECT explode(sequence(TO_DATE('2026-01-01'), to_date(CAST(CONCAT(YEAR(CURRENT_DATE())+1, '-12-31') AS DATE)), interval 1 day)) as Date
)
SELECT
    CAST(REPLACE(CAST(Date AS STRING), '-', '') AS INT) AS dbt_test_sk,
    Date AS date,
    YEAR(Date) AS year,
    CAST(CONCAT(YEAR(Date), '-01-01') AS DATE) AS start_of_year,
    CAST(CONCAT(YEAR(Date), '-12-31') AS DATE) AS end_of_year,
    MONTH(Date) AS month,
    CAST(CONCAT(YEAR(Date), '-', LPAD(MONTH(Date), 2, '0'), '-01') AS DATE) AS start_of_month,
    LAST_DAY(Date) AS end_of_month,
    DATEDIFF(LAST_DAY(Date), CAST(CONCAT(YEAR(Date), '-', LPAD(MONTH(Date), 2, '0'), '-01') AS DATE)) + 1 AS days_in_month,
    DATE_FORMAT(Date, 'yyyyMM') AS year_month_number,
    DATE_FORMAT(Date, 'yyyy-MMM') AS year_month_name,
    DAY(Date) AS day,
    DATE_FORMAT(Date, 'EEEE') AS day_name,
    DATE_FORMAT(Date, 'E') AS day_name_short,
    DATE_FORMAT(Date, 'D') AS day_of_year,
    DATE_FORMAT(Date, 'MMMM') AS month_name,
    DATE_FORMAT(Date, 'MMM') AS month_name_short,
    QUARTER(Date) AS quarter,
    CONCAT('Q', DATE_FORMAT(Date, 'Q')) AS quarter_name,
    CAST(DATE_FORMAT(Date, 'yyyyQ') AS INT) AS year_quarter_number,
    CONCAT(DATE_FORMAT(Date, 'yyyy'), 'Q', DATE_FORMAT(Date, 'Q')) AS year_quarter_name
FROM date_sequence AS ds