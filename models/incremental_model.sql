{{ config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by={"field": "event_date", "data_type": "date"}
) }}


SELECT
    id,
    user_id,
    event_type,
    event_timestamp,
    DATE(event_timestamp) AS event_date
FROM {{ source('dbt_test', 'events') }}
WHERE
    {% if var('start_date', none) and var('end_date', none) %}
        --start_date と end_date が指定された場合はその期間のみを処理
        event_timestamp BETWEEN '{{ var("start_date") }}' AND '{{ var("end_date") }}'
    {% elif is_incremental() %}
		    -- テーブルが存在している場合実行される
        event_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 DAY)
    {% else %}
		    -- 初回実行時にはテーブルがまだ存在しないため is_incremental() が false となり、
		    -- start_date も指定されていないと、WHERE句の条件が何も出力されず、SQLエラーになり
        TRUE   
    {% endif %}