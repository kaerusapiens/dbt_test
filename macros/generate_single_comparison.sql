{% macro generate_single_comparison(target_x_table, target_y_table, unique_cols_x=[], unique_cols_y=[]) %}
-- Row Count Test
SELECT
  'row_count_mismatch' AS test_type,
  '{{ target_x_table }}' AS target_x_table,
  '{{ target_y_table }}' AS target_y_table,
  TO_JSON_STRING(STRUCT(cnt_x, cnt_y, diff AS x_minus_y)) AS detail
FROM (
  SELECT
    (SELECT COUNT(*) FROM `{{ target.project }}.{{ source('target_x', target_x_table).dataset }}.{{ target_x_table }}`) AS cnt_x,
    (SELECT COUNT(*) FROM `{{ target.project }}.{{ source('target_y', target_y_table).dataset }}.{{ target_y_table }}`) AS cnt_y,
    (SELECT COUNT(*) FROM `{{ target.project }}.{{ source('target_x', target_x_table).dataset }}.{{ target_x_table }}`) -
    (SELECT COUNT(*) FROM `{{ target.project }}.{{ source('target_y', target_y_table).dataset }}.{{ target_y_table }}`) AS diff
) t
WHERE cnt_x != cnt_y

UNION ALL
-- Column Name Mismatch (simplified)
SELECT
  'column_name_mismatch' AS test_type,
  '{{ target_x_table }}' AS target_x_table,
  '{{ target_y_table }}' AS target_y_table,
  TO_JSON_STRING(STRUCT(
    COALESCE(x.column_name, y.column_name) AS column_name,
    CASE
      WHEN x.column_name IS NULL THEN 'missing_in_x'
      WHEN y.column_name IS NULL THEN 'missing_in_y'
      ELSE NULL
    END AS mismatch_type
  )) AS detail
FROM (
  SELECT column_name
  FROM `{{ target.project }}.{{ source('target_x', target_x_table).dataset }}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{{ target_x_table }}'
) x
FULL OUTER JOIN (
  SELECT column_name
  FROM `{{ target.project }}.{{ source('target_y', target_y_table).dataset }}.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = '{{ target_y_table }}'
) y
ON x.column_name = y.column_name
WHERE x.column_name IS NULL OR y.column_name IS NULL

UNION ALL
-- Data Type Mismatch
SELECT
  'data_type_mismatch' AS test_type,
  '{{ target_x_table }}' AS target_x_table,
  '{{ target_y_table }}' AS target_y_table,
  TO_JSON_STRING(STRUCT(column_name, data_type_x, data_type_y)) AS detail
FROM (
  SELECT x.column_name, x.data_type AS data_type_x, y.data_type AS data_type_y
  FROM `{{ target.project }}.{{ source('target_x', target_x_table).dataset }}.INFORMATION_SCHEMA.COLUMNS` x
  JOIN `{{ target.project }}.{{ source('target_y', target_y_table).dataset }}.INFORMATION_SCHEMA.COLUMNS` y
    ON x.column_name = y.column_name
  WHERE x.table_name = '{{ target_x_table }}'
    AND y.table_name = '{{ target_y_table }}'
    AND x.data_type != y.data_type
) t

UNION ALL
-- Mode Mismatch
SELECT
  'mode_mismatch' AS test_type,
  '{{ target_x_table }}' AS target_x_table,
  '{{ target_y_table }}' AS target_y_table,
  TO_JSON_STRING(STRUCT(column_name, mode_x, mode_y)) AS detail
FROM (
  SELECT x.column_name, x.is_nullable AS mode_x, y.is_nullable AS mode_y
  FROM `{{ target.project }}.{{ source('target_x', target_x_table).dataset }}.INFORMATION_SCHEMA.COLUMNS` x
  JOIN `{{ target.project }}.{{ source('target_y', target_y_table).dataset }}.INFORMATION_SCHEMA.COLUMNS` y
    ON x.column_name = y.column_name
  WHERE x.table_name = '{{ target_x_table }}'
    AND y.table_name = '{{ target_y_table }}'
    AND x.is_nullable != y.is_nullable
) t

-- Unique Count Mismatch for multiple columns
{% for col_x, col_y in zip(unique_cols_x, unique_cols_y) %}
UNION ALL
SELECT
  'unique_count_mismatch' AS test_type,
  '{{ target_x_table }}' AS target_x_table,
  '{{ target_y_table }}' AS target_y_table,
  TO_JSON_STRING(STRUCT(
    '{{ col_x }}' AS column_x,
    '{{ col_y }}' AS column_y,
    unique_x,
    unique_y,
    unique_x - unique_y AS diff
  )) AS detail
FROM (
  SELECT
    (SELECT COUNT(DISTINCT {{ col_x }}) FROM `{{ target.project }}.{{ source('target_x', target_x_table).dataset }}.{{ target_x_table }}`) AS unique_x,
    (SELECT COUNT(DISTINCT {{ col_y }}) FROM `{{ target.project }}.{{ source('target_y', target_y_table).dataset }}.{{ target_y_table }}`) AS unique_y
) t
WHERE unique_x != unique_y
{% endfor %}
{% endmacro %}
