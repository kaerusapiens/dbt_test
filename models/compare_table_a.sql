-- target_x table name
-- target_y table name
-- target_x columns
-- target_y columns
{{ generate_single_comparison(
    'table_a',
    'table_a',
    ['ids', 'name'],
    ['id', 'name']
) }}
