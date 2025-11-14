SELECT 
    1 as id, 
    'First successful row data' as message,
    CURRENT_TIMESTAMP as created_at
UNION ALL
SELECT 
    2 as id, 
    'Airflow run confirmed' as message,
    CURRENT_TIMESTAMP as created_at
UNION ALL
SELECT 
    3 as id, 
    'New data from the latest run' as message,
    CURRENT_TIMESTAMP as created_at