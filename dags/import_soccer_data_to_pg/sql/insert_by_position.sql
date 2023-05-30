
DELETE FROM {{ params.table_name }}
WHERE inserted_date = '{{ ds }}'
;

INSERT INTO {{ params.table_name }}
SELECT 
    *
FROM bronze.soccer_players
WHERE position = '{{ params.position }}'
AND inserted_date = '{{ ds }}'
;
    