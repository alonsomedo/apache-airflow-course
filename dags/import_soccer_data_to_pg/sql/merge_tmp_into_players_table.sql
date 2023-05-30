DELETE FROM {{ params.table_name }}
WHERE inserted_date = '{{ ds }}'
;

INSERT INTO {{ params.table_name }}
(
    player_id,
    name,
    age,
    number,
    position,
    photo,
    team_id,
    inserted_date
)
SELECT
    cast(player_id as int) player_id,
    name,
    cast(age as int) age,
    number,
    position,
    photo,
    cast(team_id as int),
    '{{ ds }}' inserted_date
FROM bronze.tmp_players_table_{{ ds_nodash }}
;