DROP TABLE IF EXISTS bronze.tmp_players_table_{{ ds_nodash }};

CREATE TABLE IF NOT EXISTS bronze.tmp_players_table_{{ ds_nodash }}
(
    player_id varchar(200),
    name varchar(200),
    age varchar(200),
    number varchar(200),
    position varchar(200),
    photo varchar(200),
    team_id varchar(200)
);
