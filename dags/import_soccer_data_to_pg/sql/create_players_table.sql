
CREATE TABLE IF NOT EXISTS {{ params.table_name }}
(
    player_id int,
    name varchar(200),
    age int,
    number varchar(200),
    position varchar(200),
    photo varchar(200),
    team_id int,
    inserted_date date
);