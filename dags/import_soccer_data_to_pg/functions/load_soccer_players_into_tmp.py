import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook 

def load_soccer_players_into_tmp(soccer_team_id, **context):
    file_path = f'/tmp/{soccer_team_id}.csv'
    pg_hook = PostgresHook()
    pg_hook.bulk_load(context["templates_dict"]["tmp_table"], file_path)
    logging.info("The data was loaded successfully.")