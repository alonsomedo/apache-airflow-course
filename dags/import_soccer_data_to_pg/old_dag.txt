import json
import requests
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook

from datetime import datetime, timedelta
from pandas import json_normalize

# Updates
# All the arguments available for BaseOperator are available too for default_args
default_args = {
    'owner': 'DataOperations',
    'retries': 5,
    'retry_delay': timedelta(seconds=30),
    'execution_timeout': timedelta(seconds=500)
}


with DAG(
    dag_id='import_soccer_data',
    start_date=datetime(2023,4,19),
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:
    
    def get_soccer_team_players(soccer_team_id):
        url = 'https://v3.football.api-sports.io/players/squads'
        params = {'team': soccer_team_id}
        headers = {
            'x-rapidapi-host': "v3.football.api-sports.io",
            'x-rapidapi-key': Variable.get('api_key_soccer')
        }
        response = requests.get(url, params=params, headers=headers)
        print(response.text)
        data = json.loads(response.text)['response'][0]['players']
        data = json_normalize(data)
        data.to_csv(f'/opt/airflow/data/{soccer_team_id}.csv', sep='\t', index=False, header=False)
        logging.info(f"The file {soccer_team_id}.csv was generated successfully.")
    
    def load_soccer_team_players(soccer_team_id):
        file_path = f'/opt/airflow/data/{soccer_team_id}.csv'
        mysql_hook = MySqlHook()
        mysql_hook.bulk_load('soccer_players', file_path)
        logging.info("The data was loaded successfully.")
        
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    soccer_players_table = MySqlOperator(
        task_id='create_soccer_players_table',
        sql="""
            CREATE TABLE IF NOT EXISTS soccer_players 
            (
                player_id int,
                name varchar(100),
                age int,
                number int,
                position varchar(100),
                photo varchar(200) 
            );
        """)
    
    get_soccer_players = PythonOperator(
        task_id='get_scoccer_players',
        python_callable=get_soccer_team_players,
        op_args=[529]
    )
    
    load_soccer_players = PythonOperator(
        task_id='load_scoccer_players',
        python_callable=load_soccer_team_players,
        op_args=[529]
    )
    
    start >> soccer_players_table >> get_soccer_players >> load_soccer_players >> end
    
