from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from datetime import datetime, timedelta
from pandas import json_normalize

import json

def process_user(ti):
    user = ti.xcom_pull(task_ids="extract_user_data")
    user = user["results"][0]
    processed_user = json_normalize(
        {
            "firstname": user['name']['first'],
            "lastname": user['name']['last'],
            "country": user['location']['country'],
            "username": user['login']['username'],
            "password": user['login']['password'],
            "email": user['email']
        }
    )
    processed_user.to_csv('/tmp/processed_user.csv', index=False, header=False)
    
def store_user():
    pg_hook = PostgresHook() # As the connection for Postgres was defined with the default_connection name, is not necessary to set it.
    pg_hook.copy_expert(sql=" COPY bronze.users FROM stdin WITH DELIMITER as ',' ",
                        filename='/tmp/processed_user.csv')


with DAG(
    dag_id='user_processing',
    start_date=datetime(2023,5,24),
    schedule='@daily',
    catchup=False
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    ## airflow tasks test user_processing create_user_table 2023-05-20
    ## docker exec -ti apache-airflow-course-postgres-dwh-1 psql -U dwh
    create_table = PostgresOperator(
        task_id="create_user_table",
        sql=""" 
            CREATE TABLE IF NOT EXISTS bronze.users(
                firstname TEXT NOT NULL,
                lastname TEXT NOT NULL,
                country TEXT NOT NULL,
                username TEXT NOT NULL,
                password TEXT NOT NULL,
                email TEXT NOT NULL
            )
        """ 
    )
    
    ## airflow tasks test user_processing is_api_available 2023-05-20
    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id ="random_user_api",
        endpoint="api/",
        poke_interval=60*5, # Wait 5 minutes until next retry.
        timeout=60*60*1, # Trigger a timeout exception after 1 hour.
        mode="poke"
    )
    
    ## airflow tasks test user_processing extract_user_data 2023-05-20
    extract_user_data = SimpleHttpOperator(
        task_id='extract_user_data',
        http_conn_id="random_user_api",
        endpoint="api/",
        method="GET",
        response_filter=lambda response: response.json(),
        log_response=True
    )
    
    ## airflow dags test user_processing 2023-05-20
    process_user = PythonOperator(
        task_id='process_user',
        python_callable=process_user
    )
    
    ## airflow dags test user_processing 2023-05-20
    store_user = PythonOperator(
        task_id="store_user",
        python_callable=store_user
    )
    
    start >> create_table >> is_api_available >> extract_user_data >> process_user >> store_user >> end