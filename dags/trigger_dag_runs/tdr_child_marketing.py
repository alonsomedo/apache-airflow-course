from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

with DAG(
    dag_id='tdr_child_marketing',
    schedule=None,
    start_date=datetime(2023,5,31),
    catchup=False,
    tags=['trigger_dag_runs', 'child' ,'marketing']
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    transform = BashOperator(task_id='download', bash_command='sleep 3')
    
    load_to_snowflake = BashOperator(task_id='load_to_snowflake', bash_command='sleep 10')

    start >> transform >> load_to_snowflake >> end