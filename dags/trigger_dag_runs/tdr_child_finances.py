from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

with DAG(
    dag_id='tdr_child_finances',
    start_date=datetime(2023,5,31),
    schedule=None,
    catchup=False,
    tags=['trigger_dag_runs', 'child' ,'finances']
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    transform = BashOperator(task_id='download', bash_command='sleep 3')
    
    load_to_snowflake = BashOperator(task_id='load_to_snowflake', bash_command='sleep 10')

    start >> transform >> load_to_snowflake >> end