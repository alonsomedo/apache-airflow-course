from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta

with DAG(
    dag_id='ets_slave',
    #start_date=datetime(2023,5,31),
    #schedule='@daily',
    start_date=datetime(2023,6,1,0,44),
    schedule='*/3 * * * *',
    catchup=False,
    tags=['external_task_sensor', 'slave']
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    download = BashOperator(task_id='download', bash_command='sleep 12')
    
    merge = BashOperator(task_id='merge', bash_command='sleep 30')

    notify = EmptyOperator(task_id='notify') 
    
    start >> download >> merge >> notify >> end