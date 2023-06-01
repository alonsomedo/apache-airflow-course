from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor

from datetime import datetime, timedelta

with DAG(
    dag_id='ets_master',
    #start_date=datetime(2023,5,31),
    #schedule='@daily',
    start_date=datetime(2023,6,1,0,46),
    schedule='*/3 * * * *',
    catchup=False,
    tags=['external_task_sensor', 'master']
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    sensor = ExternalTaskSensor(
        task_id = 'sensor',
        external_dag_id='ets_slave',
        external_task_id='download',
        poke_interval=30,
        execution_delta=timedelta(minutes=3)
    )
    
    extract = EmptyOperator(dag=dag, task_id='extract')

    transform = BashOperator(dag=dag, task_id='transform', bash_command='sleep 5')

    load = EmptyOperator(dag=dag, task_id='load') 
    
    start >> sensor >> extract >> transform >> load >> end