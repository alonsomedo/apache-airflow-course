from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

default_args = {
    "owner": "tdr"
}

with DAG(
    dag_id='tdr_parent',
    start_date=datetime(2023,5,31),
    schedule='@daily',
    catchup=False,
    tags=['trigger_dag_runs', 'parent']
) as dag:
    
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')
    
    download_loan_csv = BashOperator(task_id='download_loan_csv', bash_command='sleep 5')    
    
    trigger_marketing_process = TriggerDagRunOperator(
        task_id = 'trigger_marketing_process',
        trigger_dag_id = 'tdr_child_marketing',
        execution_date = '{{ ds }}',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=15
    )
    
    
    trigger_finances_process = TriggerDagRunOperator(
        task_id = 'trigger_finances_process',
        trigger_dag_id = 'tdr_child_finances',
        execution_date = '{{ ds }}',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=15
    )
    
    
    send_mail = BashOperator(task_id='send_mail', bash_command='sleep 5') 

    
    start >> download_loan_csv >> [trigger_marketing_process, trigger_finances_process] >> send_mail >> end