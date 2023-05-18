from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime


with DAG(dag_id="wait_for_downstream",
          start_date=datetime(2023,5,14),
          schedule='@daily',
          catchup=True,
          max_active_runs=2
        ) as dag:

  extract = BashOperator(task_id='extract', 
                          bash_command='sleep 15',
                          wait_for_downstream=True)

  transform = BashOperator(task_id='transform', 
                          bash_command='sleep 7')

  load = EmptyOperator(task_id='load')

  extract >> transform >> load