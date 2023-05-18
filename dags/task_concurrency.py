from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime


dag = DAG(dag_id="task_concurrency",
          start_date=datetime(2023,5,15),
          schedule='@daily',
          catchup=True,
          max_active_runs=1,
          max_active_tasks=2
        )

extract = EmptyOperator(dag=dag, task_id='extract')

process_a = BashOperator(dag=dag, task_id='process_a', bash_command='sleep 5')
process_b = BashOperator(dag=dag, task_id='process_b', bash_command='sleep 5')
process_c = BashOperator(dag=dag, task_id='process_c', bash_command='sleep 5')
process_d = BashOperator(dag=dag, task_id='process_d', bash_command='sleep 5')

load = EmptyOperator(dag=dag, task_id='load')

extract >> [process_a, process_b, process_c, process_d] >> load