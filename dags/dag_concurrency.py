from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime


dag = DAG(dag_id="dag_concurrency",
          start_date=datetime(2023,5,10),
          schedule='@daily',
          catchup=True,
          max_active_runs=1
          )

extract = EmptyOperator(dag=dag, task_id='extract')

transform = BashOperator(dag=dag, task_id='transform', bash_command='sleep 10')

load = EmptyOperator(dag=dag, task_id='load')

extract >> transform >> load