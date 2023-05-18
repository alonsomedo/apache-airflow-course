from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime


dag = DAG(dag_id="example-2",
          start_date=datetime(2023,5,14),
          schedule='@daily',
          catchup=True,
          max_active_runs=1
          )

#extract =  BashOperator(dag=dag, task_id='extract', bash_command='exit 1', depends_on_past=True)
extract =  BashOperator(dag=dag, task_id='extract', bash_command='exit 1', wait_for_downstream=True)

transform = BashOperator(dag=dag, task_id='transform', bash_command='sleep 10')

load = BashOperator(dag=dag, task_id='load', bash_command='sleep 10')

extract >> transform >> load