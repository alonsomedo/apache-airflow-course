from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime


dag = DAG(dag_id="my-first-example",
          start_date=datetime(2023,5,16),
          schedule='@daily',
          catchup=False
          )

extract = EmptyOperator(dag=dag, task_id='extract')

transform = BashOperator(dag=dag, task_id='transform', bash_command='sleep 5')

load = EmptyOperator(dag=dag, task_id='load')

extract >> transform >> load