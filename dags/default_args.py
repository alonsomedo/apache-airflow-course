from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

# All the arguments available for default_args are the ones that belongs to the BaseOperator.
default_args = {
    "owner": "DSRP Class",
    "start_date": datetime(2023,5,22),
    "depends_on_past": True,
    "execution_timeout": timedelta(seconds=7),
    "retries": 3,
    "retry_delay": timedelta(secods=10)
}

dag = DAG(dag_id="default_args",
          schedule='@daily',
          catchup=False,
          default_args=default_args
          )

extract = EmptyOperator(dag=dag, task_id='extract')

transform = BashOperator(dag=dag, task_id='transform', bash_command='sleep 10')

load = EmptyOperator(dag=dag, task_id='load')

extract >> transform >> load