from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_parallel import subdag_parallel

from datetime import datetime, timedelta

# All the arguments available for default_args are the ones that belongs to the BaseOperator.
default_args = {
    "owner": "DSRP Class",
    "start_date": datetime(2023,5,22),
    "depends_on_past": True,
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}

with DAG(dag_id="subdags",
          schedule='@daily',
          catchup=False,
          default_args=default_args
          ) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    init_variables = BashOperator(task_id='init_variables', bash_command='sleep 3')
    processing = BashOperator(task_id='processing', bash_command='sleep 3')
    
    task_execution = SubDagOperator(
        task_id='task_execution',
        subdag=subdag_parallel('subdags', 'task_execution', default_args=default_args)
    )
    
    start >> init_variables >> task_execution >> processing >> end
