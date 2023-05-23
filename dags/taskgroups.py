from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

from datetime import datetime, timedelta

# All the arguments available for default_args are the ones that belongs to the BaseOperator.
default_args = {
    "owner": "DSRP Class",
    "start_date": datetime(2023,5,22),
    "depends_on_past": True,
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}

with DAG(dag_id="taskgroups",
          schedule='@daily',
          catchup=False,
          default_args=default_args
          ) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    init_variables = BashOperator(task_id='init_variables', bash_command='sleep 3')
    processing = BashOperator(task_id='processing', bash_command='sleep 3')
    
    # Parallel implementation
    # with TaskGroup('task_execution') as task_execution:    
    #     for i in range(1,31):
    #         task = BashOperator(task_id=f'task_{i}', bash_command='sleep 4')
    
    # Sequential implementation
    with TaskGroup('task_execution') as task_execution:
        task_1 = BashOperator(task_id='task_1', bash_command='sleep 3')     
        task_2 = BashOperator(task_id='task_2', bash_command='sleep 3')
        task_3 = BashOperator(task_id='task_3', bash_command='sleep 3')  
        
        init_variables >> task_1 >> task_2 >> task_3
        
        
    start >> init_variables >> task_execution >> processing >> end
