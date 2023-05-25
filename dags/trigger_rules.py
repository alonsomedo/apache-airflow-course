from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule


from datetime import datetime

default_args = {
    "owner": "DSRP-Class"
}

with DAG(dag_id="trigger_rules",
          start_date=datetime(2023,5,16),
          schedule='@daily',
          catchup=False,
          default_args=default_args
          ) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    init_variables = BashOperator(task_id='init_variables', bash_command='sleep 3')
    processing = BashOperator(task_id='processing', bash_command='sleep 3', trigger_rule=TriggerRule.ONE_SUCCESS)
    
    # Sequential implementation
    with TaskGroup('models_execution') as models_execution:
        task_1 = BashOperator(task_id='model_A', bash_command='sleep 10')     
        task_2 = BashOperator(task_id='model_B', bash_command='exit 1')
        task_3 = BashOperator(task_id='model_C', bash_command='exit 1')  
        
    start >> init_variables >> models_execution >> processing >> end