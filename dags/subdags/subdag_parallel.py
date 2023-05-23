from airflow import DAG
from airflow.operators.bash import BashOperator


def subdag_parallel(parent_dag_id, child_dag_id, default_args):
    
    with DAG(
        dag_id=f'{parent_dag_id}.{child_dag_id}',
        default_args=default_args
    ) as dag:
        for i in range(1,31):
            task = BashOperator(task_id=f'task_{i}', bash_command='sleep 6')
        
        return dag