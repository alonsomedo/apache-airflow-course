from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.decorators import task

from datetime import datetime, timedelta
from random import uniform

# All the arguments available for default_args are the ones that belongs to the BaseOperator.
default_args = {
    "owner": "DSRP Class",
    "start_date": datetime(2023,5,22),
    "depends_on_past": True,
    "retries": 3,
    "retry_delay": timedelta(seconds=10)
}

with DAG(dag_id="xcoms",
          schedule='@daily',
          catchup=False,
          default_args=default_args
          ) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    downloading_data = BashOperator(task_id='downloading_data', bash_command='sleep 3')
    
   
    # Sequential implementation
    with TaskGroup('processing_models') as processing_models:
        
        def training_model(ti):
            accuracy = uniform(0.1, 10.0)
            print(f"Accuracy of the model: {accuracy}")
            ti.xcom_push(key="model_accuracy", value=accuracy)
        
        
        training_ml_model_A = PythonOperator(task_id='training_ml_model_A', 
                                             python_callable=training_model)     
        
        training_ml_model_B = PythonOperator(task_id='training_ml_model_B', 
                                             python_callable=training_model)   
        
        training_ml_model_C = PythonOperator(task_id='training_ml_model_C', 
                                             python_callable=training_model)   
    
    
    @task(task_id="choose_best_model")
    def choose_best_model(**context):
        print(context["ds"])
        values = context["ti"].xcom_pull(key="model_accuracy", task_ids=['processing_models.training_ml_model_A',
                                                                         'processing_models.training_ml_model_B',
                                                                         'processing_models.training_ml_model_C'
                                                                        ])
        best_value = max(values)
        print(f"El mejor valor es: {best_value}")
        
        
    start >> downloading_data >> processing_models >> choose_best_model() >> end
