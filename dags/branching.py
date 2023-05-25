from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task

from datetime import datetime, timedelta
from random import uniform

# All the arguments available for default_args are the ones that belongs to the BaseOperator.
default_args = {
    "owner": "DSRP Class",
    "start_date": datetime(2023,5,22),
    "depends_on_past": True
}

with DAG(dag_id="branching",
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
            accuracy = uniform(1, 10)
            print(f"Accuracy of the model: {accuracy}")
            ti.xcom_push(key="model_accuracy", value=accuracy)
        
        
        training_ml_model_A = PythonOperator(task_id='training_ml_model_A', 
                                             python_callable=training_model)     
        
        training_ml_model_B = PythonOperator(task_id='training_ml_model_B', 
                                             python_callable=training_model)   
        
        training_ml_model_C = PythonOperator(task_id='training_ml_model_C', 
                                             python_callable=training_model)   

        #training_ml_model_A >> training_ml_model_B >> training_ml_model_C
    
    def choose_best_model(**context):
        print(context["ds"])
        values = context["ti"].xcom_pull(key="model_accuracy", task_ids=['processing_models.training_ml_model_A',
                                                                         'processing_models.training_ml_model_B',
                                                                         'processing_models.training_ml_model_C'
                                                                        ])
        best_value = max(values)
        print("El mejor valor es: {best_value}".format(best_value=best_value))
        
        if best_value > 8:
            return 'best_performance'
        elif best_value > 5:
            return 'accurate'
        return 'inaccurate'
    
    choose_best_model = BranchPythonOperator(
        task_id="choose_best_model",
        python_callable=choose_best_model
    )
    
    
    accurate = BashOperator(task_id='accurate', bash_command='sleep 3')
    inaccurate = BashOperator(task_id='inaccurate', bash_command='sleep 3')
    best_performance = BashOperator(task_id='best_performance', bash_command='sleep 3')
    
    load_results_to_bigquery = BashOperator(task_id='load_results_to_bigquery', bash_command='sleep 3', trigger_rule="one_success")
        
    start >> downloading_data >> processing_models >> choose_best_model >> [accurate, inaccurate, best_performance] >> load_results_to_bigquery >> end
