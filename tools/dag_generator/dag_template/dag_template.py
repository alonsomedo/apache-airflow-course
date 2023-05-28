# TODO: Remove the directories, code, libraries and all the things are not necessary for your dag. 
# Checklist:
# - Remove imports you don't need.
# - Remove folders you don't need.
# - Remove unnecessary operators. (e.g. HelloWorld python operator)
# - Create technical documentation in confluence.
# - Create optional user-guide documentation.
# - Be sure to update the dag_config yaml file with schedules for the environments you need.
# - Be sure to update the pipeline_config yaml file if needed.
# - Be sure to update the README.md file.
# - Don't forget to remove this commented block at the end.

import logging
import os
from datetime import date, datetime, timedelta

from utils.common import get_config
from utils.common import get_md
from utils.common import get_sql


from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.trigger_rule import TriggerRule

# Importing python functions
from dag_template.functions.hello_world import print_hello_world_and_name

# Declare configuration variables
dag_file_name = os.path.basename(__file__).split('.')[0]
dag_config = get_config(dag_file_name = dag_file_name, config_filename = 'dag_config.yaml')
pipeline_config = get_config(dag_file_name = dag_file_name, config_filename = 'pipeline_config.yaml')

env=os.getenv('ENVIRONMENT')
default_arguments = dag_config['default_args'][env]

# Getting variables of pipeline configs
endpoint = pipeline_config['endpoint']

#Airflow docstring
doc_md = get_md(dag_file_name, 'README.md')

#logging.basicConfig(level=logging.INFO)

#Declare DAG insrtance and DAG operators
with DAG(dag_file_name,
          description='Very short description (optional)',
          start_date=datetime.strptime(dag_config['dag_args'][env]["start_date"], '%Y-%m-%d'),
          max_active_runs=dag_config['dag_args'][env]["max_active_runs"],
          catchup=dag_config['dag_args'][env]["catchup"],
          #max_active_tasks=dag_config['dag_args'][env]["max_active_tasks"], # This is an optional argument, if you don't need it, please remove it.
          tags = [],
          schedule_interval=dag_config['schedule'][env],
          default_args=default_arguments,
          dagrun_timeout=timedelta(hours=dag_config["dag_run_timeout"]),
          doc_md=doc_md,
          ) as dag:
    
    ##### DECLARING THE OPERATORS ######
    
    # Declare Dummy Operators
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
       

    # Custom python operator
    hello_world = PythonOperator(
        task_id='hello_world',
        provide_context=True,
        python_callable=print_hello_world_and_name,
        params={
            'key': 'value', 
        },
        op_kwargs={
            'name': 'your_name'
        }
    )
     
    start >> hello_world >> end