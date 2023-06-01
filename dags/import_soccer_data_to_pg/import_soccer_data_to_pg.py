import logging
import os
from datetime import date, datetime, timedelta

from utils.common import get_config
from utils.common import get_md
from utils.common import get_sql


from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

# Importing python functions
from import_soccer_data_to_pg.functions.get_soccer_team_players import get_soccer_team_players
from import_soccer_data_to_pg.functions.load_soccer_players_into_tmp import load_soccer_players_into_tmp

from airflow.utils.task_group import TaskGroup

# Declare configuration variables
dag_file_name = os.path.basename(__file__).split('.')[0]
dag_config = get_config(dag_file_name = dag_file_name, config_filename = 'dag_config.yaml')
pipeline_config = get_config(dag_file_name = dag_file_name, config_filename = 'pipeline_config.yaml')

env=os.getenv('ENVIRONMENT')
default_arguments = dag_config['default_args'][env]

# Getting variables of pipeline configs
endpoint = pipeline_config['endpoint']
soccer_teams_ids = pipeline_config['soccer_teams_ids']
soccer_positions = pipeline_config['soccer_positions']

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
          tags = dag_config['tags'],
          schedule_interval=dag_config['schedule'][env],
          default_args=default_arguments,
          dagrun_timeout=timedelta(hours=dag_config["dag_run_timeout"]),
          doc_md=doc_md,
          ) as dag:
    
    ##### DECLARING THE OPERATORS ######
    
    # Declare Dummy Operators
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    #wait = DummyOperator(task_id='wait')

    create_tmp_players_table = PostgresOperator(
        task_id='create_tmp_players_table',
        sql=get_sql(dag_file_name, 'create_tmp_players_table.sql')
    )
    
    drop_tmp_players_table = PostgresOperator(
        task_id='drop_tmp_players_table',
        sql=get_sql(dag_file_name, 'drop_tmp_players_table.sql')
    )
    
    create_players_table = PostgresOperator(
        task_id='create_players_table',
        sql=get_sql(dag_file_name, 'create_players_table.sql'),
        params={"table_name": "bronze.soccer_players"}
    )
    
    merge_tmp_into_players_table = PostgresOperator(
        task_id='merge_tmp_into_players_table',
        sql=get_sql(dag_file_name, 'merge_tmp_into_players_table.sql'),
        params={"table_name": "bronze.soccer_players"}
    )
    
    with TaskGroup('soccer_squads') as soccer_squads:
        for team_name, team_id in soccer_teams_ids.items():
            get_soccer_players = PythonOperator(
                task_id=f'get_{team_name}_players',
                python_callable=get_soccer_team_players,
                op_args=[team_id, endpoint]
            )
            
            load_players_into_tmp = PythonOperator(
                task_id=f'load_{team_name}_players_into_tmp',
                python_callable=load_soccer_players_into_tmp,
                op_args=[team_id],
                templates_dict={
                    "tmp_table": 'bronze.tmp_players_table_{{ ds_nodash }}'
                }
            )
            get_soccer_players >> load_players_into_tmp
            
    with TaskGroup('players_by_position') as players_by_position:
        for position in soccer_positions:
            create_position_table = PostgresOperator(
                task_id=f'create_{position}_table',
                sql=get_sql(dag_file_name, 'create_players_table.sql'),
                params={"table_name": f"bronze.soccer_players_{position}"}
            )

            insert_players_by_position = PostgresOperator(
                task_id=f'insert_{position}_players',
                sql=get_sql(dag_file_name, 'insert_by_position.sql'),
                params={
                        "table_name": f"bronze.soccer_players_{position}",
                        "position": position
                        }
            )
            create_position_table >> insert_players_by_position
       
    slack_notification = SlackWebhookOperator(
        task_id='slack_notification',
        slack_webhook_conn_id = 'slack_conn',
        message='El proceso de importar squads termino satisfactoriamente. :large_green_circle: :red_circle:',
        channel='#airflow-alerts',
        icon_emoji=':large_green_circle:'
    )
    
    
    start >> [create_tmp_players_table, create_players_table]  >> soccer_squads  >> merge_tmp_into_players_table 
    merge_tmp_into_players_table >> players_by_position >> drop_tmp_players_table >> slack_notification >> end
    
    
    