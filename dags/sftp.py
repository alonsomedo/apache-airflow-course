from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.sftp.operators.sftp import SFTPOperator

from datetime import datetime, timedelta

# All the arguments available for default_args are the ones that belongs to the BaseOperator.
default_args = {
    "owner": "DSRP Class",
    "start_date": datetime(2023,5,22),
    "depends_on_past": True
}

with DAG(dag_id="sftp",
          schedule='@daily',
          catchup=False,
          default_args=default_args
          ) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    put_file = SFTPOperator(
        task_id="test_sftp",
        ssh_conn_id="sftp_con",
        local_filepath="/opt/airflow/dags/emails.txt", #/opt/airflow/data/emails.txt
        remote_filepath="/upload/emails.txt",
        operation="put",
        create_intermediate_dirs=True
    )
    start >> put_file >>  end
