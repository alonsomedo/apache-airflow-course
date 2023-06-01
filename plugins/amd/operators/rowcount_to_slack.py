import logging

from airflow.models.baseoperator import BaseOperator

from airflow.exceptions import AirflowException
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RowCountToSlackChannelOperator(BaseOperator):
    
    def __init__(self,
                slack_conn: str = None,
                pg_conn: str = None,
                message: str = '',
                icon_emoji: str = None,
                table_name: str = None,
                table_predicate: str = '',
                *args,
                **kwargs
            ):
        super().__init__(*args, **kwargs)
        #super(RowCountToSlackChannelOperator, self).__init__(*args, **kwargs)

        self.slack_conn = slack_conn
        self.pg_conn = pg_conn
        self.message = message
        self.icon_emoji = icon_emoji
        self.table_name = table_name
        self.table_predicate = table_predicate
        self.channel='#airflow-alerts'
        self.sql=None
        
    def execute(self, context):
        if not isinstance(self.table_name, str):
            raise AirflowException(f"Argument 'table_name' of type {type(self.table_name)} is not a string.")
        if not isinstance(self.table_predicate, str):
            raise AirflowException(f"Argument 'table_predicate' of type {type(self.table_name)} is not a string.")
        
        pg_hook = None
        if self.pg_conn:
            pg_hook = PostgresHook(self.pg_conn)
        else:
            pg_hook = PostgresHook()
        
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        self.sql = f""" SELECT COUNT(1) total_records FROM {self.table_name} {self.table_predicate} """
        cursor.execute(self.sql)
        rows = cursor.fetchall()
        total_records = None
        
        for row in rows:
            logging.info(row)
            total_records = row[0]
        
        slack_alert = SlackWebhookOperator(
            task_id="slack_row_count_alert",
            slack_webhook_conn_id = self.slack_conn,
            message=f':loudspeaker: Se cargaron {total_records} registros en la tabla {self.table_name} satisfactoriamente. {self.message }',
            channel=self.channel,
            icon_emoji=':large_green_circle:'
        )
        
        slack_alert.execute(context=context)
        
        
       