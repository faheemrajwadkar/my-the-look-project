# include/utils/alerts.py
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

def slack_failure_callback(context):
    slack_msg = f"""
            :red_circle: Task Failed.
            *Task*: {context.get('task_instance').task_id}  
            *Dag*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            *Log URL*: {context.get('task_instance').log_url}
            """
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        slack_webhook_conn_id='slack_conn',
        message=slack_msg
    )
    return failed_alert.execute(context=context)