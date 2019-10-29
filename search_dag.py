from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

# https://stackoverflow.com/questions/53663534/for-apache-airflow-how-can-i-pass-the-parameters-when-manually-trigger-dag-via

dag = DAG('init-dag-1',
          default_args=default_args,
          description='sample-dag',
          # schedule_interval=timedelta(days=1)
          schedule_interval='@hourly',
          catchup=False
          )

bash_task = BashOperator(task_id="bash_task",
                         bash_command="sh {{var.value.tmpl_search_path}}",
                         dag=dag)
