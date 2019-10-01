from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

# from airflow.operators import Neo4jOperator
# from plugins.CustomPlugins import Neo4jOperator
from CustomPlugins import Neo4jOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 6, 1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=0)}

dag = DAG('neo4j-1',
          default_args=default_args,
          description='testing generic cypher',
          # schedule_interval=timedelta(days=1)
          schedule_interval='@hourly',
          catchup=False)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

cypher_1 = Neo4jOperator(task_id='node_count',
                         cql="MATCH (n) RETURN count(n)",
                         dag=dag)

t1.set_upstream(cypher_1)
