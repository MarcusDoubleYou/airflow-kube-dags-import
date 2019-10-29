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

dag = DAG('transfer_from_postgres_to_neo4j',
          default_args=default_args,
          description='loading csv data from postgres to neo4j',
          # schedule_interval=timedelta(days=1)
          schedule_interval='@hourly',
          catchup=False)

def export_large_object():
    pg_hook = PostgresHook(postgres_conn_id='rates')

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

cypher_1 = Neo4jOperator(task_id='node_count',
                         cql="MATCH (n) RETURN count(n)",
                         uri="bolt://neo-single-neo4j-core-0.neo-single-neo4j.default.svc.cluster.local:7687",
                         pw="",
                         dag=dag)

t1.set_upstream(cypher_1)
