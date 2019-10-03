from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from airflow.models import Variable

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

dag = DAG('neo4j-2-vars',
          default_args=default_args,
          description='testing generic cypher',
          # schedule_interval=timedelta(days=1)
          schedule_interval='@hourly',
          catchup=False)

t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

uri = Variable.get("neo4j_uri")
pw = Variable.get('neo4j_pw'),

cypher_set_up = Neo4jOperator(task_id='create_count',
                              cql="CREATE (c:Client) RETURN c ",
                              uri=uri,
                              pw=pw,
                              dag=dag)

cypher_1 = Neo4jOperator(task_id='node_count',
                         cql="MATCH (n) RETURN count(n)",
                         uri=uri,
                         pw=pw,
                         dag=dag)

cypher_1.set_upstream(cypher_set_up)
t1.set_upstream(cypher_1)
