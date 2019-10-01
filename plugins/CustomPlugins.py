from airflow.plugins_manager import AirflowPlugin

import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime
from airflow.operators.sensors import BaseSensorOperator

# todo move to different location
from neo4j import GraphDatabase

log = logging.getLogger(__name__)


class AirflowOperatorName(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            param1=None,
            param2=None,
            *args, **kwargs):
        super(AirflowOperatorName, self).__init__(*args, **kwargs)
        self.param1 = param1
        self.param2 = param2

    def execute(self, context):
        print(self.param1)
        print(context)


class Neo4jOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            cql,
            cql_prams=None,
            # uri="bolt://127.0.0.1:7687",
            # this is the ip address of the neo4j docker container
            uri="bolt://172.19.0.3:7687",
            # uri="bolt://localhost:7687",
            user='neo4j',
            pw='password',
            *args, **kwargs):
        super(Neo4jOperator, self).__init__(*args, **kwargs)
        self.cql_prams = cql_prams
        self.pw = pw
        self.user = user
        self.uri = uri
        self.cql = cql

    def execute(self, context):
        print("executing neo4j operator")
        print("host", self.uri)
        print("cql", self.cql)
        driver = GraphDatabase.driver(self.uri, auth=(self.user, self.pw))
        with driver.session() as tx:
            result = tx.run(self.cql, parameters=self.cql_prams)
            print(result.single())


class LogSensorXcomOperator(BaseOperator):

    @apply_defaults
    def __init__(
            self,
            *args, **kwargs):
        super(LogSensorXcomOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Log Xcom Var Operator")
        task_instance = context['task_instance']
        sensors_minute = task_instance.xcom_pull('my_sensor_task', key='sensors_minute')
        log.info('Valid minute as determined by sensor: %s', sensors_minute)


class MyFirstSensor(BaseSensorOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(MyFirstSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        current_minute = datetime.now().minute
        if current_minute % 3 != 0:
            log.info("Current minute (%s) not is divisible by 3, sensor will retry.", current_minute)
            print("SENSOR RUNNING")
            log.warning("Current minute (%s) not is divisible by 3, sensor will retry.", current_minute)
            task_instance = context['task_instance']
            task_instance.xcom_push('sensors_minute', current_minute)
            return False

        log.info("Current minute (%s) is divisible by 3, sensor finishing.", current_minute)
        return True


class CustomPlugin(AirflowPlugin):
    name = "airflow_custom_plugins"
    operators = [AirflowOperatorName, LogSensorXcomOperator, Neo4jOperator]
    sensors = [MyFirstSensor]
