# airflow/dags/test_dag.py
from airflow.sdk import dag
from airflow.sdk import task
from datetime import datetime, timedelta

@dag(dag_id="test_dag", start_date=datetime(2026, 1, 1), schedule=None, catchup=False)
def test_dag_func():
    @task
    def hello():
        print("hello")
    hello()

dag = test_dag_func()