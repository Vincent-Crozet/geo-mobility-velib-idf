#airflow/dags/velib_dbt_transformation.py
from datetime import datetime

#Airflow
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

DBT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_dag",
    start_date=datetime(2026, 1, 23),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
) as dag:

    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command="""

        # Aller dans le projet dbt
        cd /opt/airflow/dbt

        dbt run --select staging --debug
        """
    )
