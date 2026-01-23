from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    "dbt_run_station_analysis",
    start_date=datetime(2026,1,22),
    schedule="@daily",  # <-- ici
    catchup=False,
    is_paused_upon_creation=True,
    description="DAG to run dbt model and create tables",
) as dag:

    dbt_run = BashOperator(
        task_id="dbt_run_stations_latest",
        bash_command="""
        docker-compose run --rm dbt dbt run --select stations_latest
        """
    )