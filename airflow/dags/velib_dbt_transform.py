from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'velib_dbt_transform',
    default_args=default_args,
    description='DBT transformations - Staging → Marts → KPI',
    schedule='15 * * * *',  # 15min après chaque heure
    start_date=datetime(2026, 1, 1, 1, 00),
    catchup=False,
    is_paused_upon_creation=True,
    tags=['dbt', 'transformation', 'analytics'],
    max_active_runs=1,  # Évite les doublons
) as dag:

    # Installation des dépendances DBT
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command='bash /opt/airflow/dbt/run_scripts/run_dbt.sh deps',
    )

    # Staging : Nettoyage et normalisation
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command='bash /opt/airflow/dbt/run_scripts/run_dbt.sh run --select staging',
    )

    # # Marts : Dimensions et faits
    # dbt_run_marts = BashOperator(
    #     task_id='dbt_run_marts',
    #     bash_command='bash /opt/airflow/dbt/run_scripts/run_dbt.sh run --select marts',
    # )

    # # KPI : Indicateurs métier
    # dbt_run_kpi = BashOperator(
    #     task_id='dbt_run_kpi',
    #     bash_command='bash /opt/airflow/dbt/run_scripts/run_dbt.sh run --select kpi',
    # )

    # Tests de qualité
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='bash /opt/airflow/dbt/run_scripts/run_dbt.sh test',
    )

    # Génération de la documentation (optionnel mais valorisant)
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command='bash /opt/airflow/dbt/run_scripts/run_dbt.sh docs generate',
    )

    # Pipeline
    dbt_deps >> dbt_run_staging >> dbt_test >> dbt_docs_generate