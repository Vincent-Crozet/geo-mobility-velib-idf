#airflow/dags/velib_dbt_transformation.py
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta


DBT_DIR = "/opt/airflow/dbt"

# ✅ AJOUTER CETTE FONCTION
def get_latest_ingestion_time(dt):
    """Cherche l'ingestion la plus récente terminée"""
    minutes = (dt.minute // 5) * 5
    target_time = dt.replace(minute=minutes, second=0, microsecond=0)
    
    # Si on tombe pile sur un cycle, recule de 5 min
    if dt.minute % 5 == 0:
        target_time = target_time - timedelta(minutes=5)
    
    return target_time

with DAG(
    dag_id="dbt_dag",
    start_date=datetime(2026, 1, 23),
    schedule='3,33 * * * *',  # ✅ CHANGÉ : 03 et 33
    is_paused_upon_creation=False,
    max_active_runs=1,
    catchup=False,
) as dag:

    wait_for_ingestion = ExternalTaskSensor(
        task_id='wait_for_ingestion_complete',
        external_dag_id='velib_extract_ingestion_dag',
        external_task_id='load_stations_status',
        execution_date_fn=get_latest_ingestion_time,  # ✅ PAS execution_delta
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode='reschedule',  # ✅ CHANGÉ
        poke_interval=15,   # ✅ CHANGÉ
        timeout=180,        # ✅ CHANGÉ
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_DIR}
        echo ""
        echo "╔═══════════════════════════════════════════════════════════════╗"
        echo "║                          DBT RUN                              ║"
        echo "╚═══════════════════════════════════════════════════════════════╝"
        echo ""
        dbt run
        echo ""
        echo "╔═══════════════════════════════════════════════════════════════╗"
        echo "║                   DBT RUN - COMPLETED                         ║"
        echo "╚═══════════════════════════════════════════════════════════════╝"
        echo ""
        """
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"""
        cd {DBT_DIR}
        echo ""
        echo "╔═══════════════════════════════════════════════════════════════╗"
        echo "║                   DBT TEST - VALIDATION                       ║"
        echo "╚═══════════════════════════════════════════════════════════════╝"
        echo ""
        dbt test
        echo ""
        echo "╔═══════════════════════════════════════════════════════════════╗"
        echo "║                   DBT TEST - COMPLETED                        ║"
        echo "╚═══════════════════════════════════════════════════════════════╝"
        echo ""
        """
    )

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
        cd {DBT_DIR}
        echo ""
        echo "╔═══════════════════════════════════════════════════════════════╗"
        echo "║                   DBT DOCS - GENERATION                       ║"
        echo "╚═══════════════════════════════════════════════════════════════╝"
        echo ""
        dbt docs generate
        echo ""
        echo "╔═══════════════════════════════════════════════════════════════╗"
        echo "║                   DBT DOCS - COMPLETED                        ║"
        echo "╚═══════════════════════════════════════════════════════════════╝"
        echo ""
        """
    )

    # Task order
    wait_for_ingestion>>dbt_run >> dbt_test >> dbt_docs_generate