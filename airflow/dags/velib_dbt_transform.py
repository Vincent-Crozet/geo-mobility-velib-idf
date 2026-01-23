from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

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
        set -euxo pipefail  # exit on error, show commands, undefined vars fail, pipeline fails on first error

        # Afficher contexte de l'environnement
        echo "==== ENVIRONNEMENT ===="
        whoami
        id
        pwd
        env | sort

        # Vérifier les fichiers dbt
        echo "==== CONTENU /opt/airflow ===="
        ls -la /opt/airflow
        echo "==== CONTENU /opt/airflow/dbt ===="
        ls -la /opt/airflow/dbt

        # Vérifier dbt
        which dbt
        dbt --version

        # Variables dbt
        export DBT_PROFILES_DIR=/opt/airflow/dbt
        export DBT_PROJECT_DIR=/opt/airflow/dbt
        export POSTGRES_HOST=${POSTGRES_HOST}
        export POSTGRES_USER=${POSTGRES_USER}
        export POSTGRES_PASSWORD=${POSTGRES_PASSWORD}
        export POSTGRES_PORT=${POSTGRES_PORT}
        export POSTGRES_DB=${POSTGRES_DB}
        # Aller dans le projet dbt
        cd /opt/airflow/dbt

        echo "=========VARIABLES DENVIRONEMENT ============"
        echo "POSTGRES_HOST"
        echo "POSTGRES_USER"
        echo "POSTGRES_PASSWORD"
        echo "POSTGRES_PORT"
        echo "POSTGRES_DB"
        

        echo "==== dbt deps ===="
        dbt deps --debug

        echo "==== dbt ls ===="
        dbt ls --debug

        echo "==== dbt run ===="
        # dbt run --select staging --debug
        """
    )



    # dbt_deps = BashOperator(
    #     task_id="dbt_deps",
    #     bash_command=f"cd {DBT_DIR} && dbt deps",
    # )

    # dbt_seed = BashOperator(
    #     task_id="dbt_seed",
    #     bash_command=f"cd {DBT_DIR} && dbt seed",
    # )

    # dbt_run_staging = BashOperator(
    #     task_id="dbt_run_staging",
    #     bash_command=(
    #         f"cd {DBT_DIR} && "
    #         "dbt run --select staging"
    #     ),
    # )

    # # dbt_run_marts = BashOperator(
    # #     task_id="dbt_run_marts",
    # #     bash_command=(
    # #         f"cd {DBT_DIR} && "
    # #         "dbt run --select marts"
    # #     ),
    # # )

    # dbt_test = BashOperator(
    #     task_id="dbt_test",
    #     bash_command=f"cd {DBT_DIR} && dbt test",
    # )

    # dbt_docs_generate = BashOperator(
    #     task_id="dbt_docs_generate",
    #     bash_command=f"cd {DBT_DIR} && dbt docs generate",
    # )

    # # Dépendances
    # dbt_debug>>dbt_deps >> dbt_seed >> dbt_run_staging >> dbt_test >> dbt_docs_generate
    # # dbt_deps >> dbt_seed >> dbt_run_staging >> dbt_run_marts >> dbt_test >> dbt_docs_generate
