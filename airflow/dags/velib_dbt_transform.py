#airflow/dags/velib_dbt_transformation.py
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
    dbt_run >> dbt_test >> dbt_docs_generate