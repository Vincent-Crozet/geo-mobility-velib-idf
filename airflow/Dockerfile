FROM apache/airflow:3.1.0-python3.12

# passer root pour installer dbt
USER airflow

# installer dbt pour Postgres
RUN pip install --no-cache-dir dbt-postgres==1.8.2

