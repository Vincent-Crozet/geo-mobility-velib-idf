#!/bin/bash
set -e

echo "==============================="
echo " Airflow Entrypoint"
echo "==============================="

# ---------------------------
# Attente que PostgreSQL soit prêt
# ---------------------------
echo "⏳ Waiting for PostgreSQL..."
until pg_isready -h "${POSTGRES_HOST:-postgres-airflow}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-airflow}"; do
    echo "PostgreSQL is unavailable - sleeping"
    sleep 2
done

# ---------------------------
# Initialisation DB et DAGs
# ---------------------------
echo "🗄️ Migrating Airflow DB..."
airflow db migrate

# Optionnel : créer un utilisateur admin si besoin
# echo "👤 Creating admin user..."
# airflow users create \
#     --username "${AIRFLOW_ADMIN_USERNAME}" \
#     --password "${AIRFLOW_ADMIN_PASSWORD}" \
#     --firstname "${AIRFLOW_ADMIN_FIRSTNAME}" \
#     --lastname "${AIRFLOW_ADMIN_LASTNAME}" \
#     --email "${AIRFLOW_ADMIN_EMAIL}" \
#     --role Admin || true

# ---------------------------
# Lancer Airflow en standalone (webserver + scheduler local executor)
# ---------------------------
echo "🚀 Starting Airflow..."
exec airflow standalone
