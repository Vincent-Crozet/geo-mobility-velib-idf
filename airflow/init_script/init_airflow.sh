#!/bin/bash
set -e

echo "⏳ Waiting for PostgreSQL..."
until pg_isready -h postgres-airflow -p 5432 -U airflow; do
  echo "PostgreSQL not ready - sleeping"
  sleep 2
done


echo "🗄️ Migrating DB..."
echo "DB var: $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

echo "User name: $_AIRFLOW_WWW_USER_USERNAME"
airflow db migrate
airflow db check


echo "✅ Init complete, exiting"