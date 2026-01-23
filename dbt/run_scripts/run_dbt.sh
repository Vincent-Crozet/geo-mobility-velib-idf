#!/bin/bash
set -euo pipefail
set -x

echo "================= DEBUG AIRFLOW → DOCKER ================="

echo "▶ User info"
whoami || true
id || true

echo "▶ Groups (raw)"
cat /etc/group | grep docker || true

echo "▶ Docker binary"
command -v docker || true
docker --version || true

echo "▶ Docker compose plugin"
docker compose version || true

echo "▶ Docker socket info"
ls -l /var/run/docker.sock || true
stat /var/run/docker.sock || true

echo "▶ Effective permissions test"
touch /var/run/docker.sock 2>/tmp/docker_sock_test.err || true
cat /tmp/docker_sock_test.err || true

echo "▶ Docker access test"
docker info || true
docker ps || true
docker network ls || true

echo "================= END DEBUG ================="

cd /opt/airflow/dbt

echo "▶ Running DBT via docker compose"
docker compose -f docker-compose.dbt.yml run --rm dbt dbt "$@"
