#!/bin/bash
set -e

# Attendre que Postgres soit prêt
until pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER"; do
  echo "Waiting for PostGIS Velib database..."
  sleep 2
done

# Créer les tables staging si elles n'existent pas
psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'EOSQL'
CREATE TABLE IF NOT EXISTS staging_stations (
    station_id INT PRIMARY KEY,
    station_code TEXT,
    name TEXT,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    capacity INT,
    rental_methods TEXT[],
    station_opening_hours JSONB,
    last_updated_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ,
    geom GEOMETRY(Point, 4326)
);

CREATE TABLE IF NOT EXISTS staging_station_status (
    station_id INT,
    num_bikes_available INT,
    ebikes_available INT,
    mechanical_available INT,
    num_docks_available INT,
    is_installed BOOLEAN,
    is_renting BOOLEAN,
    is_returning BOOLEAN,
    last_reported_at TIMESTAMPTZ,
    retrieved_at TIMESTAMPTZ,
    PRIMARY KEY(station_id, last_reported_at)
);
EOSQL

echo "Velib staging tables initialized."
