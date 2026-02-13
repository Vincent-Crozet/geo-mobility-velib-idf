-- dbt/models/intermediate/int_station_status_with_capacity.sql

-- historical station status
WITH station_status_raw AS (
    SELECT
        s.id,
        s.station_id,
        s.station_code,
        s.num_bikes_available,
        s.mechanical_available,
        s.ebikes_available,
        s.num_docks_available,
        s.is_installed,
        s.is_renting,
        s.is_returning,
        s.rental_methods,
        s.last_reported_at,
        s.last_updated_at,
        s.extracted_at
    FROM {{ ref('stg_velib_station_status') }} AS s
),
-- station scd for capacity
station_scd AS (
    SELECT
        station_id,
        capacity,
        valid_from,
        valid_to
    FROM {{ source('velib', 'stations_scd') }}
)

-- station status with capacity
SELECT
    sr.*,
    scd.capacity
FROM station_status_raw sr
LEFT JOIN station_scd scd
ON  sr.station_id   = scd.station_id
AND sr.extracted_at >= scd.valid_from
AND (scd.valid_to IS NULL OR sr.extracted_at < scd.valid_to)
