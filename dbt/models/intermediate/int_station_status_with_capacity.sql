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
),

-- station status with capacity
status_with_own_capacity AS (
    SELECT
        sr.*,
        scd.capacity
    FROM station_status_raw sr
    LEFT JOIN station_scd scd
        ON  sr.station_id   = scd.station_id
        AND sr.extracted_at >= scd.valid_from
        AND (scd.valid_to IS NULL OR sr.extracted_at < scd.valid_to)
),

-- Now evaluate number of stations and total capacity within 500 m
neighbor_capacity AS (
    SELECT
        sr.station_id,
        sr.extracted_at,
        COALESCE(SUM(scd_n.capacity), 0) AS capacity_500m,
        COUNT(nb.neighbor_station_id)    AS neighbor_station_count_500m
    FROM station_status_raw sr
    JOIN {{ ref('int_station_neighbors_500m') }} nb
        ON nb.station_id = sr.station_id
    -- Capacité de la voisine valide au moment du snapshot (respect SCD2)
    LEFT JOIN station_scd scd_n
        ON  scd_n.station_id   = nb.neighbor_station_id
        AND sr.extracted_at   >= scd_n.valid_from
        AND (scd_n.valid_to IS NULL OR sr.extracted_at < scd_n.valid_to)
    GROUP BY sr.station_id, sr.extracted_at
)

SELECT
    sc.*,

    -- Capacité des stations voisines dans 500m (SCD2-aware)
    nc.capacity_500m,
    nc.neighbor_station_count_500m,

    -- Capacité totale du périmètre 500m (station propre + voisines)
    sc.capacity + COALESCE(nc.capacity_500m, 0) AS total_capacity_500m

FROM status_with_own_capacity sc
LEFT JOIN neighbor_capacity nc
    ON  nc.station_id   = sc.station_id
    AND nc.extracted_at = sc.extracted_at