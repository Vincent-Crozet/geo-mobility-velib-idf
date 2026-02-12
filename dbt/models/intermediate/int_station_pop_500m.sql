-- dbt/models/marts/core/int_station_pop_500m.sql

/*
For each station record (SCD2) compute the estimated population within 500m
*/


WITH stations_lambert AS (
    -- Using all station SCD, project to Lambert EPSG
    SELECT
        station_id,
        valid_from,
        ST_Transform(geometry, 2154) AS geometry_2154
    FROM {{ ref('stg_velib_station_historical') }}
),

pop_points AS (
    SELECT
        fid,
        population,
        geometry  -- déjà en EPSG:2154 (cf. source src_add_assets_pop_pointwise.yml)
    FROM {{ ref('stg_geo_pop_idf') }}
)

SELECT
    s.station_id,
    s.valid_from,
    COALESCE(SUM(p.population), 0) AS population_500m,
    COUNT(p.fid)                   AS pop_point_count_500m
FROM stations_lambert s
LEFT JOIN pop_points p
    ON ST_DWithin(s.geometry_2154, p.geometry, 500)
GROUP BY s.station_id, s.valid_from