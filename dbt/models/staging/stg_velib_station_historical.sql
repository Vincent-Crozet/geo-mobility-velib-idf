-- dbt/models/staging/stg_velib_station_historical.sql


SELECT s.id,
    s.station_id,
    s.station_code,
    s.name,
    s.capacity,
    ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326) AS "geometry",
    s.rental_methods,
    s.station_opening_hours,
    s.valid_from,
    s.valid_to,
    s.current_validity,
    s.last_updated_at,
    s.extracted_at,
    s.last_extracted_at
FROM {{ source('velib', 'stations_scd') }} AS s