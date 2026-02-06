SELECT s.id,
    s.station_id,
    s.station_code,
    s.name,
    s.capacity,
    ST_SetSRID(ST_MakePoint(s.lon, s.lat), 4326) AS "geometry",
    s.rental_methods,
    s.station_opening_hours,
    s.valid_from,
    s.last_updated_at,
    s.extracted_at,
    s.last_extracted_at,
    current_validity
FROM {{ source('velib', 'stations_scd') }} AS s
WHERE current_validity='TRUE'