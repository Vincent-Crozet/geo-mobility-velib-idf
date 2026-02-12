-- dbt/models/intermediate/int_station_historical_geo_enriched.sql

/*
Geographical enrichment for historical stations.
Joins station geometry with commune boundaries and commune-level population.
*/

SELECT
    s.station_id,
    s.station_code,
    s.name,
    s.capacity,
    s.geometry,
    s.rental_methods,
    s.valid_from,       -- needed to ensure natural key
    s.valid_to,
    s.current_validity,

    -- Commune contenant la station à cette version
    c.nomcom AS commune_name,
    c.insee   AS commune_code,

    -- Population de cette commune
    p.population AS commune_population

FROM {{ ref('stg_velib_station_historical') }} s
LEFT JOIN {{ ref('stg_geo_communes_idf') }} c
    ON ST_Within(s.geometry, c.geometry)
LEFT JOIN {{ ref('stg_geo_pop_communes') }} p
    ON c.nomcom = p.nomcom