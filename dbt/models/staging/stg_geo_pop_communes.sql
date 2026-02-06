-- dbt/models/staging/stg_geo_pop_communes_idf.sql

SELECT c.nomcom::text,
    c.population_total AS population,
    c.geometry
FROM {{ source('geo_asset', 'pop_commune_idf') }} AS c
