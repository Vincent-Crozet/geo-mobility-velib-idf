-- dbt/models/staging/stg_pop_communes_idf.sql

SELECT c.nomcom::int,
    c.geometry
FROM {{ source('geo_asset', 'pop_commune_idf') }} AS c
