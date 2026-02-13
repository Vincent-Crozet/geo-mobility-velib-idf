-- dbt/models/staging/stg_geo_communes_idf.sql

SELECT c.insee::int,
    c.nomcom::text,
    c.numdep::int,
    ST_Transform(c.geometry,4326) AS geometry
FROM {{ source('geo_asset', 'communes_idf') }} AS c
