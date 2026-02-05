-- dbt/models/staging/stg_communes_idf.sql

SELECT c.insee::int,
    c.nomcom::int,
    c.numdep::int,
    c.geometry
FROM {{ source('geo_asset', 'communes_idf') }} AS c
