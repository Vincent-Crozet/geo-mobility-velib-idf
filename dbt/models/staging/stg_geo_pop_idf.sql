-- dbt/models/staging/stg_geo_pop_idf.sql

SELECT fid,
    population,
    geometry
FROM {{ source('geo_asset', 'pop_pointwise_idf') }}
