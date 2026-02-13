-- dbt/models/staging/stg_geo_pop_idf.sql
{{
  config(
    materialized='table',
    indexes=[{'columns': ['geometry'], 'type': 'gist'}]
  )
}}

SELECT population,
    ST_Transform(geometry,2154) AS geometry
FROM {{ source('geo_asset', 'pop_pointwise_idf') }}
