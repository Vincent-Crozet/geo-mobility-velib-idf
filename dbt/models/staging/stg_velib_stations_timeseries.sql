{{ config(
    materialized='table'  -- tu peux mettre 'view' si tu ne veux pas stocker physiquement
) }}

with source as (
    select *
    from {{ source('velib', 'stations_raw') }}
)

select
    station_id::int,
    name,
    capacity::int,
    ST_SetSRID(
        ST_MakePoint(lon::float, lat::float),
        4326
    ) as geom,
    last_updated::timestamp
from source