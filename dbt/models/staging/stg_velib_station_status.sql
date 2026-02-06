-- dbt/models/staging/stg_velib_station_status.sql

{{ config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='fail',
    incremental_strategy='delete+insert'
) }}


SELECT 
    s.id,
    s.station_id,
    s.station_code,
    s.num_bikes_available,
    s.mechanical_available,
    s.ebikes_available,
    s.num_docks_available,
    s.is_installed,
    s.is_renting,
    s.is_returning,
    s.rental_methods,
    s.last_reported_at,
    s.last_updated_at,
    s.extracted_at
FROM {{ source('velib', 'station_status') }} AS s

{% if is_incremental() %}
    WHERE extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
{% endif %}