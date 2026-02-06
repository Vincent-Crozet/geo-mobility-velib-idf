-- dbt/models/marts/core/fct_station_availability.sql

{{
  config(
    materialized='incremental',
    unique_key=['station_id', 'extracted_at'],
    on_schema_change='sync_all_columns',
    indexes=[
      {'columns': ['station_id', 'extracted_at'], 'unique': true},
      {'columns': ['extracted_at']},
      {'columns': ['station_id']},
      {'columns': ['day_date']}
    ],
    partition_by={
      'field': 'extracted_at',
      'data_type': 'timestamp',
      'granularity': 'day'
    }
  )
}}

WITH source_data AS (
    SELECT
        -- Identifiants
        station_id,
        station_code,
        
        -- Timestamps
        extracted_at,
        last_reported_at,
        DATE(extracted_at) AS day_date,
        EXTRACT(HOUR FROM extracted_at) AS hour_of_day,
        
        -- Disponibilité vélos
        num_bikes_available,
        mechanical_available,
        ebikes_available,
        num_docks_available,
        
        -- États opérationnels
        is_installed,
        is_renting,
        is_returning,
        
        -- Métadonnées
        rental_methods
        
    FROM {{ ref('stg_velib_station_status') }}
    
    {% if is_incremental() %}
        -- Ingestion incrémentale : uniquement nouvelles données
        WHERE extracted_at > (SELECT MAX(extracted_at) FROM {{ this }})
    {% endif %}
),

enriched_data AS (
    SELECT
        sd.*,
        
        -- Jointure avec dimension stations (pour capacité & géo)
        ds.capacity,
        ds.station_name AS station_name,
        ds.commune_name,
        ds.commune_code,
        ds.population,
        ds.geometry,
        
        -- KPIs calculés
        CASE 
            WHEN ds.capacity > 0 
            THEN sd.num_bikes_available::NUMERIC / ds.capacity * 100
            ELSE 0 
        END AS availability_rate,
        
        CASE 
            WHEN ds.capacity > 0 
            THEN sd.num_docks_available::NUMERIC / ds.capacity * 100
            ELSE 0 
        END AS dock_availability_rate,
        
        -- Flags état critique
        CASE 
            WHEN sd.num_bikes_available < ds.capacity * 0.1 THEN true
            ELSE false
        END AS is_bike_critical,
        
        CASE 
            WHEN sd.num_docks_available < ds.capacity * 0.1 THEN true
            ELSE false
        END AS is_dock_critical,
        
        CASE 
            WHEN sd.num_bikes_available < ds.capacity * 0.1 
              OR sd.num_docks_available < ds.capacity * 0.1 
            THEN true
            ELSE false
        END AS is_critical,
        
        -- Enrichissement temporel
        EXTRACT(DOW FROM sd.extracted_at) AS day_of_week,
        TO_CHAR(sd.extracted_at, 'Day') AS day_name,
        CASE 
            WHEN EXTRACT(DOW FROM sd.extracted_at) IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END AS day_type,
        
        CASE 
            WHEN EXTRACT(HOUR FROM sd.extracted_at) BETWEEN 7 AND 9 THEN 'Morning Rush'
            WHEN EXTRACT(HOUR FROM sd.extracted_at) BETWEEN 17 AND 19 THEN 'Evening Rush'
            WHEN EXTRACT(HOUR FROM sd.extracted_at) BETWEEN 22 AND 6 THEN 'Night'
            ELSE 'Off-Peak'
        END AS time_period,
        
        -- Flag station opérationnelle
        CASE 
            WHEN sd.is_installed = 1 
             AND sd.is_renting = 1 
             AND sd.is_returning = 1 
            THEN true
            ELSE false
        END AS is_fully_operational
        
    FROM source_data sd
    LEFT JOIN {{ ref('dim_station') }} ds
        ON sd.station_id = ds.station_id
        AND ds.current_validity = true  -- SCD Type 2 : version actuelle
)

SELECT
    -- Clés primaires
    station_id,
    extracted_at,
    day_date,
    
    -- Identifiants
    station_code,
    station_name,
    
    -- Timestamps
    last_reported_at,
    hour_of_day,
    
    -- Disponibilité brute
    num_bikes_available,
    mechanical_available,
    ebikes_available,
    num_docks_available,
    capacity,
    
    -- KPIs calculés
    ROUND(availability_rate, 2) AS availability_rate,
    ROUND(dock_availability_rate, 2) AS dock_availability_rate,
    
    -- Flags état
    is_bike_critical,
    is_dock_critical,
    is_critical,
    is_fully_operational,
    
    -- États opérationnels
    is_installed,
    is_renting,
    is_returning,
    
    -- Enrichissement temporel
    day_of_week,
    day_name,
    day_type,
    time_period,
    
    -- Enrichissement géographique
    commune_name,
    commune_code,
    population,
    geometry
    
    -- Métadonnées
    rental_methods,
    
    -- Audit
    CURRENT_TIMESTAMP AS dbt_updated_at

FROM enriched_data